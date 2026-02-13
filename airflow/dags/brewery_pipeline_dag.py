import subprocess
import json

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException
from airflow.operators.python import ShortCircuitOperator

from pipeline.core.config import BRONZE_DIR, BRONZE_PARTITION_KEY, BREWERIES_DATASET
from pipeline.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_ARGS = {
    "owner": "lais marques",
    "retry_delay": timedelta(minutes=5),
    "retries": 2,
    "email_on_failure": False,
    "email": ["lais.teles@hotmail.com"],
    "execution_timeout": timedelta(minutes=30),
}

def run_stage(stage: str, execution_dt: str):
    """
    Executa seu entrypoint (pipeline.main) como subprocess.
    A DAG orquestra; o pipeline executa fora do contexto do Airflow.
    """

    command = [
        "python", 
        "-m", 
        "pipeline.main", 
        "--stage", stage, 
        "--date", execution_dt
    ]

    logger.info(f"Running stage '{stage}' with command: {' '.join(command)}")    

    r = subprocess.run(
        command, 
        cwd="/",
        capture_output=True, 
        text=True
    )

    if r.stdout:
        logger.info(f"stdout | stage={stage} | {r.stdout}")

    if r.returncode != 0:
        if r.stderr:
            logger.error(f"stderr | stage={stage} | {r.stderr}")
        raise AirflowFailException(f"Stage '{stage}' failed (code={r.returncode})")


def check_bronze_data(execution_dt: str, **context) -> bool:
    """
    Verifica se os dados da camada Bronze estão disponíveis para a data de execução.
    A camada Silver depende desses dados para ser executada, portanto essa função atua como um gate
    para a execução dos estágios seguintes.
    """
    ds = context['ds']
    
    p = (
        Path(BRONZE_DIR)
        / BREWERIES_DATASET
        / f"{BRONZE_PARTITION_KEY}={execution_dt}"
        / f"{BREWERIES_DATASET}.json"
    )
    
    logger.info(f"Checking Bronze data | path={p}")
    logger.info(f"cwd={Path().resolve()} | BRONZE_DIR={BRONZE_DIR} | resolved={Path(BRONZE_DIR).resolve()}")
    logger.info(f"checking={p} | resolved={p.resolve()}")

    if not p.exists():
        return False

    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return isinstance(data, list) and len(data) > 0
    except Exception as e:
        logger.error(f"Failed reading Bronze JSON | path={p} | err={e}")
        return False


@dag(
    dag_id="brewery_pipeline_dag",
    description="Pipeline de processamento de dados de cervejarias, do Bronze ao Gold",
    schedule="@daily",
    start_date=datetime(2026, 2, 9),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["brewery", "pipeline", "bronze", "silver", "gold"],
    max_active_runs=1,
)
def brewery_pipeline():

    """
    DAG principal para orquestrar a execução do pipeline de dados de cervejarias, desde a extração (Bronze)
    até a agregação final (Gold). 
    A DAG é composta por tarefas que executam os estágios do pipeline como subprocessos, garantindo isolamento e portabilidade do código.
    - A tarefa de verificação de dados atua como um gate para os estágios seguintes, 
        garantindo que a pipeline só avance para Silver e Gold se os dados da camada Bronze estiverem disponíveis para a data de execução. 
    - Cada estágio é executado de forma independente, permitindo melhor monitoramento e isolamento de falhas.  
    - O uso de subprocessos para executar o pipeline fora do contexto do Airflow mantém a flexibilidade e portabilidade do código,
        permitindo que ele seja executado em diferentes ambientes sem dependências específicas do Airflow.
    
    """

    @task(task_id="data_interval_start")
    def get_execution_date(data_interval_start=None) -> str:
        """
        Retorna a data de início do intervalo de execução 
        formatada como string, 
        usada para determinar quais dados processar em cada estágio do pipeline.
        Args:
            data_interval_start (datetime, optional): Data de início do intervalo de execução. 
                Fornecida automaticamente pelo Airflow.
        Returns:
            str: Data de início do intervalo formatada como 'YYYY-MM-DD'.
        """
        return data_interval_start.strftime("%Y-%m-%d")

    exec_date = get_execution_date()

    with TaskGroup("stages"):

        @task
        def bronze(execution_dt: str) -> None:
            run_stage("bronze", execution_dt)

        @task
        def silver(execution_dt: str) -> None:
            run_stage("silver", execution_dt)

        @task
        def gold(execution_dt: str) -> None:
            run_stage("gold", execution_dt)

        bronze_task = bronze(exec_date)

        bronze_gate = ShortCircuitOperator(
            task_id="check_bronze_data",
            python_callable=check_bronze_data,
            op_args=[exec_date],
        )
        bronze_task >> bronze_gate >> silver(exec_date) >> gold(exec_date)


brewery_pipeline()