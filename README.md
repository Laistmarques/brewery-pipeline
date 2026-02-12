# Brewery Data Pipeline 🍺

## 📌 Visão Geral

Este projeto implementa uma pipeline de dados que segue a arquitetura
**Medallion (Bronze → Silver → Gold)** utilizando **Python, PySpark,
Apache Airflow e Docker**.

O objetivo é consumir dados da API pública Open Brewery DB, processá-los
e gerar uma camada analítica agregada pronta para consultas.

------------------------------------------------------------------------

## 📦 Dependências do Projeto

### Requisitos

- **Python 3.10+** – Linguagem principal do pipeline.
- **Docker** – Containerização do ambiente.
- **Docker Compose** – Orquestração dos serviços (Airflow, etc.).


### Principais Bibliotecas Python

**pyspark**  
Framework de processamento distribuído utilizado para:
- Transformações nas camadas Silver e Gold
- Agregações analíticas
- Escrita e leitura em formato Parquet
- Particionamento eficiente dos dados

**apache-airflow**  
Ferramenta de orquestração utilizada para:
- Definir o fluxo Bronze → Silver → Gold
- Controlar dependências entre etapas
- Configurar retries automáticos
- Monitorar execuções
- Agendar pipelines

**requests**  
Biblioteca HTTP utilizada na camada Bronze para:
- Consumir a API Open Brewery DB
- Implementar paginação
- Controlar erros e retries

**pytest**  
Framework de testes utilizado para:
- Testes unitários de transformers
- Testes de jobs (mock de orquestração)
- Testes de integração de writers

**pytest-cov**  
Extensão do pytest utilizada para:
- Medir cobertura de código
- Identificar partes do pipeline que não estão sendo testadas
- Aumentar confiabilidade antes de deploy

### Instalação (Execução Local)

``` bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

------------------------------------------------------------------------

## 🏗 Arquitetura

O pipeline é dividido em três camadas:

### 🥉 Bronze --- Ingestão

-   Consome dados da API Open Brewery DB.
-   Implementa paginação e controle de retry.
-   Armazena dados brutos em JSON.
-   Particionado por `ingestion_date`.

### 🥈 Silver --- Padronização

-   Seleciona colunas relevantes.
-   Renomeia `state_province` para `state`.
-   Remove registros com `country` ou `state` nulos.
-   Adiciona `ingestion_date`.
-   Armazena em Parquet.
-   Particionado por `ingestion_date`, `country` e `state`.

### 🥇 Gold --- Camada Analítica

-   Agrega por `brewery_type`, `country`, `state`, `city`.
-   Métricas:
    -   `num_breweries` (count distinct por id)
    -   `num_city`
-   Armazenado em Parquet.
-   Particionado por `ingestion_date` e `country`.

------------------------------------------------------------------------

## 📂 Estrutura do Projeto

``` bash
brewery-pipeline/
│
├── app/
│   ├── src/pipeline/
│   │   ├── bronze/
│   │   ├── silver/
│   │   ├── gold/
│   │   ├── core/
│   │   └── main.py
│   └── tests/
│
├── airflow/
│   └── dags/
│       └── brewery_pipeline_dag.py
│
├── docker-compose.yml
├── Dockerfile.airflow
└── README.md
```

------------------------------------------------------------------------

## 🐳 Executando com Docker e Airflow

``` bash
docker compose up --build
```

Acesse: http://localhost:8080

Ative e execute o DAG `brewery_pipeline_dag`.

------------------------------------------------------------------------

## 🖥 Execução Local

``` bash
python -m src.pipeline.main --stage bronze --date 2026-02-11
python -m src.pipeline.main --stage silver --date 2026-02-11
python -m src.pipeline.main --stage gold --date 2026-02-11
```

------------------------------------------------------------------------

## 🧪 Testes

``` bash
pytest --cov=src --cov-report=term-missing
```
Inclui: 
- Testes unitários de transformers 
- Testes de jobs com mocks 
- Testes de integração de writers 
- Mock da API


------------------------------------------------------------------------

## 📊 Monitoramento (atual) e melhorias para produção

### O que já existe hoje
- **Orquestração via Airflow**: cada etapa (Bronze/Silver/Gold) é executada como tarefa, com logs e status de execução.
- **Retries / falhas**: o Airflow permite configurar tentativas automáticas e facilita identificar rapidamente qual etapa falhou.
- **Logs estruturados**: logs do pipeline ajudam na investigação (ex.: schema/colunas ausentes, paths e tempo de execução).
- **Particionamento por `ingestion_date`**: facilita reprocessar somente o dia afetado sem reprocessar o dataset inteiro.

---

### O que eu faria para deixar “produção-ready”

#### 1) Alertas e incident management

- **Alertas por falha de task** (Slack/Teams/PagerDuty): notificar automaticamente quando Bronze/Silver/Gold falhar.
- **Alertas por atraso (SLA)**: se a execução do dia não terminar até um horário limite, disparar alerta.
- **Escalonamento**: depois de X falhas consecutivas, abrir incidente.

#### 2) Observabilidade (métricas e dashboards)

- Enviar métricas para Prometheus/Grafana/Datadog/CloudWatch, por exemplo:
  - duração por etapa (bronze/silver/gold)
  - número de registros por camada
  - número de partições geradas por dia
  - volume de dados escrito (MB/GB)

- Criar dashboard com:
  - sucesso/falha por dia
  - tempo médio por execução
  - tendência de crescimento do dataset

#### 3) Data Quality automatizado (DQ)

Além do pipeline “rodar”, garantir que os dados fazem sentido:
- **Checks de schema**: campos obrigatórios e tipos (ex.: `id` string, `country` string).
- **Checks de completude**: % nulos por coluna (ex.: `country/state` não nulos na Silver).
- **Checks de unicidade**: `id` único por dia/localidade (ou ao menos monitorar duplicidade).
- **Checks de consistência**: `brewery_count` >= 0, `city_count` <= `brewery_count`, etc.
- **Checks de freshness**: garantir que existe partição `ingestion_date=YYYY-MM-DD` para o dia esperado.
- Ferramentas recomendadas: **Great Expectations** ou **Soda** (com relatórios por execução).

#### 4) Ambiente e performance (Spark)
- Separar configurações por ambiente (dev/staging/prod)
- Ajustar recursos no cluster (executors/memory/cores)
- Persistência de tabelas em storage confiável (S3/GCS/ADLS)


------------------------------------------------------------------------

## ⚙️ Decisões Técnicas

-   PySpark para processamento escalável
-   Parquet para armazenamento eficiente
-   Particionamento por localização
-   Docker para ambiente reproduzível
-   Airflow para orquestração

------------------------------------------------------------------------

## 🚀 Evoluções Futuras

-   Integração com Delta Lake
-   Data Quality framework
-   CI/CD
-   Deploy em Cloud

------------------------------------------------------------------------

## 🏁 Conclusão

Projeto estruturado seguindo boas práticas de engenharia de dados, com
arquitetura clara, testes automatizados e ambiente reproduzível.