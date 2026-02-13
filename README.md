# Brewery Data Pipeline 🍺

## 📌 Contexto do Desafio

O desafio consistiu em construir uma pipeline de dados a partir da API pública Open Brewery DB, garantindo:

- Organização em camadas

- Reprocessamento por data

- Separação clara de responsabilidades

- Estrutura preparada para evolução futura

A solução foi implementada utilizando Python, PySpark, Apache Airflow e Docker, seguindo o padrão Medallion Architecture (Bronze → Silver → Gold).

## 📌 Visão Geral

A pipeline realiza:

- Extração paginada da API

- Armazenamento bruto (Bronze)

- Padronização e limpeza (Silver)

- Agregação analítica (Gold)

O resultado final é uma camada analítica pronta para consumo.

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

```bash
                 +----------------------+
                 |  Open Brewery DB API |
                 +----------+-----------+
                            |
                            v
+----------------------+  extract   +-------------------------------+
| Airflow DAG (@daily) +----------->| Bronze (JSON raw)             |
| retries + timeout    |            | partition: ingestion_date     |
+----------+-----------+            +---------------+---------------+
           |                                           |
           |                               gate: check bronze exists
           v                                           v
+-------------------------------+          +-------------------------------+
| Silver (Parquet curated)      |<---------+ ShortCircuit / validation     |
| partition: date/country/state |          +-------------------------------+
+---------------+---------------+
                |
                v
+-------------------------------+
| Gold (Parquet analytics)      |
| count by type + location      |
| partition: date/country       |
+-------------------------------+
```

A arquitetura foi projetada para garantir:

- Separação clara entre ingestão, transformação e agregação

- Idempotência por data

- Reprocessamento seguro

- Evolução futura para Data Lake ou storage cloud

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

## ⚖️ Decisões Técnicas e Arquiteturais

Nesta seção explico as principais decisões técnicas adotadas no projeto.

---

### 🗂️ Uso do Parquet

Optei por utilizar **Parquet** nas camadas Silver e Gold por ser um formato colunar, eficiente para consultas analíticas e totalmente integrado ao Spark.

Mantém simplicidade e performance adequadas ao escopo do case.

---

🧱 Estratégia de Particionamento

A Silver é particionada por:

- ingestion_date

- country

- state

A Gold é particionada por:

- ingestion_date

- country

Essa estratégia:

- Permite partition pruning

- Facilita reprocessamento por data

- Mantém organização lógica dos dados

- Evita sobrescrita completa do dataset

---

### 🔁 Overwrite por partição (e não total)

Implementei sobrescrita dinâmica de partição para permitir reprocessamento de datas específicas sem apagar histórico.

Isso garante:
- Idempotência
- Segurança no reprocessamento
- Preservação das demais partições

---

### ⚙️ Orquestração com Airflow (LocalExecutor)

O Airflow foi utilizado para:

- Definir dependências Bronze → Silver → Gold

- Configurar retries automáticos

- Controlar timeout por task

- Monitorar execuções

O LocalExecutor foi escolhido por ser suficiente para o escopo do projeto, mantendo simplicidade e paralelismo básico.

---

### 🐳 Spark containerizado

Executar Spark dentro do Docker garante:
- Reprodutibilidade
- Ambiente consistente
- Facilidade para avaliação do projeto

------------------------------------------------------------------------

## 📂 Estrutura do Projeto

``` bash
brewery-pipeline/
│
├── src/
│   ├── pipeline/
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

Credenciais padrão:
- Usuário: `admin`
- Senha: `admin`

Passos:
1. Despausar a DAG `brewery_pipeline_dag`
2. Executar manualmente ou aguardar o agendamento automático

------------------------------------------------------------------------
## 🔄 Scheduling, Retries e Tratamento de Erros

A DAG foi configurada com:

- Execução diária
- 2 retries automáticos em caso de falha
- Retry delay de 5 minutos
- Timeout de 30 min por task
- Separação clara de etapas: Bronze → Silver → Gold

Tratamento implementado:
- Controle de erros HTTP (429 / 5xx)
- Paginação completa da API
- Logs estruturados
- Pipeline idempotente baseado em `ingestion_date`

------------------------------------------------------------------------

## 📂 Estrutura Física de Saída

### Bronze
```
data/bronze/ingestion_date=YYYY-MM-DD/*.json
```

### Silver
```
data/silver/ingestion_date=YYYY-MM-DD/country=XX/state=YY/*.parquet
```

### Gold
```
data/gold/ingestion_date=YYYY-MM-DD/country=XX/*.parquet
```

------------------------------------------------------------------------

## 🖥 Execução Local

``` bash
python -m pipeline.main --stage bronze --date 2026-02-11
python -m pipeline.main --stage silver --date 2026-02-11
python -m pipeline.main --stage gold --date 2026-02-11
```

------------------------------------------------------------------------

## 🧪 Testes

``` bash
pytest --cov=pipeline --cov-report=term-missing
```
Cobertura inclui:
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

1. Data Quality automatizado
2. Armazenamento transacional (Delta Lake)
3. Deploy em ambiente cloud
4. CI/CD


------------------------------------------------------------------------

## 🏁 Conclusão

A solução foi construída priorizando:

- Clareza arquitetural

- Idempotência

- Reprocessamento seguro

- Organização analítica

- Ambiente reproduzível

Mantém simplicidade adequada ao escopo do case, mas já estruturada para evoluções futuras em ambiente produtivo.