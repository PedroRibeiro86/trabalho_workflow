# Trabalho Final - Orquestracao de Workflow

Projeto final da disciplina com foco em um pipeline de dados simples, robusto e totalmente executavel via Docker.

**Video pitch:** [trabalho_workflow](https://1drv.ms/v/c/4ecb51ba76294497/IQBC0-1VDyvKSbM7jZDqc76tAXwOs4cFOMTCwX7FD93wFA0?e=wyfrLa)

## 1. Problema e contexto

Este projeto resolve o problema de coletar, organizar e disponibilizar cotacoes de Bitcoin para analise diaria, sem duplicar dados em reexecucoes.

## 2. Arquitetura da solucao

O pipeline usa arquitetura em camadas:

- Bronze: coleta dado bruto da API publica CoinGecko.
- Silver: aplica padronizacao e tipagem.
- Gold: gera metrica analitica diaria (variacao percentual de 1 dia).

Fluxo no Airflow:

1. `create_tables`
2. `extract_to_bronze`
3. `transform_to_silver`
4. `build_gold`

## 3. Ferramentas utilizadas

- Apache Airflow: orquestracao, agendamento, retries e observabilidade.
- PostgreSQL: persistencia de metadados do Airflow e dados do pipeline.
- Docker Compose: reproducao de ambiente com um comando.

## 4. Como executar do zero

### 4.1 Pre-requisitos

- Docker Desktop instalado e em execucao.
- Portas 8080 e 5432 livres.

### 4.2 Subir ambiente

No diretorio do projeto, execute:

```bash
docker compose up -d
```

### 4.3 Acessar Airflow

- URL: http://localhost:8080
- Usuario: `admin`
- Senha: `admin`

Ative a DAG `bitcoin_medallion_pipeline` e rode manualmente uma execucao para demonstracao.

## 5. Requisitos minimos atendidos

- Agendamento: cron diario as 08:00 (`America/Sao_Paulo`).
- Resiliencia: retries configurados no DAG e timeout na chamada HTTP.
- Idempotencia: `ON CONFLICT ... DO UPDATE` em todas as camadas persistidas.
- Modularidade: tarefas separadas por responsabilidade.
- Persistencia: dados gravados em tabelas no PostgreSQL.
- Observabilidade: UI do Airflow e logs por task.

## 6. Decisoes tecnicas relevantes

- Airflow em vez de Prefect: melhor aderencia ao foco de agendamento cron e UI consolidada.
- PostgreSQL: reduz complexidade operacional e acelera setup.
- Medalhao simples: facilita a gestão de cada camada.

## 7. Estrutura de dados

Schema: `workflow`

- `bronze_bitcoin_quotes`
- `silver_bitcoin_quotes`
- `gold_bitcoin_daily_metrics`

## 8. Video pitch (5-10 min)
Video pitch ja compartilhado: [trabalho_workflow](https://1drv.ms/v/c/4ecb51ba76294497/IQBC0-1VDyvKSbM7jZDqc76tAXwOs4cFOMTCwX7FD93wFA0?e=wyfrLa)
Video complementar: [Trabalho Workflow - Video Complementar](https://1drv.ms/v/c/4ecb51ba76294497/IQB4gAMAdrGYTLAS2S1YSmhzAUr43OEqjnEHly-sFsNJLSE?e=xaT8U4)

## 9. Comandos uteis

Parar tudo:

```bash
docker compose down
```

Remover volumes (reinicio completo):

```bash
docker compose down -v
```
