# NogTech — Pipeline de ETL com Apache Airflow

Projeto acadêmico da disciplina de **Big Data** que implementa um pipeline de ETL (Extract, Transform, Load) completo para a plataforma fictícia NogTech, uma empresa de cursos de tecnologia. O objetivo é consolidar um relatório diário cruzando transações financeiras com métricas de engajamento dos alunos, aplicando enriquecimento de dados via API pública e anonimização conforme a LGPD.

---

## Sumário

1. [Por que Apache Airflow?](#1-por-que-apache-airflow)
2. [Arquitetura do Sistema](#2-arquitetura-do-sistema)
3. [Estrutura do Repositório](#3-estrutura-do-repositório)
4. [Infraestrutura com Docker](#4-infraestrutura-com-docker)
5. [O Pipeline ETL em Detalhes](#5-o-pipeline-etl-em-detalhes)
6. [Estratégia de Idempotência](#6-estratégia-de-idempotência)
7. [Resiliência e Tratamento de Falhas](#7-resiliência-e-tratamento-de-falhas)
8. [Conformidade com a LGPD](#8-conformidade-com-a-lgpd)
9. [Schema do Banco de Dados](#9-schema-do-banco-de-dados)
10. [Como Executar o Projeto](#10-como-executar-o-projeto)
11. [Acessos e Credenciais](#11-acessos-e-credenciais)

---

## 1. Por que Apache Airflow?

A atividade oferecia três opções de orquestração: **Apache Airflow**, **Apache NiFi** e **Luigi**. A escolha pelo Airflow foi fundamentada nos seguintes critérios técnicos:

### Airflow vs. NiFi

O **Apache NiFi** é uma ferramenta orientada a fluxo de dados em tempo real (streaming), com interface visual de arrastar e soltar. Embora poderosa, sua força está em cenários de ingestão contínua de dados (IoT, mensageria, eventos em tempo real). Para o cenário da NogTech — um relatório **diário** processado em **lote (batch)** — o NiFi adiciona complexidade operacional desnecessária: os Processors precisam ser configurados individualmente pela interface gráfica, o que dificulta versionamento em Git e revisão de código em equipe.

O Airflow, por outro lado, é **código como configuração**: toda a lógica do pipeline vive em um único arquivo Python rastreável por Git, revisável via pull request e testável unitariamente.

### Airflow vs. Luigi

O **Luigi** (desenvolvido pelo Spotify) é orientado a alvos e resolve a dependência entre tarefas de trás para frente, verificando se o `output()` de cada `Task` já existe antes de reexecutar. É elegante para pipelines simples com dependências lineares e outputs em disco. Porém, o Luigi **não tem interface web nativa robusta** — o Luigi Central Scheduler oferece uma UI básica, muito inferior ao Airflow em termos de observabilidade, filtragem de execuções históricas e detalhamento de logs por tarefa.

### Por que Airflow vence neste cenário

| Critério | Airflow | NiFi | Luigi |
|---|---|---|---|
| Paradigma | Batch / código Python | Streaming / fluxo visual | Batch / orientado a alvos |
| Versionamento (Git) | Nativo (arquivos `.py`) | Difícil (XML/JSON exportado) | Nativo (arquivos `.py`) |
| Interface web | Completa e detalhada | Completa, porém complexa | Básica |
| Agendamento (cron) | Nativo | Nativo | Manual / externo |
| Observabilidade | Alta (grafo, logs, XCom, histórico) | Alta | Baixa |
| Curva de aprendizado | Moderada | Alta | Baixa |
| Ecossistema Python | Total (qualquer lib pip) | Limitado (Groovy/Jython) | Total |
| Idempotência nativa | Via re-run de DAG | Via back-pressure | Via `output()` target |

O Airflow oferece o melhor equilíbrio entre **poder expressivo em Python**, **observabilidade** e **facilidade de agendamento** para o caso de uso de relatórios diários em batch, que é exatamente o que a NogTech exige.

---

## 2. Arquitetura do Sistema

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network: nogtech-network           │
│                                                             │
│  ┌──────────────┐    ┌──────────────────────────────────┐  │
│  │  postgres-   │    │         Apache Airflow           │  │
│  │  airflow     │◄───│  ┌───────────┐  ┌─────────────┐ │  │
│  │  (metadados) │    │  │ Webserver │  │  Scheduler  │ │  │
│  │  porta: 5432 │    │  │ :8080     │  │  (interno)  │ │  │
│  │  (interno)   │    │  └───────────┘  └─────────────┘ │  │
│  └──────────────┘    └──────────────────────────────────┘  │
│                                          │                  │
│                                          │ executa DAG      │
│                                          ▼                  │
│                              ┌──────────────────┐          │
│                              │  nogtech_pipeline │          │
│                              │  (DAG Python)     │          │
│                              └──────────────────┘          │
│                                          │                  │
│            ┌─────────────┬──────────────┘                  │
│            ▼             ▼                                  │
│      ┌──────────┐  ┌──────────┐                            │
│      │  CSV     │  │  JSON    │  (arquivos locais em /data) │
│      │transações│  │engajamento                            │
│      └──────────┘  └──────────┘                            │
│            │             │                                  │
│            └──────┬──────┘                                  │
│                   ▼                                         │
│            ┌─────────────┐     ┌─────────────┐             │
│            │  Transform  │────►│  BrasilAPI  │ (externa)   │
│            │  + cache    │     │  CEP/Feriado│             │
│            └─────────────┘     └─────────────┘             │
│                   │                                         │
│                   ▼                                         │
│          ┌────────────────┐                                 │
│          │ postgres-      │                                 │
│          │ nogtech (DW)   │  porta exposta: 5433            │
│          │ fato_vendas    │                                 │
│          └────────────────┘                                 │
└─────────────────────────────────────────────────────────────┘
```

O sistema possui **dois bancos de dados PostgreSQL isolados**:

- **postgres-airflow**: banco interno do Airflow, armazena metadados do orquestrador (usuários, histórico de execuções, XComs, conexões). Não é acessado diretamente pela aplicação de negócio.
- **postgres-nogtech**: o Data Warehouse da NogTech, destino final dos dados processados. Exposto na porta `5433` para consultas externas (ferramentas de BI, queries manuais).

---

## 3. Estrutura do Repositório

```
NogTech-main/
│
├── dags/
│   └── nogtech_pipeline.py     # Definição do DAG e toda a lógica ETL
│
├── scripts/
│   ├── extract.py              # Módulo de extração (CSV e JSON)
│   ├── transform.py            # Módulo de transformação e enriquecimento
│   ├── load.py                 # Módulo de carga UPSERT no PostgreSQL
│   └── init_db.sql             # Script DDL — cria a tabela fato_vendas
│
├── data/
│   ├── transacoes_nogtech.csv  # Fonte A: transações financeiras (ERP)
│   └── engajamento_alunos.json # Fonte B: métricas de engajamento (plataforma de vídeo)
│
├── cache/                      # Cache local de CEPs consultados (JSON)
├── logs/                       # Logs do Airflow
├── compose.yml                 # Orquestração de todos os serviços Docker
└── .env                        # Variáveis de ambiente (não versionar em produção)
```

---

## 4. Infraestrutura com Docker

Todo o ecossistema é inicializado com um único comando via Docker Compose. Os serviços e suas responsabilidades são:

| Serviço | Imagem | Função | Porta |
|---|---|---|---|
| `postgres-airflow` | `postgres:15` | Metadados internos do Airflow | 5432 (interno) |
| `postgres-nogtech` | `postgres:15` | Data Warehouse — destino do ETL | 5433 (exposta) |
| `airflow-init` | `apache/airflow:2.9.1` | Inicializa o banco e cria o usuário admin | — |
| `airflow-webserver` | `apache/airflow:2.9.1` | Interface web de monitoramento | 8080 |
| `airflow-scheduler` | `apache/airflow:2.9.1` | Executa as DAGs no horário agendado | — |

### Dependências de inicialização

O Docker Compose define uma ordem explícita de subida dos serviços usando `depends_on` com `condition`:

```
postgres-airflow (healthy)
        │
        ▼
  airflow-init (completed_successfully)
        │
        ├──► airflow-webserver
        └──► airflow-scheduler
```

Isso garante que o banco esteja pronto antes da inicialização do Airflow e que o Airflow esteja inicializado antes do webserver e do scheduler subirem, evitando erros de conexão na inicialização.

### Healthchecks

Ambos os bancos PostgreSQL possuem healthchecks configurados com `pg_isready`, o utilitário nativo do PostgreSQL que verifica se o servidor está aceitando conexões. O Airflow aguarda esse sinal antes de se conectar.

```yaml
healthcheck:
  test: ["CMD", "pg_isready", "-U", "nogtech"]
  interval: 10s
  retries: 5
  start_period: 10s
```

### Volumes persistentes

Os dados dos bancos de dados são armazenados em volumes Docker nomeados (`postgres-airflow-data` e `postgres-nogtech-data`), garantindo que os dados sobrevivam a reinicializações dos containers. Os diretórios `dags/`, `scripts/` e `data/` são montados diretamente do repositório local, permitindo que alterações no código sejam refletidas sem rebuild da imagem.

---

## 5. O Pipeline ETL em Detalhes

O DAG `nogtech-pipeline` é agendado para executar **diariamente às 06h00** (`schedule_interval="0 6 * * *"`). O grafo de dependências é:

```
task_extract_transacoes ──┐
                           ├──► task_transform ──► task_load
task_extract_engajamento ──┘
```

As duas extrações rodam **em paralelo** (sem dependência entre si), economizando tempo de execução. A transformação só inicia após ambas concluírem, e a carga só ocorre após a transformação.

### 5.1 Extract — Extração

#### Fonte A: Transações Financeiras (CSV)

O arquivo `transacoes_nogtech.csv` é exportado pelo ERP financeiro com duas particularidades que exigem configuração explícita na leitura:

- **Encoding `latin-1`** (ISO-8859-1): comum em sistemas legados brasileiros que precisam representar caracteres acentuados sem UTF-8.
- **Delimitador `;`**: padrão em exportações de Excel/ERPs no Brasil, onde a vírgula é reservada para casas decimais.

```python
with open(filepath, encoding="latin-1") as f:
    reader = csv.DictReader(f, delimiter=";")
```

#### Fonte B: Engajamento dos Alunos (JSON)

O arquivo `engajamento_alunos.json` é exportado pela plataforma de vídeo em UTF-8 — o encoding padrão moderno. Cada registro contém métricas mensais de consumo por aluno: horas assistidas, tickets de suporte abertos e pontuação NPS.

#### Comunicação entre tarefas: XCom

O Airflow oferece o mecanismo de **XCom (Cross-Communication)** para passar pequenos volumes de dados entre tarefas sem necessidade de arquivos intermediários. Cada task de extração empurra seus dados via `xcom_push`, e a task de transformação os recupera via `xcom_pull`:

```python
# Na task de extração:
context["ti"].xcom_push(key="transacoes", value=transacoes)

# Na task de transformação:
transacoes = ti.xcom_pull(task_ids="task_extract_transacoes", key="transacoes")
```

XComs são armazenados no banco de metadados do Airflow, por isso são adequados apenas para volumes pequenos a moderados de dados (não para conjuntos de dados de gigabytes).

---

### 5.2 Transform — Transformação

A fase de transformação aplica seis operações em sequência sobre os dados extraídos:

#### 5.2.1 Padronização de CPF

CPFs chegam em dois formatos distintos nas fontes de dados:

| Formato | Exemplo |
|---|---|
| Com máscara | `123.456.789-00` |
| Somente dígitos | `12345678900` |

A função `padronizar_cpf()` remove todos os caracteres não numéricos com regex (`re.sub(r"\D", "", cpf)`) e reconstrói o CPF no formato com máscara, validando que o resultado contém exatamente 11 dígitos. CPFs com número de dígitos diferente de 11 são considerados inválidos, logados e descartados do processamento.

#### 5.2.2 Normalização de Datas

Datas de transação aparecem em dois formatos:

| Formato | Exemplo |
|---|---|
| Brasileiro | `29/01/2024` |
| ISO 8601 | `2024-01-29` |

A função `normalizar_data()` tenta parsear a data em cada formato sequencialmente e sempre retorna no padrão ISO 8601 (`YYYY-MM-DD`), que é o formato nativo do PostgreSQL para o tipo `DATE`.

#### 5.2.3 Junção das Fontes (LEFT JOIN)

As transações são cruzadas com o engajamento usando **dois campos como chave composta**: `cpf_aluno` (normalizado) + `mes_referencia` (os 7 primeiros caracteres da data no formato `YYYY-MM`).

O engajamento é indexado em um dicionário Python antes do loop principal, garantindo lookup em O(1) por transação ao invés de O(n²):

```python
eng_index = {}
for eng in engajamento:
    chave = (cpf_norm, eng["mes_referencia"])
    eng_index[chave] = eng

# Depois, para cada transação:
eng = eng_index.get((cpf_norm, mes_ref), {})
```

Quando não há engajamento correspondente para o mês da transação, o dicionário retorna `{}` e os campos de engajamento ficam `None` — comportamento equivalente a um **LEFT JOIN** em SQL.

#### 5.2.4 Enriquecimento de Localização (BrasilAPI CEP)

Para cada CEP de cobrança, o pipeline consulta o endpoint público da BrasilAPI:

```
GET https://brasilapi.com.br/api/cep/v2/{CEP}
```

A resposta retorna `city`, `state` e `neighborhood`, que são adicionados ao registro como `cidade`, `estado` e `bairro`.

**Cache local**: para evitar consultas repetidas à API para o mesmo CEP (o que desperdiçaria tempo e sobrecarregaria o serviço público), os resultados são armazenados em um arquivo JSON em `/opt/airflow/cache/cep_cache.json`. O cache é carregado no início da transformação e salvo ao final, garantindo que CEPs já consultados em execuções anteriores não precisem ser consultados novamente.

#### 5.2.5 Detecção de Feriados (BrasilAPI Feriados)

Para identificar se uma transação ocorreu em feriado nacional, o pipeline consulta:

```
GET https://brasilapi.com.br/api/feriados/v1/{ANO}
```

A resposta é uma lista de datas no formato ISO 8601. O campo `venda_em_feriado` é um booleano calculado verificando se `data_transacao` está nessa lista.

**Cache por ano**: uma única chamada à API cobre todas as transações do mesmo ano. A lista de feriados é mantida em um dicionário em memória durante a execução, evitando chamadas repetidas para o mesmo ano.

**Fallback de resiliência**: caso a API de feriados esteja indisponível, o pipeline utiliza uma lista de feriados nacionais de 2024 embutida no código (`FERIADOS_FALLBACK`). Isso garante que o pipeline conclua mesmo em cenários de instabilidade da API, sem corromper o dado ou falhar a execução.

#### 5.2.6 Anonimização (LGPD)

Dois campos de identificação pessoal são tratados antes da carga:

**CPF**: mascarado mantendo apenas os 6 dígitos centrais visíveis, tornando impossível a identificação direta sem a chave de reversão:

```
Entrada:  123.456.789-00
Saída:    *.456.789-**
```

**Nome do aluno**: **completamente removido** do registro final. Mascarar um nome próprio ainda permite identificação indireta (por contexto, combinação com outros dados). A remoção total é a única abordagem que garante conformidade com o princípio de minimização de dados da LGPD (Art. 6º, III).

---

### 5.3 Load — Carga

A carga é realizada via **`execute_batch`** do `psycopg2`, que agrupa múltiplos registros em lotes de 200 para envio ao banco. Isso reduz o overhead de round-trips de rede em comparação ao envio registro a registro.

A instrução SQL utilizada é um **UPSERT** (explicado na seção 6).

---

## 6. Estratégia de Idempotência

**Idempotência** significa que executar o pipeline múltiplas vezes com os mesmos dados de entrada produz exatamente o mesmo resultado, sem duplicatas ou inconsistências no destino.

### Estratégia escolhida: Chave Natural + UPSERT

A estratégia adotada utiliza o campo `id_transacao` como **chave natural** — um identificador único atribuído pelo ERP financeiro no momento da transação. Esse campo é declarado como `PRIMARY KEY` na tabela `fato_vendas`.

A carga utiliza a instrução `INSERT ... ON CONFLICT ... DO UPDATE` do PostgreSQL (conhecida como UPSERT):

```sql
INSERT INTO fato_vendas (id_transacao, cpf_aluno_anonimo, ...)
VALUES (%(id_transacao)s, %(cpf_aluno_anonimo)s, ...)
ON CONFLICT (id_transacao) DO UPDATE SET
    cpf_aluno_anonimo = EXCLUDED.cpf_aluno_anonimo,
    cidade            = EXCLUDED.cidade,
    ...
    dt_carga          = NOW();
```

**Como funciona**: o PostgreSQL verifica atomicamente se já existe uma linha com o mesmo `id_transacao`. Se não existir, insere normalmente. Se já existir, atualiza todos os campos com os novos valores. Em nenhum cenário uma transação aparece duas vezes na tabela.

**Por que essa estratégia**: as outras estratégias aceitas pelo enunciado — particionamento por data com overwrite ou tabela de controle por hash — exigiriam lógica adicional de gerenciamento de partições ou tabelas auxiliares. O UPSERT via chave natural é a abordagem mais simples, atômica (sem risco de condição de corrida) e transparente para reprocessamentos.

---

## 7. Resiliência e Tratamento de Falhas

O pipeline possui três camadas de proteção contra falhas:

### Camada 1: Retry de Tarefas do Airflow

Configurado nos `default_args` do DAG, todas as tarefas têm política de retentativa automática:

```python
"retries": 2,
"retry_delay": timedelta(minutes=1),
```

Se uma task falhar (por qualquer motivo — erro de rede, banco temporariamente indisponível, exceção Python), o Airflow aguarda 1 minuto e tenta novamente, até 2 vezes. Se todas as tentativas esgotarem, a task é marcada como `failed` e o restante do DAG é bloqueado, evitando que dados parciais sejam carregados.

### Camada 2: Retry com Backoff Exponencial nas Chamadas de API

Para as consultas à BrasilAPI (CEP e feriados), o código implementa retentativas com **backoff exponencial**: a espera entre tentativas dobra a cada falha (`2^tentativa` segundos), evitando sobrecarregar o serviço em momentos de instabilidade:

```python
for tentativa in range(1, 4):  # 3 tentativas
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            # sucesso
            break
    except Exception as e:
        print(f"[CEP] Tentativa {tentativa} falhou: {e}")
    if tentativa < 3:
        time.sleep(2 ** tentativa)  # 2s, 4s entre tentativas
```

| Tentativa | Espera antes da próxima |
|---|---|
| 1ª falha | 2 segundos |
| 2ª falha | 4 segundos |
| 3ª falha | Encerra, registra None |

### Camada 3: Fallback de Dados para Feriados

Caso a API de feriados esteja completamente indisponível após todas as tentativas, o pipeline não falha nem descarta os registros. Em vez disso, utiliza uma lista de feriados nacionais de 2024 embutida no código como fallback. Os logs registram claramente quando o fallback foi acionado, permitindo revisão posterior.

### Registros inválidos

Transações com CPF inválido (diferente de 11 dígitos) ou data não reconhecida são descartadas individualmente e registradas nos logs com a mensagem `[TRANSFORM] CPF inválido ignorado`. O pipeline não interrompe o processamento dos registros válidos por causa de registros corrompidos.

---

## 8. Conformidade com a LGPD

A Lei Geral de Proteção de Dados (LGPD — Lei nº 13.709/2018) exige que dados pessoais sejam tratados com o mínimo necessário para a finalidade declarada. O pipeline aplica dois princípios:

**Pseudonimização do CPF**: o CPF é transformado em uma representação que não permite identificação direta sem o dado original. A máscara `*.456.789-**` expõe apenas os dígitos centrais, tornando a re-identificação impraticável sem acesso à fonte.

**Supressão do nome**: o campo `nome_aluno` é removido antes da carga via `del reg["nome_aluno"]`. Nomes próprios são dados que permitem identificação direta (Art. 5º, I da LGPD) e não são necessários para o relatório analítico da diretoria — portanto, não devem estar no Data Warehouse.

---

## 9. Schema do Banco de Dados

A tabela `fato_vendas` no banco `nogtech_dw` é criada automaticamente ao subir o container `postgres-nogtech`, via script montado em `/docker-entrypoint-initdb.d/`:

```sql
CREATE TABLE IF NOT EXISTS fato_vendas (
    id_transacao        VARCHAR(50)    PRIMARY KEY,      -- chave natural do ERP
    cpf_aluno_anonimo   VARCHAR(20)    NOT NULL,         -- CPF mascarado (LGPD)
    plano_adquirido     VARCHAR(255),                    -- plano contratado
    valor_brl           NUMERIC(10,2),                   -- valor da transação
    data_transacao      DATE,                            -- data normalizada ISO
    cep_cobranca        VARCHAR(10),                     -- CEP limpo (só dígitos)
    cidade              VARCHAR(100),                    -- via BrasilAPI
    estado              VARCHAR(2),                      -- UF via BrasilAPI
    bairro              VARCHAR(100),                    -- via BrasilAPI
    venda_em_feriado    BOOLEAN        DEFAULT FALSE,    -- via BrasilAPI feriados
    horas_assistidas    NUMERIC(10,2),                   -- engajamento do mês
    tickets_suporte     INTEGER,                         -- engajamento do mês
    nps_score           INTEGER,                         -- engajamento do mês
    mes_referencia      VARCHAR(7),                      -- ex: "2024-01"
    dt_carga            TIMESTAMP      DEFAULT NOW()     -- timestamp da carga ETL
);

-- Índices para consultas analíticas frequentes
CREATE INDEX IF NOT EXISTS idx_fato_vendas_data    ON fato_vendas (data_transacao);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_estado  ON fato_vendas (estado);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_feriado ON fato_vendas (venda_em_feriado);
```

Os índices foram criados nas colunas com maior probabilidade de uso em filtros analíticos: data da transação (análise temporal), estado (análise geográfica) e flag de feriado (análise de sazonalidade).

---

## 10. Como Executar o Projeto

### Pré-requisitos

- Docker Engine instalado e em execução
- Docker Compose (v2+)
- Portas `8080` e `5433` disponíveis na máquina local

### Inicialização

```bash
# Clone o repositório
git clone <url-do-repositorio>
cd NogTech-main

# Suba todos os serviços em background
docker compose up -d
```

O processo de inicialização leva aproximadamente **1 a 2 minutos** na primeira execução (download das imagens + inicialização dos bancos). Acompanhe o progresso:

```bash
docker compose logs -f airflow-init
```

Quando o log exibir `Admin user admin created`, o ambiente está pronto.

### Disparar o pipeline manualmente

1. Acesse a interface web do Airflow em `http://localhost:8080`
2. Faça login com as credenciais de admin
3. Localize o DAG `nogtech-pipeline` na lista
4. Clique no botão de play (▶) para disparar uma execução manual
5. Acompanhe o grafo de execução em tempo real na aba **Graph**

### Verificar os dados carregados

Conecte-se ao Data Warehouse com qualquer cliente PostgreSQL (DBeaver, psql, TablePlus):

```
Host:     localhost
Porta:    5433
Database: nogtech_dw
Usuário:  nogtech
Senha:    nogtech123
```

Ou via linha de comando:

```bash
docker exec -it postgres-nogtech psql -U nogtech -d nogtech_dw -c "SELECT COUNT(*) FROM fato_vendas;"
```

### Encerrar o ambiente

```bash
docker compose down          # para os containers (mantém os dados nos volumes)
docker compose down -v       # para os containers e remove os volumes (reset completo)
```

---

## 11. Acessos e Credenciais

| Serviço | URL / Host | Porta | Usuário | Senha |
|---|---|---|---|---|
| Airflow Web UI | `http://localhost:8080` | 8080 | `admin` | `admin` |
| PostgreSQL NogTech DW | `localhost` | 5433 | `nogtech` | `nogtech123` |
| PostgreSQL Airflow (interno) | `postgres-airflow` | 5432 | `airflow` | `airflow` |

> O banco `postgres-airflow` é acessível apenas dentro da rede Docker (`nogtech-network`) e não é exposto para o host. Ele contém apenas metadados do Airflow e não precisa ser consultado diretamente.
