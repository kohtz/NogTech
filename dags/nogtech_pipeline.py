from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "nogtech",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

FERIADOS_FALLBACK = {
    2024: {
        "2024-01-01", "2024-02-12", "2024-02-13", "2024-03-29",
        "2024-04-21", "2024-05-01", "2024-05-30", "2024-06-20",
        "2024-09-07", "2024-10-12", "2024-11-02", "2024-11-15",
        "2024-11-20", "2024-12-25"
    }
}


def run_extract_transacoes(**context):
    import csv
    import os

    DATA_DIR = "/opt/airflow/data"
    filepath = os.path.join(DATA_DIR, "transacoes_nogtech.csv")
    transacoes = []

    with open(filepath, encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            transacoes.append(dict(row))

    print(f"[EXTRACT] {len(transacoes)} transações extraídas.")
    print(f"[EXTRACT] Colunas detectadas: {list(transacoes[0].keys()) if transacoes else 'nenhuma'}")
    context["ti"].xcom_push(key="transacoes", value=transacoes)


def run_extract_engajamento(**context):
    import json
    import os

    DATA_DIR = "/opt/airflow/data"
    filepath = os.path.join(DATA_DIR, "engajamento_alunos.json")

    with open(filepath, encoding="utf-8") as f:
        engajamento = json.load(f)

    print(f"[EXTRACT] {len(engajamento)} registros de engajamento extraídos.")
    context["ti"].xcom_push(key="engajamento", value=engajamento)


def run_transform(**context):
    import re
    import json
    import os
    import time
    import requests
    from datetime import datetime as dt

    ti          = context["ti"]
    transacoes  = ti.xcom_pull(task_ids="task_extract_transacoes",  key="transacoes")
    engajamento = ti.xcom_pull(task_ids="task_extract_engajamento", key="engajamento")

    CACHE_DIR      = "/opt/airflow/cache"
    CEP_CACHE_FILE = os.path.join(CACHE_DIR, "cep_cache.json")
    os.makedirs(CACHE_DIR, exist_ok=True)


    def padronizar_cpf(cpf: str) -> str:
        d = re.sub(r"\D", "", cpf)
        if len(d) != 11:
            raise ValueError(f"CPF inválido: {cpf}")
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"

    def anonimizar_cpf(cpf: str) -> str:
        return f"*.{cpf[4:11]}-**"

    def normalizar_data(data_str: str) -> str:
        data_str = data_str.strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return dt.strptime(data_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        print(f"[TRANSFORM] Data não reconhecida: {data_str}")
        return None

    def carregar_cache_cep() -> dict:
        if os.path.exists(CEP_CACHE_FILE):
            with open(CEP_CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
        return {}

    def salvar_cache_cep(cache: dict) -> None:
        with open(CEP_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    def consultar_cep(cep: str, cache: dict) -> dict:
        cep_limpo = re.sub(r"\D", "", cep)
        if not cep_limpo:
            return {"cidade": None, "estado": None, "bairro": None}
        if cep_limpo in cache:
            return cache[cep_limpo]
        url       = f"https://brasilapi.com.br/api/cep/v2/{cep_limpo}"
        resultado = {"cidade": None, "estado": None, "bairro": None}
        for tentativa in range(1, 4):
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    resultado = {
                        "cidade": data.get("city"),
                        "estado": data.get("state"),
                        "bairro": data.get("neighborhood"),
                    }
                    break
                elif resp.status_code == 404:
                    print(f"[CEP] CEP {cep_limpo} não encontrado.")
                    break
            except Exception as e:
                print(f"[CEP] Tentativa {tentativa} falhou: {e}")
            if tentativa < 3:
                time.sleep(2)
        cache[cep_limpo] = resultado
        return resultado

    def carregar_feriados(ano: int, cache_feriados: dict) -> set:
        if ano in cache_feriados:
            return cache_feriados[ano]
        url     = f"https://brasilapi.com.br/api/feriados/v1/{ano}"
        feriados = set()
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                for item in resp.json():
                    feriados.add(item["date"])
                print(f"[FERIADOS] API: {len(feriados)} feriados para {ano}.")
                cache_feriados[ano] = feriados
                return feriados
        except Exception as e:
            print(f"[FERIADOS] API indisponível: {e}. Usando fallback.")
        feriados = FERIADOS_FALLBACK.get(ano, set())
        print(f"[FERIADOS] Fallback: {len(feriados)} feriados para {ano}.")
        cache_feriados[ano] = feriados
        return feriados

    eng_index = {}
    for eng in engajamento:
        try:
            cpf_norm = padronizar_cpf(eng["cpf_aluno"])
        except ValueError:
            continue
        chave = (cpf_norm, eng["mes_referencia"])
        eng_index[chave] = eng

    cache_cep      = carregar_cache_cep()
    cache_feriados = {}
    resultado      = []
    ignorados      = 0

    for tx in transacoes:
        try:
            cpf_norm = padronizar_cpf(tx.get("cpf_aluno", ""))
        except ValueError:
            print(f"[TRANSFORM] CPF inválido ignorado: {tx.get('cpf_aluno')}")
            ignorados += 1
            continue

        data_tx = normalizar_data(tx.get("data_transacao", ""))
        if not data_tx:
            ignorados += 1
            continue

        mes_ref = data_tx[:7]

        eng = eng_index.get((cpf_norm, mes_ref), {})

        cep_info = consultar_cep(tx.get("cep_cobranca", ""), cache_cep)

        try:
            ano      = int(data_tx[:4])
            feriados = carregar_feriados(ano, cache_feriados)
            venda_em_feriado = data_tx in feriados
        except Exception:
            venda_em_feriado = False

        valor_raw = tx.get("valor_brl", "").replace(",", ".").strip()
        try:
            valor = float(valor_raw)
        except ValueError:
            valor = None

        resultado.append({
            "id_transacao":     tx.get("id_transacao"),
            "cpf_aluno_anonimo": anonimizar_cpf(cpf_norm),
            "plano_adquirido":  tx.get("plano_adquirido"),
            "valor_brl":        valor,
            "data_transacao":   data_tx,
            "cep_cobranca":     re.sub(r"\D", "", tx.get("cep_cobranca", "")),
            "cidade":           cep_info["cidade"],
            "estado":           cep_info["estado"],
            "bairro":           cep_info["bairro"],
            "venda_em_feriado": venda_em_feriado,
            "horas_assistidas": eng.get("horas_assistidas"),
            "tickets_suporte":  eng.get("tickets_suporte"),
            "nps_score":        eng.get("nps_score"),
            "mes_referencia":   mes_ref,
        })

    salvar_cache_cep(cache_cep)
    print(f"[TRANSFORM] {len(resultado)} registros prontos | {ignorados} ignorados.")
    ti.xcom_push(key="registros_transformados", value=resultado)


def run_load(**context):
    import psycopg2
    import psycopg2.extras

    ti        = context["ti"]
    registros = ti.xcom_pull(task_ids="task_transform", key="registros_transformados")

    if not registros:
        raise ValueError("[LOAD] Nenhum registro para carregar.")

    DB_CONFIG = {
        "host":     "postgres-nogtech",
        "port":     5432,
        "dbname":   "nogtech_dw",
        "user":     "nogtech",
        "password": "nogtech123",
    }

    UPSERT_SQL = """
    INSERT INTO fato_vendas (
        id_transacao, cpf_aluno_anonimo, plano_adquirido, valor_brl,
        data_transacao, cep_cobranca, cidade, estado, bairro,
        venda_em_feriado, horas_assistidas, tickets_suporte, nps_score,
        mes_referencia, dt_carga
    ) VALUES (
        %(id_transacao)s, %(cpf_aluno_anonimo)s, %(plano_adquirido)s, %(valor_brl)s,
        %(data_transacao)s, %(cep_cobranca)s, %(cidade)s, %(estado)s, %(bairro)s,
        %(venda_em_feriado)s, %(horas_assistidas)s, %(tickets_suporte)s, %(nps_score)s,
        %(mes_referencia)s, NOW()
    )
    ON CONFLICT (id_transacao) DO UPDATE SET
        cpf_aluno_anonimo = EXCLUDED.cpf_aluno_anonimo,
        plano_adquirido   = EXCLUDED.plano_adquirido,
        valor_brl         = EXCLUDED.valor_brl,
        data_transacao    = EXCLUDED.data_transacao,
        cep_cobranca      = EXCLUDED.cep_cobranca,
        cidade            = EXCLUDED.cidade,
        estado            = EXCLUDED.estado,
        bairro            = EXCLUDED.bairro,
        venda_em_feriado  = EXCLUDED.venda_em_feriado,
        horas_assistidas  = EXCLUDED.horas_assistidas,
        tickets_suporte   = EXCLUDED.tickets_suporte,
        nps_score         = EXCLUDED.nps_score,
        mes_referencia    = EXCLUDED.mes_referencia,
        dt_carga          = NOW();
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, registros, page_size=200)
        print(f"[LOAD] {len(registros)} registros gravados em fato_vendas.")
    finally:
        conn.close()


with DAG(
    dag_id="nogtech-pipeline",
    description="Pipeline - transações x engajamento NogTech",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["nogtech", "etl"],
) as dag:

    task_extract_transacoes = PythonOperator(
        task_id="task_extract_transacoes",
        python_callable=run_extract_transacoes,
    )

    task_extract_engajamento = PythonOperator(
        task_id="task_extract_engajamento",
        python_callable=run_extract_engajamento,
    )

    task_transform = PythonOperator(
        task_id="task_transform",
        python_callable=run_transform,
    )

    task_load = PythonOperator(
        task_id="task_load",
        python_callable=run_load,
    )

    [task_extract_transacoes, task_extract_engajamento] >> task_transform >> task_load
