import logging
import os

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("NOGTECH_DB_HOST",     "postgres-nogtech"),
    "port":     int(os.getenv("NOGTECH_DB_PORT", "5432")),
    "dbname":   os.getenv("NOGTECH_DB_NAME",     "nogtech_dw"),
    "user":     os.getenv("NOGTECH_DB_USER",     "nogtech"),
    "password": os.getenv("NOGTECH_DB_PASSWORD", "nogtech123"),
}

UPSERT_SQL = """
INSERT INTO fato_vendas (
    id_transacao,
    cpf_aluno_anonimo,
    curso,
    valor_transacao,
    data_transacao,
    cep_cobranca,
    cidade,
    estado,
    bairro,
    venda_em_feriado,
    total_acessos,
    total_minutos,
    mes_referencia,
    dt_carga
)
VALUES (
    %(id_transacao)s,
    %(cpf_aluno_anonimo)s,
    %(curso)s,
    %(valor_transacao)s,
    %(data_transacao)s,
    %(cep_cobranca)s,
    %(cidade)s,
    %(estado)s,
    %(bairro)s,
    %(venda_em_feriado)s,
    %(total_acessos)s,
    %(total_minutos)s,
    %(mes_referencia)s,
    NOW()
)
ON CONFLICT (id_transacao) DO UPDATE SET
    cpf_aluno_anonimo = EXCLUDED.cpf_aluno_anonimo,
    curso             = EXCLUDED.curso,
    valor_transacao   = EXCLUDED.valor_transacao,
    data_transacao    = EXCLUDED.data_transacao,
    cep_cobranca      = EXCLUDED.cep_cobranca,
    cidade            = EXCLUDED.cidade,
    estado            = EXCLUDED.estado,
    bairro            = EXCLUDED.bairro,
    venda_em_feriado  = EXCLUDED.venda_em_feriado,
    total_acessos     = EXCLUDED.total_acessos,
    total_minutos     = EXCLUDED.total_minutos,
    mes_referencia    = EXCLUDED.mes_referencia,
    dt_carga          = NOW();
"""


def load(registros: list[dict]) -> None:

    if not registros:
        logger.warning("[LOAD] Nenhum registro para carregar.")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, registros, page_size=100)

        logger.info(f"[LOAD] {len(registros)} registros gravados em fato_vendas (UPSERT).")

    except psycopg2.Error as e:
        logger.error(f"[LOAD] Erro ao gravar no banco: {e}")
        raise

    finally:
        if conn:
            conn.close()
