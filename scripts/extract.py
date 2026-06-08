
import csv
import json
import logging
import os

logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")


def extract_transacoes() -> list[dict]:

    filepath = os.path.join(DATA_DIR, "transacoes_nogtech.csv")
    transacoes = []

    with open(filepath, encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            transacoes.append(dict(row))

    logger.info(f"[EXTRACT] {len(transacoes)} transações extraídas do CSV.")
    return transacoes


def extract_engajamento() -> list[dict]:

    filepath = os.path.join(DATA_DIR, "engajamento_alunos.json")

    with open(filepath, encoding="utf-8") as f:
        engajamento = json.load(f)

    logger.info(f"[EXTRACT] {len(engajamento)} registros de engajamento extraídos do JSON.")
    return engajamento
