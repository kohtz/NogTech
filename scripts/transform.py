import json
import logging
import os
import re
import time
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

CACHE_DIR  = os.getenv("CACHE_DIR",  "/opt/airflow/cache")
CEP_CACHE_FILE = os.path.join(CACHE_DIR, "cep_cache.json")

def _padronizar_cpf(cpf: str) -> str:

    digits = re.sub(r"\D", "", cpf)
    if len(digits) != 11:
        raise ValueError(f"CPF inválido: '{cpf}'")
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"

def _anonimizar_cpf(cpf_mascarado: str) -> str:

    return f"*.{cpf_mascarado[4:11]}-**"


def _carregar_cache_cep() -> dict:
    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(CEP_CACHE_FILE):
        with open(CEP_CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _salvar_cache_cep(cache: dict) -> None:
    with open(CEP_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _consultar_cep(cep: str, cache: dict, max_retries: int = 3) -> dict:

    cep_limpo = re.sub(r"\D", "", cep)

    if cep_limpo in cache:
        return cache[cep_limpo]

    url = f"https://brasilapi.com.br/api/cep/v2/{cep_limpo}"
    resultado = {"cidade": None, "estado": None, "bairro": None}

    for tentativa in range(1, max_retries + 1):
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
                logger.warning(f"[CEP] CEP {cep_limpo} não encontrado na BrasilAPI.")
                break
            else:
                logger.warning(f"[CEP] Tentativa {tentativa} falhou (HTTP {resp.status_code}) para CEP {cep_limpo}.")
        except requests.RequestException as e:
            logger.warning(f"[CEP] Tentativa {tentativa} — erro de rede: {e}")

        if tentativa < max_retries:
            time.sleep(2 ** tentativa)  

    cache[cep_limpo] = resultado
    return resultado

def _carregar_feriados(ano: int, cache_feriados: dict, max_retries: int = 3) -> set:

    if ano in cache_feriados:
        return cache_feriados[ano]

    url = f"https://brasilapi.com.br/api/feriados/v1/{ano}"
    feriados = set()

    for tentativa in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                for item in resp.json():
                    feriados.add(item["date"])
                logger.info(f"[FERIADOS] {len(feriados)} feriados carregados para {ano}.")
                break
            else:
                logger.warning(f"[FERIADOS] Tentativa {tentativa} falhou (HTTP {resp.status_code}) para ano {ano}.")
        except requests.RequestException as e:
            logger.warning(f"[FERIADOS] Tentativa {tentativa} — erro de rede: {e}")

        if tentativa < max_retries:
            time.sleep(2 ** tentativa)

    cache_feriados[ano] = feriados
    return feriados

def _juntar_fontes(transacoes: list[dict], engajamento: list[dict]) -> list[dict]:
    eng_index = {}
    for eng in engajamento:
        try:
            cpf_norm = _padronizar_cpf(eng["cpf_aluno"])
        except ValueError:
            continue
        chave = (cpf_norm, eng["mes_referencia"])
        eng_index[chave] = eng

    resultado = []
    for tx in transacoes:
        try:
            cpf_norm = _padronizar_cpf(tx["cpf_aluno"])
        except ValueError:
            logger.warning(f"[TRANSFORM] CPF inválido ignorado: {tx.get('cpf_aluno')}")
            continue

        data_tx = tx.get("data_transacao", "")
        mes_ref = data_tx[:7] if len(data_tx) >= 7 else None

        eng = eng_index.get((cpf_norm, mes_ref), {})

        resultado.append({
            "id_transacao":     tx["id_transacao"],
            "cpf_aluno":        cpf_norm,   
            "nome_aluno":       tx["nome_aluno"],
            "curso":            tx["curso"],
            "valor_transacao":  tx["valor_transacao"].replace(",", ".") if tx.get("valor_transacao") else None,
            "data_transacao":   data_tx,
            "cep_cobranca":     tx.get("cep_cobranca", ""),
            "mes_referencia":   mes_ref,
            "total_acessos":    eng.get("total_acessos"),
            "total_minutos":    eng.get("total_minutos"),
        })

    logger.info(f"[TRANSFORM] {len(resultado)} registros após junção.")
    return resultado


def transform(transacoes: list[dict], engajamento: list[dict]) -> list[dict]:

    registros = _juntar_fontes(transacoes, engajamento)

    cache_cep      = _carregar_cache_cep()
    cache_feriados = {}

    resultado = []
    for reg in registros:

        cep_info = _consultar_cep(reg["cep_cobranca"], cache_cep)
        reg["cidade"] = cep_info["cidade"]
        reg["estado"] = cep_info["estado"]
        reg["bairro"] = cep_info["bairro"]

        try:
            ano = int(reg["data_transacao"][:4])
            feriados = _carregar_feriados(ano, cache_feriados)
            reg["venda_em_feriado"] = reg["data_transacao"] in feriados
        except (ValueError, TypeError):
            reg["venda_em_feriado"] = False

        reg["cpf_aluno_anonimo"] = _anonimizar_cpf(reg["cpf_aluno"])
        del reg["cpf_aluno"]    
        del reg["nome_aluno"]

        resultado.append(reg)

    _salvar_cache_cep(cache_cep)

    logger.info(f"[TRANSFORM] {len(resultado)} registros prontos para carga.")
    return resultado
