import os
import logging
from datetime import date

import pandas as pd
import boto3
import basedosdados as bd
from dotenv import load_dotenv

load_dotenv()

BILLING_PROJECT_ID = os.getenv("BILLING_PROJECT_ID")
S3_BUCKET = os.getenv("S3_BUCKET")
DATA_EXECUCAO = date.today().isoformat()

BASE_DIR_LOCAL = "pipeline/data/tmp"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

DATASET_ID = "br_inep_censo_escolar"
TABLE_ID = "escola"
ANO_REFERENCIA = 2024

COLUNAS_PRIORITARIAS = [
    "ano", "sigla_uf", "id_municipio", "id_escola", "rede",
    "agua_potavel", "internet", "equipamento_computador",
    "biblioteca", "laboratorio_informatica", "esgoto_rede_publica",
]


def extrair_escola_2024():
    colunas_sql = ", ".join(COLUNAS_PRIORITARIAS)
    logger.info(f"Extraindo {DATASET_ID}.{TABLE_ID}, ano={ANO_REFERENCIA}...")

    df = bd.read_sql(
        f"""
        SELECT {colunas_sql}
        FROM `basedosdados.{DATASET_ID}.{TABLE_ID}`
        WHERE ano = {ANO_REFERENCIA}
        """,
        billing_project_id=BILLING_PROJECT_ID,
    )
    logger.info(f"{DATASET_ID}.{TABLE_ID}: {len(df)} linhas extraidas")
    return df


def salvar_local(df):
    base_dir = f"{BASE_DIR_LOCAL}/{DATASET_ID}/{TABLE_ID}"
    os.makedirs(base_dir, exist_ok=True)
    path = f"{base_dir}/ano={ANO_REFERENCIA}/data_execucao={DATA_EXECUCAO}.parquet"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def upload_s3(caminho_local):
    caminho_relativo = caminho_local.replace(f"{BASE_DIR_LOCAL}/{DATASET_ID}/{TABLE_ID}/", "")
    s3_key = f"bronze/{DATASET_ID}/{TABLE_ID}/{caminho_relativo}"
    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")


def main():
    df = extrair_escola_2024()
    caminho = salvar_local(df)
    upload_s3(caminho)
    logger.info("Ingestao da fonte externa (Censo Escolar) concluida")


if __name__ == "__main__":
    main()
