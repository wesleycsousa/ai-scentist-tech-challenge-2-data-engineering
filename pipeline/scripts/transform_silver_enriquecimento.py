import os
import tempfile
import logging
from datetime import date

import pandas as pd
import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
DATA_EXECUCAO = date.today().isoformat()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

DATASET_ID = "br_inep_censo_escolar"
TABLE_ID = "escola"

COLUNAS_INFRAESTRUTURA = [
    "agua_potavel", "internet", "equipamento_computador",
    "biblioteca", "laboratorio_informatica", "esgoto_rede_publica",
]


def ler_parquet_s3(s3_key):
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp.name
    tmp.close()
    s3_client.download_file(S3_BUCKET, s3_key, tmp_path)
    df = pd.read_parquet(tmp_path)
    os.remove(tmp_path)
    return df


def listar_arquivos(prefixo):
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                arquivos.append(obj["Key"])
    return arquivos


def ler_bronze_completa():
    arquivos = listar_arquivos(f"bronze/{DATASET_ID}/{TABLE_ID}/")
    dfs = [ler_parquet_s3(a) for a in arquivos]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Bronze lida: {len(df)} linhas (escolas)")
    return df


def agregar_por_municipio(df_escola):
    logger.info("Agregando infraestrutura escolar por municipio...")

    # para cada coluna de infraestrutura, calcula o % de escolas com valor 1
    agregados = df_escola.groupby("id_municipio").agg(
        total_escolas=("id_escola", "count"),
        **{
            f"pct_escolas_{col}": (col, lambda s: round(s.mean() * 100, 1))
            for col in COLUNAS_INFRAESTRUTURA
        }
    ).reset_index()

    agregados["ano"] = 2024
    logger.info(f"Agregado: {len(agregados)} municipios")
    return agregados


def salvar_silver(df):
    base_dir = f"pipeline/data/tmp/silver_enriquecimento/{TABLE_ID}_por_municipio"
    os.makedirs(base_dir, exist_ok=True)
    path = f"{base_dir}/ano=2024/data_execucao={DATA_EXECUCAO}.parquet"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def upload_silver_s3(caminho_local):
    caminho_relativo = caminho_local.replace(f"pipeline/data/tmp/silver_enriquecimento/{TABLE_ID}_por_municipio/", "")
    s3_key = f"silver/{TABLE_ID}_por_municipio/{caminho_relativo}"
    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")


def main():
    df_escola = ler_bronze_completa()
    df_agregado = agregar_por_municipio(df_escola)
    caminho = salvar_silver(df_agregado)
    upload_silver_s3(caminho)
    logger.info("Silver de enriquecimento (infraestrutura por municipio) concluida")


if __name__ == "__main__":
    main()
