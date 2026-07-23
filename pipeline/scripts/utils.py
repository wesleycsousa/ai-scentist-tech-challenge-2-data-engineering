import os
import tempfile
import logging

import pandas as pd
import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


def ler_parquet_s3(s3_key):
    """Baixa 1 arquivo parquet do S3 e le como DataFrame."""
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp.name
    tmp.close()
    s3_client.download_file(S3_BUCKET, s3_key, tmp_path)
    df = pd.read_parquet(tmp_path)
    os.remove(tmp_path)
    return df


def listar_arquivos(prefixo):
    """Lista todos os .parquet dentro de uma pasta do S3."""
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                arquivos.append(obj["Key"])
    return arquivos


def ler_tabela_completa(prefixo):
    """Le e concatena TODOS os arquivos parquet de um prefixo do S3."""
    arquivos = listar_arquivos(prefixo)
    dfs = [ler_parquet_s3(a) for a in arquivos]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"{prefixo}: {len(df)} linhas lidas")
    return df


def limpar_prefixo_s3(prefixo):
    """Remove todos os arquivos existentes num prefixo do S3 antes de
    escrever de novo - garante idempotencia (rodar 2x nao duplica dado)."""
    paginator = s3_client.get_paginator("list_objects_v2")
    objetos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        objetos.extend(page.get("Contents", []))
    if objetos:
        s3_client.delete_objects(
            Bucket=S3_BUCKET,
            Delete={"Objects": [{"Key": o["Key"]} for o in objetos]},
        )
        logger.info(f"Limpeza: {len(objetos)} arquivo(s) removido(s) de {prefixo}")


def upload_s3(caminho_local, s3_key):
    """Sobe um arquivo local pro S3, numa chave especifica."""
    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")
