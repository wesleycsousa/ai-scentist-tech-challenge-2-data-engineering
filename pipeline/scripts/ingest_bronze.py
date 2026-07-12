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

TABELAS = [
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "uf", "partition_by_ano": True},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "municipio", "partition_by_ano": True},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "alunos", "partition_by_ano": True},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "meta_alfabetizacao_brasil", "partition_by_ano": False},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "meta_alfabetizacao_uf", "partition_by_ano": False},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "meta_alfabetizacao_municipio", "partition_by_ano": False},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "dicionario", "partition_by_ano": False},
    {"dataset_id": "br_bd_diretorios_brasil", "table_id": "uf", "partition_by_ano": False},
    {"dataset_id": "br_bd_diretorios_brasil", "table_id": "municipio", "partition_by_ano": False},
]


def extrair_tabela(dataset_id, table_id):
    logger.info(f"Extraindo {dataset_id}.{table_id} do BigQuery...")
    df = bd.read_table(
        dataset_id=dataset_id,
        table_id=table_id,
        billing_project_id=BILLING_PROJECT_ID,
    )
    logger.info(f"{dataset_id}.{table_id}: {len(df)} linhas extraidas")
    return df


def salvar_local(df, table_id, dataset_id, particionado):
    caminhos = []
    base_dir = f"{BASE_DIR_LOCAL}/{dataset_id}/{table_id}"
    os.makedirs(base_dir, exist_ok=True)

    if particionado and "ano" in df.columns:
        for ano, grupo in df.groupby("ano"):
            path = f"{base_dir}/ano={ano}/data_execucao={DATA_EXECUCAO}.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            grupo.to_parquet(path, index=False)
            caminhos.append(path)
    else:
        path = f"{base_dir}/data_execucao={DATA_EXECUCAO}.parquet"
        df.to_parquet(path, index=False)
        caminhos.append(path)

    return caminhos


def upload_s3(caminho_local, table_id, dataset_id):
    caminho_relativo = caminho_local.replace(f"{BASE_DIR_LOCAL}/{dataset_id}/{table_id}/", "")
    s3_key = f"bronze/{dataset_id}/{table_id}/{caminho_relativo}"

    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")


def main(tabelas_teste=None):
    config = TABELAS
    if tabelas_teste:
        config = [t for t in TABELAS if t["table_id"] in tabelas_teste]

    resumo = []
    for item in config:
        try:
            df = extrair_tabela(item["dataset_id"], item["table_id"])
            caminhos = salvar_local(df, item["table_id"], item["dataset_id"], item["partition_by_ano"])
            for caminho in caminhos:
                upload_s3(caminho, item["table_id"], item["dataset_id"])
            resumo.append({"tabela": f"{item['dataset_id']}.{item['table_id']}", "status": "OK", "linhas": len(df)})
        except Exception as e:
            logger.error(f"Falha em {item['table_id']}: {e}")
            resumo.append({"tabela": f"{item['dataset_id']}.{item['table_id']}", "status": "FALHOU", "erro": str(e)})

    logger.info("=== Resumo da execucao ===")
    for r in resumo:
        logger.info(r)


if __name__ == "__main__":
    main()
