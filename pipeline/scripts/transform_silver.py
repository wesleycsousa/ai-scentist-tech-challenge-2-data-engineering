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


def ler_parquet_s3(s3_key: str) -> pd.DataFrame:
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp.name
    tmp.close()

    s3_client.download_file(S3_BUCKET, s3_key, tmp_path)
    df = pd.read_parquet(tmp_path)

    os.remove(tmp_path)
    return df


def listar_arquivos(prefixo: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                arquivos.append(obj["Key"])
    return arquivos


def ler_tabela_bronze_completa(dataset_id: str, table_id: str) -> pd.DataFrame:
    """Le e concatena TODOS os arquivos parquet de uma tabela (todas as particoes de ano)."""
    arquivos = listar_arquivos(f"bronze/{dataset_id}/{table_id}/")
    dfs = [ler_parquet_s3(a) for a in arquivos]
    df_completo = pd.concat(dfs, ignore_index=True)
    logger.info(f"{dataset_id}.{table_id}: {len(df_completo)} linhas lidas de {len(arquivos)} arquivo(s)")
    return df_completo


def montar_dicionario_rede(dicionario_df: pd.DataFrame, id_tabela: str) -> dict:
    """Monta um dict {codigo: descricao} filtrando o dicionario para uma tabela e coluna especifica."""
    filtro = (dicionario_df["id_tabela"] == id_tabela) & (dicionario_df["nome_coluna"] == "rede")
    subset = dicionario_df[filtro]
    return dict(zip(subset["chave"].astype(str), subset["valor"]))

def salvar_silver_local(df: pd.DataFrame, table_id: str) -> list[str]:
    """Salva o DataFrame como parquet local, particionado por ano."""
    caminhos = []
    base_dir = f"pipeline/data/tmp/silver/{table_id}"
    os.makedirs(base_dir, exist_ok=True)

    for ano, grupo in df.groupby("ano"):
        path = f"{base_dir}/ano={ano}/data_execucao={DATA_EXECUCAO}.parquet"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        grupo.to_parquet(path, index=False)
        caminhos.append(path)

    return caminhos


def upload_silver_s3(caminho_local: str, table_id: str):
    caminho_relativo = caminho_local.replace(f"pipeline/data/tmp/silver/{table_id}/", "")
    s3_key = f"silver/{table_id}/{caminho_relativo}"

    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")

def transformar_geografia(table_id: str, diretorio_table_id: str, chave_join_bronze: str, chave_join_diretorio: str) -> pd.DataFrame:
    logger.info(f"=== Transformando tabela {table_id.upper()} ===")

    df_bruta = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", table_id)
    df_dicionario = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "dicionario")
    df_diretorio = ler_tabela_bronze_completa("br_bd_diretorios_brasil", diretorio_table_id)

    mapa_rede = montar_dicionario_rede(df_dicionario, table_id)
    df_bruta["rede_descricao"] = df_bruta["rede"].astype(str).map(mapa_rede)

    df_enriquecido = df_bruta.merge(
        df_diretorio,
        left_on=chave_join_bronze,
        right_on=chave_join_diretorio,
        how="left",
        suffixes=("", "_diretorio"),
    )

    logger.info(f"{table_id.upper()} enriquecida: {len(df_enriquecido)} linhas, {df_enriquecido.shape[1]} colunas")
    return df_enriquecido

if __name__ == "__main__":
    # UF
    resultado_uf = transformar_geografia(
        table_id="uf",
        diretorio_table_id="uf",
        chave_join_bronze="sigla_uf",
        chave_join_diretorio="sigla",
    )
    caminhos = salvar_silver_local(resultado_uf, "uf")
    for caminho in caminhos:
        upload_silver_s3(caminho, "uf")

    # MUNICIPIO
    resultado_municipio = transformar_geografia(
        table_id="municipio",
        diretorio_table_id="municipio",
        chave_join_bronze="id_municipio",
        chave_join_diretorio="id_municipio",  # AJUSTAR conforme resultado do comando acima
    )
    caminhos = salvar_silver_local(resultado_municipio, "municipio")
    for caminho in caminhos:
        upload_silver_s3(caminho, "municipio")

    logger.info("=== Transformacao da Silver (geografia) concluida ===")