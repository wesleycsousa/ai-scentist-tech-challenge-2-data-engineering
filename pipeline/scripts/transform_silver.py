import os
import tempfile
import logging
from datetime import date

import pandas as pd
import boto3
from dotenv import load_dotenv

import gc


load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
DATA_EXECUCAO = date.today().isoformat()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


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


def ler_tabela_bronze_completa(dataset_id, table_id):
    arquivos = listar_arquivos(f"bronze/{dataset_id}/{table_id}/")
    dfs = [ler_parquet_s3(a) for a in arquivos]
    df_completo = pd.concat(dfs, ignore_index=True)
    logger.info(f"{dataset_id}.{table_id}: {len(df_completo)} linhas lidas de {len(arquivos)} arquivo(s)")
    return df_completo


def montar_mapa_dicionario(dicionario_df, id_tabela, nome_coluna):
    filtro = (dicionario_df["id_tabela"] == id_tabela) & (dicionario_df["nome_coluna"] == nome_coluna)
    subset = dicionario_df[filtro]
    return dict(zip(subset["chave"].astype(str), subset["valor"]))


def salvar_silver_local(df, table_id):
    caminhos = []
    base_dir = f"pipeline/data/tmp/silver/{table_id}"
    os.makedirs(base_dir, exist_ok=True)

    for ano, grupo in df.groupby("ano"):
        path = f"{base_dir}/ano={ano}/data_execucao={DATA_EXECUCAO}.parquet"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        grupo.to_parquet(path, index=False)
        caminhos.append(path)

    return caminhos


def upload_silver_s3(caminho_local, table_id):
    caminho_relativo = caminho_local.replace(f"pipeline/data/tmp/silver/{table_id}/", "")
    s3_key = f"silver/{table_id}/{caminho_relativo}"

    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")


def transformar_geografia(table_id, diretorio_table_id, chave_join_bronze, chave_join_diretorio):
    logger.info(f"=== Transformando tabela {table_id.upper()} ===")

    df_bruta = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", table_id)
    df_dicionario = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "dicionario")
    df_diretorio = ler_tabela_bronze_completa("br_bd_diretorios_brasil", diretorio_table_id)

    mapa_rede = montar_mapa_dicionario(df_dicionario, table_id, "rede")
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


def transformar_alunos():
    logger.info("=== Transformando tabela ALUNOS ===")

    df_alunos = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "alunos")
    df_dicionario = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "dicionario")
    df_diretorio_municipio = ler_tabela_bronze_completa("br_bd_diretorios_brasil", "municipio")

    colunas_uteis_diretorio = ["id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao"]
    df_diretorio_municipio = df_diretorio_municipio[colunas_uteis_diretorio]

    colunas_codificadas = ["rede", "alfabetizado", "presenca", "preenchimento_caderno"]
    for coluna in colunas_codificadas:
        mapa = montar_mapa_dicionario(df_dicionario, "alunos", coluna)
        if mapa:
            df_alunos[f"{coluna}_descricao"] = df_alunos[coluna].astype(str).map(mapa)
        else:
            logger.info(f"Sem mapeamento no dicionario para alunos.{coluna} - mantendo como esta")

    df_enriquecido = df_alunos.merge(
        df_diretorio_municipio,
        on="id_municipio",
        how="left",
        suffixes=("", "_diretorio"),
    )

    logger.info(f"ALUNOS enriquecida: {len(df_enriquecido)} linhas, {df_enriquecido.shape[1]} colunas")
    return df_enriquecido


def transformar_meta_brasil():
    logger.info("=== Transformando tabela META_ALFABETIZACAO_BRASIL ===")

    df = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "meta_alfabetizacao_brasil")

    logger.info(f"META_ALFABETIZACAO_BRASIL processada: {len(df)} linhas, {df.shape[1]} colunas")
    return df


def transformar_meta_uf():
    logger.info("=== Transformando tabela META_ALFABETIZACAO_UF ===")

    df_meta = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "meta_alfabetizacao_uf")
    df_diretorio_uf = ler_tabela_bronze_completa("br_bd_diretorios_brasil", "uf")

    df_enriquecido = df_meta.merge(
        df_diretorio_uf,
        left_on="sigla_uf",
        right_on="sigla",
        how="left",
        suffixes=("", "_diretorio"),
    )

    logger.info(f"META_ALFABETIZACAO_UF enriquecida: {len(df_enriquecido)} linhas, {df_enriquecido.shape[1]} colunas")
    return df_enriquecido


def transformar_meta_municipio():
    logger.info("=== Transformando tabela META_ALFABETIZACAO_MUNICIPIO ===")

    df_meta = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "meta_alfabetizacao_municipio")
    df_diretorio_municipio = ler_tabela_bronze_completa("br_bd_diretorios_brasil", "municipio")

    colunas_uteis_diretorio = ["id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao"]
    df_diretorio_municipio = df_diretorio_municipio[colunas_uteis_diretorio]

    df_enriquecido = df_meta.merge(
        df_diretorio_municipio,
        on="id_municipio",
        how="left",
        suffixes=("", "_diretorio"),
    )

    logger.info(f"META_ALFABETIZACAO_MUNICIPIO enriquecida: {len(df_enriquecido)} linhas, {df_enriquecido.shape[1]} colunas")
    return df_enriquecido


if __name__ == "__main__":
    resultado_uf = transformar_geografia(
        table_id="uf", diretorio_table_id="uf",
        chave_join_bronze="sigla_uf", chave_join_diretorio="sigla",
    )
    for caminho in salvar_silver_local(resultado_uf, "uf"):
        upload_silver_s3(caminho, "uf")
    del resultado_uf
    gc.collect()

    resultado_municipio = transformar_geografia(
        table_id="municipio", diretorio_table_id="municipio",
        chave_join_bronze="id_municipio", chave_join_diretorio="id_municipio",
    )
    for caminho in salvar_silver_local(resultado_municipio, "municipio"):
        upload_silver_s3(caminho, "municipio")
    del resultado_municipio
    gc.collect()

    resultado_alunos = transformar_alunos()
    for caminho in salvar_silver_local(resultado_alunos, "alunos"):
        upload_silver_s3(caminho, "alunos")
    del resultado_alunos
    gc.collect()

    resultado_meta_brasil = transformar_meta_brasil()
    for caminho in salvar_silver_local(resultado_meta_brasil, "meta_alfabetizacao_brasil"):
        upload_silver_s3(caminho, "meta_alfabetizacao_brasil")
    del resultado_meta_brasil
    gc.collect()

    resultado_meta_uf = transformar_meta_uf()
    for caminho in salvar_silver_local(resultado_meta_uf, "meta_alfabetizacao_uf"):
        upload_silver_s3(caminho, "meta_alfabetizacao_uf")
    del resultado_meta_uf
    gc.collect()

    resultado_meta_municipio = transformar_meta_municipio()
    for caminho in salvar_silver_local(resultado_meta_municipio, "meta_alfabetizacao_municipio"):
        upload_silver_s3(caminho, "meta_alfabetizacao_municipio")
    del resultado_meta_municipio
    gc.collect()

    logger.info("=== Transformacao da Silver concluida (uf, municipio, alunos, meta_brasil, meta_uf, meta_municipio) ===")
