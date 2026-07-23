import os
import tempfile
import logging
from datetime import date

import pandas as pd
import boto3
from dotenv import load_dotenv

from data_quality import DataQualityCheck

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
DATA_EXECUCAO = date.today().isoformat()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


# baixa 1 arquivo parquet do S3 e le como dataframe
def ler_parquet_s3(s3_key):
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp.name
    tmp.close()  # fecha antes do boto3 escrever (bug do Windows)
    s3_client.download_file(S3_BUCKET, s3_key, tmp_path)
    df = pd.read_parquet(tmp_path)
    os.remove(tmp_path)
    return df


# lista os arquivos .parquet dentro de uma pasta do S3
def listar_arquivos(prefixo):
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                arquivos.append(obj["Key"])
    return arquivos


# le e junta todos os arquivos de uma tabela Silver (particionada por ano)
def ler_silver_completa(table_id):
    arquivos = listar_arquivos(f"silver/{table_id}/")
    dfs = [ler_parquet_s3(a) for a in arquivos]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"silver.{table_id}: {len(df)} linhas lidas")
    return df


# salva a Gold local, particionada por ano (mesmo padrao da bronze/silver)
def salvar_gold_local(df, table_id):
    caminhos = []
    base_dir = f"pipeline/data/tmp/gold/{table_id}"
    os.makedirs(base_dir, exist_ok=True)
    for ano, grupo in df.groupby("ano"):
        path = f"{base_dir}/ano={ano}/data_execucao={DATA_EXECUCAO}.parquet"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        grupo.to_parquet(path, index=False)
        caminhos.append(path)
    return caminhos


# sobe o parquet local pro S3, na pasta gold/<tabela>/
def upload_gold_s3(caminho_local, table_id):
    caminho_relativo = caminho_local.replace(f"pipeline/data/tmp/gold/{table_id}/", "")
    s3_key = f"gold/{table_id}/{caminho_relativo}"
    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")


# ==================== GOLD 1: indicador_por_municipio ====================

def construir_indicador_por_municipio():
    logger.info("=== Construindo GOLD: indicador_por_municipio ===")
    df = ler_silver_completa("municipio")

    # rede="5" = "Publica (Estadual e Municipal)" - escolhido apos checar
    # os dados reais: rede="0" (Total) esta praticamente vazio nos dois anos
    # (0 linhas em 2023, quase nulo em 2024). rede="5" e o agregado mais
    # completo e consistente disponivel (4.950 municipios em 2023, 5.516 em
    # 2024) - tambem faz sentido de dominio, ja que a politica publica em
    # questao foca na rede publica de ensino
    df_rede_publica = df[df["rede"] == "5"].copy()

    colunas_finais = [
        "ano", "id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao",
        "taxa_alfabetizacao", "media_portugues",
        "proporcao_aluno_nivel_0", "proporcao_aluno_nivel_4", "proporcao_aluno_nivel_8",
    ]
    df_gold = df_rede_publica[colunas_finais].reset_index(drop=True)
    logger.info(f"indicador_por_municipio: {len(df_gold)} linhas")
    return df_gold


def validar_indicador_por_municipio(df):
    dq = DataQualityCheck(df, "gold.indicador_por_municipio")
    dq.check_duplicidade(["ano", "id_municipio"])  # 1 linha por municipio/ano
    dq.check_nulos("id_municipio")  # chave nunca pode ser nula
    dq.check_range("taxa_alfabetizacao", 0, 100)  # percentual valido
    dq.logar(logger)
    dq.falhar_se_critico()  # Gold = fail-fast


# ==================== GOLD 2: evolucao_temporal ====================

def construir_evolucao_temporal():
    logger.info("=== Construindo GOLD: evolucao_temporal ===")

    # rede="5" (Publica) - mesma decisao e mesmo motivo da funcao acima,
    # validado tanto em uf quanto em municipio antes de aplicar aqui
    df_uf = ler_silver_completa("uf")
    df_uf_rede_publica = df_uf[df_uf["rede"] == "5"].copy()
    df_uf_rede_publica["nivel_geografico"] = "uf"
    df_uf_rede_publica["local_id"] = df_uf_rede_publica["sigla_uf"]
    df_uf_rede_publica["local_nome"] = df_uf_rede_publica["nome"]

    df_municipio = ler_silver_completa("municipio")
    df_municipio_rede_publica = df_municipio[df_municipio["rede"] == "5"].copy()
    df_municipio_rede_publica["nivel_geografico"] = "municipio"
    df_municipio_rede_publica["local_id"] = df_municipio_rede_publica["id_municipio"]
    df_municipio_rede_publica["local_nome"] = df_municipio_rede_publica["nome"]

    # empilha as duas (uf embaixo de municipio), so as colunas em comum
    colunas_comuns = ["nivel_geografico", "local_id", "local_nome", "ano", "taxa_alfabetizacao"]
    df_unificado = pd.concat([
        df_uf_rede_publica[colunas_comuns],
        df_municipio_rede_publica[colunas_comuns],
    ], ignore_index=True)

    # calcula a variacao ano a ano, por local (shift = "olha a linha anterior")
    df_unificado = df_unificado.sort_values(["nivel_geografico", "local_id", "ano"])
    df_unificado["taxa_alfabetizacao_ano_anterior"] = df_unificado.groupby(
        ["nivel_geografico", "local_id"]
    )["taxa_alfabetizacao"].shift(1)
    df_unificado["variacao_pp"] = (
        df_unificado["taxa_alfabetizacao"] - df_unificado["taxa_alfabetizacao_ano_anterior"]
    ).round(2)

    logger.info(f"evolucao_temporal: {len(df_unificado)} linhas")
    return df_unificado.reset_index(drop=True)


def validar_evolucao_temporal(df):
    dq = DataQualityCheck(df, "gold.evolucao_temporal")
    dq.check_duplicidade(["nivel_geografico", "local_id", "ano"])
    dq.check_range("taxa_alfabetizacao", 0, 100)
    dq.logar(logger)
    dq.falhar_se_critico()


# ==================== GOLD 3: metas_vs_resultados ====================

# transforma as colunas meta_alfabetizacao_2024..2030 (wide) em linhas (long)
def _melt_metas(df, nivel_geografico, col_id=None, col_nome=None):
    meta_cols = [c for c in df.columns if c.startswith("meta_alfabetizacao_")]

    id_vars = ["ano", "taxa_alfabetizacao", "percentual_participacao"]
    if col_id:
        id_vars.append(col_id)
    if col_nome:
        id_vars.append(col_nome)

    # melt = o "despachante" de colunas pra linhas
    df_long = df.melt(
        id_vars=id_vars,
        value_vars=meta_cols,
        var_name="ano_meta_raw",
        value_name="valor_meta",
    )
    # extrai o ano do nome da coluna original (ex: meta_alfabetizacao_2025 -> 2025)
    df_long["ano_meta"] = df_long["ano_meta_raw"].str.extract(r"(\d{4})").astype(int)
    df_long = df_long.drop(columns=["ano_meta_raw"])

    df_long["nivel_geografico"] = nivel_geografico
    df_long["local_id"] = df_long[col_id] if col_id else "BR"
    df_long["local_nome"] = df_long[col_nome] if col_nome else "Brasil"
    df_long = df_long.rename(columns={
        "ano": "ano_resultado",
        "taxa_alfabetizacao": "taxa_alfabetizacao_real",
    })

    colunas_finais = [
        "nivel_geografico", "local_id", "local_nome",
        "ano_resultado", "taxa_alfabetizacao_real",
        "ano_meta", "valor_meta", "percentual_participacao",
    ]
    return df_long[colunas_finais]


def construir_metas_vs_resultados():
    logger.info("=== Construindo GOLD: metas_vs_resultados ===")

    # nota: meta_alfabetizacao_* nao tem coluna "rede" - ja vem consolidada
    # na fonte, entao aqui nao ha filtro de rede pra aplicar
    df_brasil = ler_silver_completa("meta_alfabetizacao_brasil")
    long_brasil = _melt_metas(df_brasil, "brasil")

    df_uf = ler_silver_completa("meta_alfabetizacao_uf")
    long_uf = _melt_metas(df_uf, "uf", col_id="sigla_uf", col_nome="nome")

    df_municipio = ler_silver_completa("meta_alfabetizacao_municipio")
    long_municipio = _melt_metas(df_municipio, "municipio", col_id="id_municipio", col_nome="nome")

    df_gold = pd.concat([long_brasil, long_uf, long_municipio], ignore_index=True)

    # "ano" precisa existir pro salvar_gold_local particionar certo
    df_gold["ano"] = df_gold["ano_resultado"]

    logger.info(f"metas_vs_resultados: {len(df_gold)} linhas")
    return df_gold


def validar_metas_vs_resultados(df):
    dq = DataQualityCheck(df, "gold.metas_vs_resultados")
    dq.check_duplicidade(["nivel_geografico", "local_id", "ano_resultado", "ano_meta"])
    dq.check_range("valor_meta", 0, 100)
    dq.logar(logger)
    dq.falhar_se_critico()


# ==================== EXECUCAO ====================

def main():
    # cada tabela segue o mesmo ciclo: constroi -> valida -> salva -> sobe
    df_indicador = construir_indicador_por_municipio()
    validar_indicador_por_municipio(df_indicador)
    for caminho in salvar_gold_local(df_indicador, "indicador_por_municipio"):
        upload_gold_s3(caminho, "indicador_por_municipio")

    df_evolucao = construir_evolucao_temporal()
    validar_evolucao_temporal(df_evolucao)
    for caminho in salvar_gold_local(df_evolucao, "evolucao_temporal"):
        upload_gold_s3(caminho, "evolucao_temporal")

    df_metas = construir_metas_vs_resultados()
    validar_metas_vs_resultados(df_metas)
    for caminho in salvar_gold_local(df_metas, "metas_vs_resultados"):
        upload_gold_s3(caminho, "metas_vs_resultados")

    logger.info("=== Gold nucleo concluida (3/3 tabelas) ===")


if __name__ == "__main__":
    main()