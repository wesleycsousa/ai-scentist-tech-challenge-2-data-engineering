import os
from datetime import date

from utils import (
    ler_tabela_completa,
    limpar_prefixo_s3,
    upload_s3,
    s3_client,
    S3_BUCKET,
    logger,
)
from data_quality import DataQualityCheck

DATA_EXECUCAO = date.today().isoformat()

TABLE_ID = "indicador_x_infraestrutura_escolar"

# infraestrutura so tem dado de 2024 - aplicamos o mesmo valor aos anos
# do nucleo (2023 e 2024), assumindo que infraestrutura fisica de escola
# nao muda drasticamente de um ano pro outro. Decisao registrada.
ANO_REFERENCIA_INFRAESTRUTURA = 2024


def salvar_gold_local(df):
    caminhos = []
    base_dir = f"pipeline/data/tmp/gold/{TABLE_ID}"
    os.makedirs(base_dir, exist_ok=True)
    for ano, grupo in df.groupby("ano"):
        path = f"{base_dir}/ano={ano}/data_execucao={DATA_EXECUCAO}.parquet"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        grupo.to_parquet(path, index=False)
        caminhos.append(path)
    return caminhos


def construir_gold_enriquecida():
    logger.info(f"=== Construindo GOLD ENRIQUECIDA: {TABLE_ID} ===")

    # gold nucleo - tem 2023 e 2024
    df_indicador = ler_tabela_completa("gold/indicador_por_municipio/")

    # silver do enriquecimento - so tem 2024
    df_infra = ler_tabela_completa("silver/escola_por_municipio/")

    # remove "ano" da infraestrutura antes do join - sempre vale 2024 e
    # entraria em conflito com o "ano" do indicador (que varia)
    df_infra = df_infra.drop(columns=["ano"])

    # left join: mantem TODAS as linhas do indicador (2023+2024), mesmo
    # sem match na infraestrutura (fica nulo - esperado, nao e falha)
    df_gold = df_indicador.merge(df_infra, on="id_municipio", how="left")

    df_gold["ano_referencia_infraestrutura"] = ANO_REFERENCIA_INFRAESTRUTURA

    logger.info(f"{TABLE_ID}: {len(df_gold)} linhas")
    return df_gold


def validar_gold_enriquecida(df):
    dq = DataQualityCheck(df, f"gold.{TABLE_ID}")
    dq.check_duplicidade(["ano", "id_municipio"])
    # nulos em pct_escolas_* SAO esperados (municipio sem match) - nao
    # checamos nulos aqui, so validamos que os percentuais existentes
    # sao plausiveis
    dq.check_range("pct_escolas_agua_potavel", 0, 100)
    dq.check_range("pct_escolas_internet", 0, 100)
    dq.check_range("pct_escolas_equipamento_computador", 0, 100)
    dq.check_range("pct_escolas_biblioteca", 0, 100)
    dq.check_range("pct_escolas_laboratorio_informatica", 0, 100)
    dq.check_range("pct_escolas_esgoto_rede_publica", 0, 100)
    dq.logar(logger)
    dq.falhar_se_critico()


def main():
    limpar_prefixo_s3(f"gold/{TABLE_ID}/")  # garante idempotencia

    df_gold = construir_gold_enriquecida()
    validar_gold_enriquecida(df_gold)

    for caminho in salvar_gold_local(df_gold):
        caminho_relativo = caminho.replace(f"pipeline/data/tmp/gold/{TABLE_ID}/", "")
        s3_key = f"gold/{TABLE_ID}/{caminho_relativo}"
        upload_s3(caminho, s3_key)

    logger.info("=== Gold enriquecida concluida ===")


if __name__ == "__main__":
    main()
