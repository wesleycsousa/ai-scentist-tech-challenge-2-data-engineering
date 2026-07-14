import os              # pra mexer com caminhos de arquivo e variaveis de ambiente
import tempfile         # pra criar arquivos temporarios (usado pra baixar o parquet do S3 antes de ler)
import logging          # pra registrar mensagens de progresso/erro no terminal, em vez de usar print()
from datetime import date   # pra pegar a data de hoje e usar no nome dos arquivos salvos

import pandas as pd     # a biblioteca que manipula os dados em formato de tabela (DataFrame)
import boto3            # biblioteca oficial da AWS - e o que fala com o S3
from dotenv import load_dotenv   # le as variaveis do arquivo .env (credenciais, nome do bucket, etc)

import gc                # garbage collector - forca o Python a liberar memoria de variaveis que ja usei


# carrega as variaveis do .env pra dentro do programa (S3_BUCKET, chaves da AWS, etc)
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")        # nome do bucket, vem do .env
DATA_EXECUCAO = date.today().isoformat()   # data de hoje, tipo "2026-07-13" - vai no nome dos arquivos salvos

# configura como as mensagens de log vao aparecer: hora | nivel | mensagem
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# cliente que efetivamente conversa com o S3 (upload, download, listar arquivos)
s3_client = boto3.client("s3")


def ler_parquet_s3(s3_key: str) -> pd.DataFrame:
    """
    Baixa UM arquivo parquet do S3 e le ele como DataFrame.
    Precisa passar por um arquivo temporario porque o pandas le do disco,
    nao le direto de uma "chave" do S3.
    """
    # cria o arquivo temporario e ja fecha ele na hora (Windows nao deixa
    # outro programa escrever num arquivo que ainda esta aberto - foi bug que corrigimos)
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp.name
    tmp.close()

    s3_client.download_file(S3_BUCKET, s3_key, tmp_path)  # baixa do S3 pro caminho temporario
    df = pd.read_parquet(tmp_path)   # le o arquivo baixado como tabela

    os.remove(tmp_path)   # apaga o arquivo temporario, ja nao precisa mais dele
    return df


def listar_arquivos(prefixo: str) -> list[str]:
    """
    Lista todos os arquivos .parquet dentro de uma "pasta" do S3.
    Usa paginator porque o S3 pode ter mais arquivos do que cabe numa resposta so
    (aqui na pratica nunca deve precisar de mais de 1 pagina, mas e o jeito seguro de fazer).
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                arquivos.append(obj["Key"])
    return arquivos


def ler_tabela_bronze_completa(dataset_id: str, table_id: str) -> pd.DataFrame:
    """
    Le TODOS os arquivos parquet de uma tabela da Bronze e junta num DataFrame so.
    Precisa disso porque na ingestao eu particionei por ano - entao uma tabela
    "uf", por exemplo, vira 2 arquivos separados (ano=2023 e ano=2024) no S3.
    O pd.concat "empilha" esses pedacos de volta numa tabela unica.
    """
    arquivos = listar_arquivos(f"bronze/{dataset_id}/{table_id}/")
    dfs = [ler_parquet_s3(a) for a in arquivos]   # baixa e le cada arquivo, um por um
    df_completo = pd.concat(dfs, ignore_index=True)   # junta tudo numa tabela unica
    logger.info(f"{dataset_id}.{table_id}: {len(df_completo)} linhas lidas de {len(arquivos)} arquivo(s)")
    return df_completo


def montar_mapa_dicionario(dicionario_df: pd.DataFrame, id_tabela: str, nome_coluna: str) -> dict:
    """
    Pega a tabela "dicionario" (que traduz codigo numerico pra texto, tipo 2 = "Estadual")
    e filtra so as linhas que interessam pra uma coluna especifica de uma tabela especifica.
    Depois monta um dict tipo {"1": "Federal", "2": "Estadual", ...} - fica facil de usar
    com .map() depois pra traduzir a coluna inteira de uma vez.
    """
    filtro = (dicionario_df["id_tabela"] == id_tabela) & (dicionario_df["nome_coluna"] == nome_coluna)
    subset = dicionario_df[filtro]
    return dict(zip(subset["chave"].astype(str), subset["valor"]))


def salvar_silver_local(df: pd.DataFrame, table_id: str) -> list[str]:
    """
    Salva o DataFrame ja tratado como parquet, no meu computador (rascunho antes do upload),
    particionado por ano - mesmo padrao que usei na ingestao da Bronze.
    Retorna a lista de caminhos gerados, pra eu saber o que subir pro S3 depois.
    """
    caminhos = []
    base_dir = f"pipeline/data/tmp/silver/{table_id}"
    os.makedirs(base_dir, exist_ok=True)

    for ano, grupo in df.groupby("ano"):   # separa o DataFrame em pedacos, um por ano
        path = f"{base_dir}/ano={ano}/data_execucao={DATA_EXECUCAO}.parquet"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        grupo.to_parquet(path, index=False)
        caminhos.append(path)

    return caminhos


def upload_silver_s3(caminho_local: str, table_id: str):
    """Sobe um arquivo parquet ja salvo localmente pra pasta silver/ do bucket no S3."""
    caminho_relativo = caminho_local.replace(f"pipeline/data/tmp/silver/{table_id}/", "")
    s3_key = f"silver/{table_id}/{caminho_relativo}"

    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")


def transformar_geografia(table_id: str, diretorio_table_id: str, chave_join_bronze: str, chave_join_diretorio: str) -> pd.DataFrame:
    """
    Transformacao generica pra tabelas geograficas (uf e municipio).
    Uso a mesma funcao pras duas porque a logica e identica - so muda
    qual tabela, qual diretorio, e qual coluna usar pra cruzar (JOIN).
    """
    logger.info(f"=== Transformando tabela {table_id.upper()} ===")

    # le as 3 fontes que preciso: a tabela bruta, o dicionario (traducao) e o diretorio (nomes)
    df_bruta = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", table_id)
    df_dicionario = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "dicionario")
    df_diretorio = ler_tabela_bronze_completa("br_bd_diretorios_brasil", diretorio_table_id)

    # traduz a coluna "rede" (numero) pra texto (ex: 2 -> "Estadual"), cria coluna nova
    mapa_rede = montar_mapa_dicionario(df_dicionario, table_id, "rede")
    df_bruta["rede_descricao"] = df_bruta["rede"].astype(str).map(mapa_rede)

    # JOIN (merge) com o diretorio, pra trazer nome/regiao que a tabela bruta nao tem
    # how="left" garante que nao perco nenhuma linha da tabela bruta, mesmo se nao achar par
    df_enriquecido = df_bruta.merge(
        df_diretorio,
        left_on=chave_join_bronze,
        right_on=chave_join_diretorio,
        how="left",
        suffixes=("", "_diretorio"),   # evita conflito se as duas tabelas tiverem coluna com mesmo nome
    )

    logger.info(f"{table_id.upper()} enriquecida: {len(df_enriquecido)} linhas, {df_enriquecido.shape[1]} colunas")
    return df_enriquecido


def transformar_alunos() -> pd.DataFrame:
    """
    Transformacao especifica da tabela alunos - separei da funcao generica porque
    aqui tem mais de uma coluna codificada pra traduzir (nao so "rede"), e o volume
    e bem maior (quase 4 milhoes de linhas), entao precisei cuidar de memoria tambem.
    """
    logger.info("=== Transformando tabela ALUNOS ===")

    df_alunos = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "alunos")
    df_dicionario = ler_tabela_bronze_completa("br_inep_avaliacao_alfabetizacao", "dicionario")
    df_diretorio_municipio = ler_tabela_bronze_completa("br_bd_diretorios_brasil", "municipio")

    # o diretorio de municipio tem 27 colunas, mas so preciso de 5 delas.
    # cortar isso ANTES do merge foi o que resolveu o erro de falta de memoria
    # (juntar 4 milhoes de linhas com 27 colunas extras tava estourando a RAM)
    colunas_uteis_diretorio = ["id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao"]
    df_diretorio_municipio = df_diretorio_municipio[colunas_uteis_diretorio]

    # aqui tem 4 colunas codificadas (nao so "rede" como nas tabelas geograficas)
    # entao percorro cada uma no loop e traduzo, uma de cada vez
    colunas_codificadas = ["rede", "alfabetizado", "presenca", "preenchimento_caderno"]
    for coluna in colunas_codificadas:
        mapa = montar_mapa_dicionario(df_dicionario, "alunos", coluna)
        if mapa:   # so cria a coluna traduzida se realmente existir traducao no dicionario
            df_alunos[f"{coluna}_descricao"] = df_alunos[coluna].astype(str).map(mapa)
        else:
            logger.info(f"Sem mapeamento no dicionario para 'alunos.{coluna}' - mantendo como esta")

    # junta com o diretorio (ja reduzido) pra trazer nome do municipio/UF/regiao
    df_enriquecido = df_alunos.merge(
        df_diretorio_municipio,
        on="id_municipio",   # nome da coluna e igual nas duas tabelas, entao uso "on" em vez de left_on/right_on
        how="left",
        suffixes=("", "_diretorio"),
    )

    logger.info(f"ALUNOS enriquecida: {len(df_enriquecido)} linhas, {df_enriquecido.shape[1]} colunas")
    return df_enriquecido


if __name__ == "__main__":
    # processo uf, municipio e alunos em sequencia.
    # depois de cada uma, apago a variavel (del) e forco a limpeza de memoria (gc.collect())
    # pra nao acumular RAM usada de tabela em tabela - foi o que evitou o erro de memoria no alunos

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

    logger.info("=== Transformacao da Silver concluida (uf, municipio, alunos) ===")