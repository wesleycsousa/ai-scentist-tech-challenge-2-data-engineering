import os
import tempfile
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
s3_client = boto3.client("s3")


def listar_arquivos_bronze(prefixo="bronze/"):
    """Lista todos os arquivos .parquet dentro de bronze/, agrupados por dataset/tabela."""
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                arquivos.append(obj["Key"])
    return arquivos


def ler_parquet_s3(s3_key: str) -> pd.DataFrame:
    """Baixa um arquivo parquet do S3 para um arquivo temporário local e le com pandas."""
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp.name
    tmp.close()  # fecha o handle imediatamente - só precisamos do caminho, não do arquivo aberto

    s3_client.download_file(S3_BUCKET, s3_key, tmp_path)
    df = pd.read_parquet(tmp_path)

    os.remove(tmp_path)
    return df

def agrupar_por_tabela(arquivos: list[str]) -> dict:
    """Agrupa os caminhos S3 por dataset/tabela, guardando só 1 arquivo de exemplo por tabela."""
    grupos = {}
    for caminho in arquivos:
        # Ex: bronze/br_inep_avaliacao_alfabetizacao/alunos/ano=2023/data_execucao=...parquet
        partes = caminho.split("/")
        dataset_id = partes[1]
        table_id = partes[2]
        chave = f"{dataset_id}.{table_id}"
        grupos.setdefault(chave, []).append(caminho)
    return grupos


def gerar_relatorio_schema(grupos: dict) -> pd.DataFrame:
    linhas_relatorio = []
    for tabela, arquivos_da_tabela in grupos.items():
        # pega só o primeiro arquivo como amostra representativa do schema
        exemplo = arquivos_da_tabela[0]
        df = ler_parquet_s3(exemplo)

        for coluna in df.columns:
            linhas_relatorio.append({
                "tabela": tabela,
                "arquivos_na_tabela": len(arquivos_da_tabela),
                "coluna": coluna,
                "tipo": str(df[coluna].dtype),
                "linhas_no_arquivo_exemplo": len(df),
                "nulos_no_arquivo_exemplo": df[coluna].isnull().sum(),
                "pct_nulos": round(df[coluna].isnull().mean() * 100, 1),
            })

    return pd.DataFrame(linhas_relatorio)
if __name__ == "__main__":
    arquivos = listar_arquivos_bronze()
    print(f"Total de arquivos encontrados: {len(arquivos)}")

    grupos = agrupar_por_tabela(arquivos)
    print(f"\nTabelas encontradas: {len(grupos)}")
    for tabela, arqs in grupos.items():
        print(f" - {tabela}: {len(arqs)} arquivo(s)")

    print("\nGerando relatório de schema...")
    relatorio = gerar_relatorio_schema(grupos)

    # salva localmente pra consulta posterior
    relatorio.to_csv("pipeline/docs/bronze_schema_report.csv", index=False)
    print("\nRelatório salvo em pipeline/docs/bronze_schema_report.csv")
    print(relatorio.to_string())