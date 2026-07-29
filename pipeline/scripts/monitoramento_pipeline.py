import os
from datetime import date, datetime

from utils import listar_arquivos, s3_client, S3_BUCKET, logger


CAMADAS = {
    "Bronze (principal)": "bronze/br_inep_avaliacao_alfabetizacao/",
    "Bronze (diretorios)": "bronze/br_bd_diretorios_brasil/",
    "Bronze (enriquecimento)": "bronze/br_inep_censo_escolar/",
    "Bronze (streaming)": "bronze/streaming/",
    "Silver (principal)": "silver/",
    "Gold (nucleo + enriquecida)": "gold/",
}


def analisar_prefixo(prefixo):
    """Para um prefixo do S3, retorna: qtd de arquivos, tamanho total,
    e a data de execucao mais recente encontrada nos nomes dos arquivos."""
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        arquivos.extend(page.get("Contents", []))

    if not arquivos:
        return {"qtd_arquivos": 0, "tamanho_mb": 0, "ultima_execucao": None}

    tamanho_total_mb = sum(a["Size"] for a in arquivos) / (1024 * 1024)

    # cada arquivo tem "LastModified" - usamos isso como proxy de quando
    # rodou, sem precisar parsear o nome do arquivo (mais confiavel)
    ultima_execucao = max(a["LastModified"] for a in arquivos)

    return {
        "qtd_arquivos": len(arquivos),
        "tamanho_mb": round(tamanho_total_mb, 2),
        "ultima_execucao": ultima_execucao,
    }


def gerar_relatorio():
    logger.info("=== Relatorio de Monitoramento do Pipeline ===")
    logger.info(f"Gerado em: {datetime.now().isoformat()}\n")

    linhas_relatorio = []
    for nome_camada, prefixo in CAMADAS.items():
        info = analisar_prefixo(prefixo)

        # sinaliza se a camada esta "velha" (sem execucao ha mais de 7 dias)
        # - proxy simples para "possivel falha ou pipeline parado"
        status = "OK"
        if info["ultima_execucao"] is None:
            status = "VAZIO - nunca rodou ou foi apagado"
        else:
            dias_desde_execucao = (
                datetime.now(info["ultima_execucao"].tzinfo) - info["ultima_execucao"]
            ).days
            if dias_desde_execucao > 7:
                status = f"ATENCAO - sem execucao ha {dias_desde_execucao} dias"

        logger.info(
            f"{nome_camada:35} | {info['qtd_arquivos']:3} arquivos | "
            f"{info['tamanho_mb']:8.2f} MB | ultima: {info['ultima_execucao']} | {status}"
        )

        linhas_relatorio.append({
            "camada": nome_camada,
            "prefixo": prefixo,
            **info,
            "status": status,
        })

    return linhas_relatorio


def salvar_relatorio_local(linhas):
    import pandas as pd
    df = pd.DataFrame(linhas)
    os.makedirs("pipeline/docs", exist_ok=True)
    path = f"pipeline/docs/monitoramento_{date.today().isoformat()}.csv"
    df.to_csv(path, index=False)
    logger.info(f"\nRelatorio salvo em: {path}")


def main():
    linhas = gerar_relatorio()
    salvar_relatorio_local(linhas)


if __name__ == "__main__":
    main()
