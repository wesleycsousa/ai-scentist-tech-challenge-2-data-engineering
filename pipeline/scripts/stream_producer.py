import os
import json          # serializar o evento (dict) para o formato que o Kafka manda pela rede
import time           # controlar o delay entre envios (simular chegada gradual)
import logging
import tempfile

import pandas as pd
import boto3
from kafka import KafkaProducer   # cliente Kafka - biblioteca kafka-python
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
# "bootstrap" = endereco inicial que o cliente usa pra descobrir o cluster Kafka
# no nosso caso, e o IP publico da EC2 + porta 9092

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

TOPIC = "alunos.eventos"
# nome do topico Kafka onde os eventos vao ser publicados
# (equivalente a "nome da tabela", mas para streaming)

N_MUNICIPIOS_AMOSTRA = 2
# quantos municipios usar na simulacao - pegamos os com MAIS linhas,
# nao aleatorio, pra ter uma amostra "cheia" e representativa

MAX_EVENTOS = 300
# teto de seguranca - mesmo que os municipios escolhidos tenham
# milhares de alunos, nao deixamos passar disso (senao a simulacao
# demoraria horas, dado o delay entre cada envio)

DELAY_SEGUNDOS = 0.3
# pausa entre cada mensagem publicada - e isso que simula "streaming"
# em vez de despejar tudo de uma vez


def ler_parquet_s3(s3_key):
    # baixa 1 arquivo parquet do S3 pra um arquivo temporario local e le com pandas
    # (mesma funcao no transform_silver.py)
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp.name
    tmp.close()                          # fecha antes do boto3 escrever (bug do Windows)
    s3_client.download_file(S3_BUCKET, s3_key, tmp_path)
    df = pd.read_parquet(tmp_path)
    os.remove(tmp_path)
    return df


def listar_arquivos(prefixo):
    # lista todos os .parquet dentro de uma "pasta" do S3
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                arquivos.append(obj["Key"])
    return arquivos


def carregar_amostra():
    """
    Le a tabela 'alunos' da BRONZE (dado bruto, sem traducao/enriquecimento -
    e assim que a aplicacao de origem "enviaria" o dado de verdade),
    escolhe os municipios com mais alunos, e corta pra no maximo MAX_EVENTOS.
    """
    logger.info("Lendo tabela alunos (bronze) do S3...")
    arquivos = listar_arquivos("bronze/br_inep_avaliacao_alfabetizacao/alunos/")
    dfs = [ler_parquet_s3(a) for a in arquivos]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total lido da bronze: {len(df)} linhas")

    # value_counts() conta quantas linhas cada municipio tem, ja ordenado
    # do maior pro menor - head(2) pega os 2 primeiros (mais alunos)
    municipios_top = df["id_municipio"].value_counts().head(N_MUNICIPIOS_AMOSTRA).index.tolist()
    amostra = df[df["id_municipio"].isin(municipios_top)].copy()
    logger.info(f"Municipios selecionados: {municipios_top} ({len(amostra)} linhas ao todo)")

    if len(amostra) > MAX_EVENTOS:
        # sample() escolhe linhas aleatorias dentro da amostra ja filtrada
        # random_state=42 fixa a "semente" aleatoria - rodando de novo,
        # sempre sai a MESMA amostra (reprodutibilidade)
        amostra = amostra.sample(n=MAX_EVENTOS, random_state=42)
        logger.info(f"Amostra reduzida para {MAX_EVENTOS} eventos (aleatoriamente, seed fixa)")

    return amostra


def criar_producer():
    # monta o cliente que vai publicar as mensagens no Kafka
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,

        # o Kafka so entende bytes - esses "serializers" convertem
        # o dado Python pro formato que viaja pela rede
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        # o evento inteiro (dict) vira JSON, depois vira bytes

        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        # a chave (id_municipio) vira texto, depois bytes

        acks="all",
        # espera confirmacao de TODAS as replicas antes de considerar
        # a mensagem "enviada de verdade" - garantia forte de entrega
        # (decisao de confiabilidade que documentamos)

        retries=5,
        # se der erro de rede, tenta reenviar ate 5 vezes antes de desistir
    )


def main():
    amostra = carregar_amostra()
    producer = criar_producer()

    logger.info(f"Publicando {len(amostra)} eventos no topico '{TOPIC}' (delay {DELAY_SEGUNDOS}s)...")
    enviados = 0

    # iterrows() percorre o DataFrame linha por linha - cada linha vira 1 evento
    for _, linha in amostra.iterrows():
        evento = linha.to_dict()
        # to_dict() transforma a linha do pandas num dicionario Python comum,
        # formato que o JSON entende

        future = producer.send(TOPIC, key=evento["id_municipio"], value=evento)
        # send() e assincrono por padrao - retorna um "future" (promessa),
        # nao trava esperando a confirmacao sozinho

        future.get(timeout=10)
        # aqui SIM esperamos a confirmacao (ate 10s) - deixa o envio
        # sincrono de proposito, pra sabermos na hora se algo falhou

        enviados += 1
        if enviados % 20 == 0:                # log de progresso a cada 20 envios
            logger.info(f"{enviados}/{len(amostra)} eventos publicados")

        time.sleep(DELAY_SEGUNDOS)             # a pausa que simula "tempo passando"

    producer.flush()   # garante que nada ficou pendente no buffer interno
    producer.close()   # fecha a conexao com o Kafka de forma limpa
    logger.info(f"Publicacao concluida: {enviados} eventos enviados")


if __name__ == "__main__":
    main()