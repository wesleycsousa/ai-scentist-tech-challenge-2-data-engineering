import os
import json          # converter bytes recebidos de volta para dict Python
import time
import logging
import tempfile
from datetime import date

import pandas as pd
import boto3
from kafka import KafkaConsumer   # cliente Kafka para LER mensagens (contraparte do KafkaProducer)
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
DATA_EXECUCAO = date.today().isoformat()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

# mesmo topico que o producer publicou
TOPIC = "alunos.eventos"

# identifica esse consumer perante o Kafka - permite retomar de onde parou
GROUP_ID = "consumer-bronze-streaming"


def criar_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,

        # se esse group_id nunca leu antes, comeca do INICIO do log
        auto_offset_reset="earliest",

        # confirma automaticamente ate onde ja processou
        enable_auto_commit=True,

        # inverso dos serializers do producer: bytes -> dict Python
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,

        # para de esperar apos 10s sem mensagem nova
        consumer_timeout_ms=10000,
    )


def salvar_streaming_local(eventos):
    # lista de dicts vira DataFrame direto
    df = pd.DataFrame(eventos)

    base_dir = "pipeline/data/tmp/bronze_streaming/alunos"
    os.makedirs(base_dir, exist_ok=True)

    path = f"{base_dir}/data_execucao={DATA_EXECUCAO}.parquet"
    df.to_parquet(path, index=False)

    logger.info(f"{len(df)} eventos salvos localmente em {path}")
    return path


def upload_streaming_s3(caminho_local):
    nome_arquivo = os.path.basename(caminho_local)

    # pasta separada da bronze batch - mesma camada, origem diferente
    s3_key = f"bronze/streaming/alunos/{nome_arquivo}"

    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")


def main():
    consumer = criar_consumer()
    logger.info(f"Consumindo topico {TOPIC} (aguarda ate 10s sem mensagem nova pra parar)...")

    eventos = []
    # entrega uma mensagem de cada vez, conforme chegam (ou ja esperando)
    for msg in consumer:
        eventos.append(msg.value)
        if len(eventos) % 50 == 0:
            logger.info(f"{len(eventos)} eventos consumidos ate agora")

    # loop termina sozinho apos o timeout de 10s sem mensagem nova
    consumer.close()
    logger.info(f"Consumo finalizado: {len(eventos)} eventos recebidos")

    if not eventos:
        logger.warning("Nenhum evento recebido - nada para salvar")
        return

    caminho = salvar_streaming_local(eventos)
    upload_streaming_s3(caminho)
    logger.info("Consumer concluido com sucesso")


if __name__ == "__main__":
    main()