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

TOPIC = "alunos.eventos"
# mesmo topico que o producer publicou - o consumer precisa "assinar"
# exatamente esse nome pra receber as mensagens certas

GROUP_ID = "consumer-bronze-streaming"
# identifica ESSE consumer (ou grupo de consumers) perante o Kafka.
# o Kafka usa isso pra lembrar "ate onde esse grupo ja leu" (offset).
# se rodar de novo com o MESMO group_id, ele so traria mensagens NOVAS,
# nao as que ja foram lidas antes


def criar_consumer():
    return KafkaConsumer(
        TOPIC,
        # topico(s) que esse consumer vai "escutar" - pode ser mais de um

        bootstrap_servers=KAFKA_BOOTSTRAP,
        # mesmo endereco (IP da EC2 + porta 9092) que o producer usou

        group_id=GROUP_ID,

        auto_offset_reset="earliest",
        # o que fazer se esse group_id NUNCA leu nada desse topico antes:
        # "earliest"   = comeca do INICIO do log (le tudo que ja existe)
        # "latest"     = comeca so a partir de AGORA, ignora o que ja passou
        # escolhemos "earliest" de proposito, pra pegar os 300 eventos
        # que ja estavam esperando

        enable_auto_commit=True,
        # confirma automaticamente, em intervalos, ate onde o consumer
        # ja processou - assim, se rodar de novo, nao le tudo de novo

        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        # o inverso EXATO dos serializers do producer:
        # bytes -> texto (decode) -> dict Python (json.loads)

        consumer_timeout_ms=10000,
        # depois de 10s SEM nenhuma mensagem nova chegando, para de
        # esperar e sai do loop. Sem isso, o script ficaria escutando
        # pra sempre (comportamento normal em streaming real, mas
        # aqui queremos que o script termine sozinho)
    )


def salvar_streaming_local(eventos):
    # eventos e uma LISTA de dicionarios (um por mensagem recebida) -
    # pd.DataFrame(lista_de_dicts) monta a tabela direto a partir dela
    df = pd.DataFrame(eventos)

    base_dir = "pipeline/data/tmp/bronze_streaming/alunos"
    os.makedirs(base_dir, exist_ok=True)

    path = f"{base_dir}/data_execucao={DATA_EXECUCAO}.parquet"
    df.to_parquet(path, index=False)

    logger.info(f"{len(df)} eventos salvos localmente em {path}")
    return path


def upload_streaming_s3(caminho_local):
    nome_arquivo = os.path.basename(caminho_local)
    # pega so o nome do arquivo (sem o caminho de pastas local),
    # pra montar a chave (key) certa no S3

    s3_key = f"bronze/streaming/alunos/{nome_arquivo}"
    # pasta separada da bronze "batch" (bronze/br_inep.../alunos/) -
    # mesma camada Bronze, mas evidenciando que veio via streaming

    s3_client.upload_file(caminho_local, S3_BUCKET, s3_key)
    logger.info(f"Upload OK: s3://{S3_BUCKET}/{s3_key}")


def main():
    consumer = criar_consumer()
    logger.info(f"Consumindo topico {TOPIC} (aguarda ate 10s sem mensagem nova pra parar)...")

    eventos = []
    for msg in consumer:
        # "for msg in consumer" e um loop especial - ele fica "escutando"
        # o Kafka e entrega uma mensagem de cada vez, assim que chegam
        # (ou ja tiver muitas esperando, como foi o nosso caso)

        eventos.append(msg.value)
        # msg tem varias infos (topico, partition, offset, key, timestamp),
        # mas aqui so guardamos o VALUE (o dict do aluno em si)

        if len(eventos) % 50 == 0:          # log de progresso a cada 50
            logger.info(f"{len(eventos)} eventos consumidos ate agora")

    # o loop "for msg in consumer" so termina quando bate o timeout
    # de 10s sem mensagem nova (consumer_timeout_ms) - foi ai que
    # ele saiu do loop sozinho

    consumer.close()
    logger.info(f"Consumo finalizado: {len(eventos)} eventos recebidos")

    if not eventos:
        # protecao: se por algum motivo nao chegou nenhuma mensagem,
        # nao faz sentido tentar salvar um DataFrame vazio
        logger.warning("Nenhum evento recebido - nada para salvar")
        return

    caminho = salvar_streaming_local(eventos)
    upload_streaming_s3(caminho)
    logger.info("Consumer concluido com sucesso")


if __name__ == "__main__":
    main()