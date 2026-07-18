# Setup da instância EC2 para Kafka (Streaming simulado)

## Por que EC2

Ver decisão registrada no documento de decisões arquiteturais — resumo: Kafka
precisa estar hospedado na AWS para atender ao requisito de "Implementação em
Cloud" do enunciado; EC2 free tier evita o custo de MSK/Kinesis mantendo a
mesma tecnologia (Kafka real, via Docker).

## Especificação da instância

- **AMI:** Amazon Linux 2023 (free tier eligible)
- **Instance type:** t2.micro
- **Security Group:**
  - SSH (porta 22) — origem: My IP
  - Custom TCP (porta 9092) — origem: My IP
- **Key pair:** gerada na criação, mantida localmente em `~/.ssh/` (fora do repositório)
- **Tag:** project = tech-challenge-fase2

## Passos para reproduzir

### 1. Criar a instância
Console AWS → EC2 → Launch instance, conforme especificação acima.

### 2. Conectar via SSH
ssh -i "~/.ssh/kafka-tech-challenge-key.pem" ec2-user@<IP-PUBLICO>

### 3. Instalar Docker
```bash
sudo dnf update -y
sudo dnf install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user
# sair e reconectar via SSH para aplicar o grupo
```

### 4. Instalar Docker Compose
```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

### 5. Copiar o docker-compose.yml deste repositório para a instância
scp -i "~/.ssh/kafka-tech-challenge-key.pem" pipeline/streaming/docker-compose.yml ec2-user@<IP-PUBLICO>:~/kafka-streaming/

### 6. Subir os containers
```bash
cd ~/kafka-streaming
docker compose up -d
docker compose ps
```

## Permissões IAM adicionais necessárias

Além da política base (S3, Glue, Athena), foi necessário adicionar ao
usuário do projeto:

```json
{
  "Sid": "EC2KafkaInstance",
  "Effect": "Allow",
  "Action": [
    "ec2:RunInstances",
    "ec2:TerminateInstances",
    "ec2:StopInstances",
    "ec2:StartInstances",
    "ec2:DescribeInstances",
    "ec2:DescribeInstanceStatus",
    "ec2:CreateTags",
    "ec2:CreateSecurityGroup",
    "ec2:AuthorizeSecurityGroupIngress",
    "ec2:DescribeSecurityGroups",
    "ec2:CreateKeyPair",
    "ec2:DescribeKeyPairs",
    "ec2:DeleteKeyPair",
    "ec2:DescribeImages",
    "ec2:DescribeAvailabilityZones",
    "ec2:DescribeSubnets",
    "ec2:DescribeVpcs"
  ],
  "Resource": "*"
}
```

**Nota de limitação conhecida:** diferente das políticas de S3/Glue, essas
permissões de EC2 usam `Resource: "*"` sem restrição por tag ou prefixo de
nome, já que a API de EC2 tem suporte mais limitado a isso na criação de
recursos. Em um cenário de produção, o ideal seria refinar com `Condition`
baseada em tag.

## Encerramento

Ao final do projeto, ou quando não estiver em uso ativo:
Parar a instância (não cobra compute, mas mantém o disco)
via console AWS: EC2 -> Instances -> Stop instance
Ou terminar definitivamente (remove tudo, inclusive o disco)
via console AWS: EC2 -> Instances -> Terminate instance
## Troubleshooting — problemas encontrados e soluções

### 1. Kafka morre ao iniciar (falta de memória)

**Sintoma:** container `kafka` aparece com status `Exited (1)` logo após subir;
logs mostram `Native memory allocation (mmap) failed to map 1073741824 bytes`.

**Causa:** a imagem `confluentinc/cp-kafka` tenta reservar 1GB de heap por
padrão, mas instâncias `t2.micro`/`t3.micro` só têm 1GB de RAM no total
(já ocupada em parte pelo sistema operacional + Zookeeper).

**Solução:** adicionar `KAFKA_HEAP_OPTS` no `docker-compose.yml`, limitando
o heap a um valor menor:`KAFKA_HEAP_OPTS: "-Xmx400M -Xms400M`
### 2. Producer/consumer não conseguem conectar de fora da instância

**Sintoma:** `KafkaTimeoutError: Unable to bootstrap` mesmo com o container
`kafka` rodando (`Up`) e a porta 9092 liberada no Security Group.

**Causa:** `KAFKA_ADVERTISED_LISTENERS` estava configurado com
`PLAINTEXT_HOST://0.0.0.0:9092`. O Kafka rejeita `0.0.0.0` nesse campo
especificamente, pois é o endereço que ele "divulga" para clientes externos
se conectarem de volta — `0.0.0.0` não é um endereço roteável real.

**Solução:** trocar pelo IP público real da instância: KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://<IP-PUBLICO-DA-EC2>:9092 

**Limitação conhecida:** o IP público muda se a instância for parada e
iniciada novamente (sem Elastic IP associado) — nesse caso, é necessário
atualizar essa variável e reiniciar os containers (`docker compose down && docker compose up -d`).

### 3. Security Group bloqueando após IP local mudar

**Sintoma:** `Connection timed out` (SSH ou porta 9092), mesmo com a regra
já configurada anteriormente.

**Causa:** IP público doméstico é dinâmico e mudou desde a última configuração
da regra.

**Solução:** antes de cada sessão de trabalho, confirmar o IP atual e
comparar com a regra:
```powershell
(Invoke-RestMethod -Uri "https://api.ipify.org")
```
Atualizar a regra via "Edit inbound rules" no Security Group, usando o
botão "My IP" para preencher automaticamente.
