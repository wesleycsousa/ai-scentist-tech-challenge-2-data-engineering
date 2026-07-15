# Setup da instância EC2 para Kafka (Streaming simulado)

## Por que EC2 (não local, não Kinesis/MSK)

Ver decisão registrada no documento de decisões arquiteturais — resumo: Kafka
precisa estar hospedado na AWS para atender ao requisito de "Implementação em
Cloud" do enunciado; EC2 free tier evita o custo de MSK/Kinesis mantendo a
mesma tecnologia (Kafka real, via Docker).

## Especificação da instância

- **AMI:** Amazon Linux 2023 (free tier eligible)
- **Instance type:** t2.micro / t3.micro
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

## Encerramento (importante para FinOps)

Ao final do projeto, ou quando não estiver em uso ativo:
Parar a instância (não cobra compute, mas mantém o disco)
via console AWS: EC2 -> Instances -> Stop instance
Ou terminar definitivamente (remove tudo, inclusive o disco)
via console AWS: EC2 -> Instances -> Terminate instance