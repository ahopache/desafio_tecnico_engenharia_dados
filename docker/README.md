# Docker - SiCooperative Data Lake POC

Este diretório contém os arquivos Docker para execução automatizada do pipeline ETL.

## 📦 Arquivos

- **`docker-compose.yml`**: Orquestração dos serviços (MySQL + Spark)
- **`Dockerfile`**: Imagem customizada do ambiente Spark
- **`run-pipeline.sh`**: Script de execução para Linux/Mac
- **`run-pipeline.bat`**: Script de execução para Windows

## 🚀 Início Rápido

### 1. Subir o Ambiente

```bash
# No diretório docker/
docker-compose up -d
```

**O que acontece:**
- MySQL é iniciado na porta 3306
- Scripts SQL são executados automaticamente (`01_create_schema.sql`, `02_insert_data.sql`)
- Container Spark é criado com todas as dependências

### 2. Verificar Status

```bash
docker-compose ps
```

Você deve ver:
```
NAME                    STATUS              PORTS
sicooperative-mysql     Up (healthy)        0.0.0.0:3306->3306/tcp
sicooperative-spark     Up                  
```

### 3. Executar o Pipeline

**Opção A: Script Automatizado (Recomendado)**

```bash
# Linux/Mac
./run-pipeline.sh

# Windows
run-pipeline.bat
```

**Opção B: Comando Direto**

```bash
docker-compose exec spark python src/etl_pipeline.py
```

**Opção C: Com Argumentos Customizados**

```bash
docker-compose exec spark python src/etl_pipeline.py --output /app/output --log-level DEBUG
```

### 4. Verificar Resultado

```bash
# Listar arquivos gerados
ls -lh ../output/

# Ver primeiras linhas do CSV
head ../output/movimento_flat.csv
```

### 5. Parar o Ambiente

```bash
# Parar containers (mantém dados)
docker-compose stop

# Parar e remover containers (mantém volumes)
docker-compose down

# Remover tudo (incluindo dados do MySQL)
docker-compose down -v
```

## 🔧 Comandos Úteis

### Logs

```bash
# Ver logs de todos os serviços
docker-compose logs -f

# Ver logs apenas do MySQL
docker-compose logs -f mysql

# Ver logs apenas do Spark
docker-compose logs -f spark
```

### Acessar Containers

```bash
# Acessar shell do MySQL
docker-compose exec mysql mysql -u root -p sicooperative_db

# Acessar shell do Spark
docker-compose exec spark bash

# Executar query SQL (senha será solicitada)
docker-compose exec mysql mysql -u root -p sicooperative_db -e "SELECT COUNT(*) FROM movimento;"
```

### Rebuild

```bash
# Rebuild da imagem Spark (após mudanças no Dockerfile)
docker-compose build --no-cache spark

# Rebuild e restart
docker-compose up -d --build
```

## 🏗️ Arquitetura

## 🐛 Troubleshooting

### Problema: MySQL não inicia

```bash
# Ver logs
docker-compose logs mysql

# Verificar se porta 3306 já está em uso
netstat -an | grep 3306  # Linux/Mac
netstat -an | findstr 3306  # Windows

# Remover volume e recriar
docker-compose down -v
docker-compose up -d
```

### Problema: Scripts SQL não executam

```bash
# Verificar se scripts estão montados
docker-compose exec mysql ls -la /docker-entrypoint-initdb.d/

# Forçar execução manual (senha será solicitada)
docker-compose exec mysql mysql -u root -p sicooperative_db < ../sql/01_create_schema.sql
docker-compose exec mysql mysql -u root -p sicooperative_db < ../sql/02_insert_data.sql
```

### Problema: Spark não encontra MySQL

```bash
# Verificar conectividade
docker-compose exec spark ping -c 3 mysql

# Verificar se MySQL está healthy
docker-compose ps

# Testar conexão JDBC
docker-compose exec spark python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('jdbc').option('url', 'jdbc:mysql://mysql:3306/sicooperative_db').option('user', 'root').option('password', open('/run/secrets/mysql_root_password').read().strip()).option('driver', 'com.mysql.cj.jdbc.Driver').option('dbtable', '(SELECT 1) AS test').load()
print(df.count())
"
```

### Problema: Permissões no Windows

```powershell
# Executar PowerShell como Administrador
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

# Ou usar Docker Desktop com WSL2
```

## 📚 Recursos Adicionais

- [Docker Secrets Documentation](https://docs.docker.com/engine/swarm/secrets/)
- [MySQL Security Best Practices](https://dev.mysql.com/doc/refman/8.0/en/security.html)
- [Password Security Guidelines](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                    DOCKER COMPOSE                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────┐         ┌─────────────────────┐    │
│  │   MySQL Container   │         │   Spark Container   │    │
│  ├─────────────────────┤         ├─────────────────────┤    │
│  │ - MySQL 8.0         │◄────────┤ - Python 3.10       │    │
│  │ - Port 3306         │  JDBC   │ - PySpark 3.5       │    │
│  │ - Auto-init SQL     │         │ - Java 17           │    │
│  │ - Volume: mysql_data│         │ - MySQL Connector   │    │
│  └─────────────────────┘         └─────────────────────┘    │
│           │                                │                │
│           │                                │                │
│           ▼                                ▼                │
│  ┌─────────────────────┐         ┌─────────────────────┐    │
│  │  Volume: mysql_data │         │  Volume: output/    │    │
│  │  (Persistência)     │         │  (CSV gerado)       │    │
│  └─────────────────────┘         └─────────────────────┘    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 📊 Volumes

### `mysql_data`
- **Propósito**: Persistir dados do MySQL
- **Localização**: Gerenciado pelo Docker
- **Backup**: `docker run --rm -v sicooperative-mysql-data:/data -v $(pwd):/backup ubuntu tar czf /backup/mysql-backup.tar.gz /data`

### `../output`
- **Propósito**: Armazenar CSV gerado
- **Localização**: `output/` no diretório raiz do projeto
- **Acesso**: Diretamente no host

## 🌐 Networking

- **Rede**: `sicooperative-network` (bridge)
- **Comunicação**: Containers se comunicam via nomes de serviço
- **Isolamento**: Rede isolada do host (exceto portas expostas)

## 📝 Notas

- **Primeira execução**: Pode demorar alguns minutos para baixar imagens e inicializar
- **Reinicializações**: MySQL preserva dados entre reinicializações (volume persistente)
- **Performance**: Ajuste memória do Spark em `docker-compose.yml` conforme necessário
- **Produção**: Este setup é para desenvolvimento/demonstração. Para produção, use secrets, SSL, etc.

## 📊 Observabilidade e Métricas

### Logs Estruturados (JSON)

O pipeline gera logs estruturados em formato JSON para facilitar monitoramento e análise:

#### Exemplo de Log de Início do Pipeline:
```json
{
  "timestamp": "2024-01-15T14:30:25.123Z",
  "level": "INFO",
  "logger": "etl_pipeline",
  "message": "Iniciando pipeline ETL - MODO INCREMENTAL",
  "pipeline_mode": "incremental",
  "spark_app_name": "SiCooperative-ETL",
  "run_id": "run_20240115_143025"
}
```

#### Exemplo de Log de Extração de Dados:
```json
{
  "timestamp": "2024-01-15T14:30:26.456Z",
  "level": "INFO",
  "logger": "etl_pipeline",
  "message": "Tabela 'movimento' extraída: 15432 registros",
  "table": "movimento",
  "records_count": 15432,
  "extraction_time_seconds": 2.34,
  "partitioning_used": true,
  "partition_column": "id",
  "num_partitions": 8
}
```

#### Exemplo de Log de Transformação:
```json
{
  "timestamp": "2024-01-15T14:30:28.789Z",
  "level": "INFO",
  "logger": "etl_pipeline",
  "message": "JOINs concluídos: 15432 registros",
  "stage": "transform",
  "input_records": 15432,
  "output_records": 15432,
  "joins_performed": 3,
  "transform_time_seconds": 1.12
}
```

#### Exemplo de Log de Carregamento:
```json
{
  "timestamp": "2024-01-15T14:30:30.012Z",
  "level": "INFO",
  "logger": "etl_pipeline",
  "message": "CSV gerado com sucesso",
  "stage": "load",
  "output_format": "csv",
  "output_path": "/app/output/csv/movimento_flat.csv",
  "records_written": 15432,
  "file_size_mb": 4.2,
  "load_time_seconds": 1.45
}
```

#### Exemplo de Log de Qualidade de Dados:
```json
{
  "timestamp": "2024-01-15T14:30:27.345Z",
  "level": "WARNING",
  "logger": "data_quality",
  "message": "Aviso de qualidade detectado",
  "check_name": "null_check_cartao",
  "status": "WARNING",
  "null_percentage": 0.02,
  "threshold": 0.01,
  "affected_records": 3,
  "total_records": 15432
}
```

### Métricas Coletadas

O sistema coleta métricas detalhadas por etapa:

#### Métricas de Performance:
```json
{
  "etl_stage_duration_seconds": {
    "stage": "extract",
    "value": 2.34,
    "timestamp": "2024-01-15T14:30:26.456Z"
  },
  "etl_stage_duration_seconds": {
    "stage": "transform",
    "value": 1.12,
    "timestamp": "2024-01-15T14:30:28.789Z"
  },
  "etl_stage_duration_seconds": {
    "stage": "load",
    "value": 1.45,
    "timestamp": "2024-01-15T14:30:30.012Z"
  }
}
```

#### Métricas de Volume:
```json
{
  "etl_records_input": {
    "stage": "extract",
    "table": "movimento",
    "value": 15432,
    "timestamp": "2024-01-15T14:30:26.456Z"
  },
  "etl_records_output": {
    "stage": "transform",
    "value": 15432,
    "timestamp": "2024-01-15T14:30:28.789Z"
  }
}
```

#### Métricas de Qualidade:
```json
{
  "etl_quality_checks_total": {
    "stage": "extract",
    "value": 12,
    "timestamp": "2024-01-15T14:30:27.345Z"
  },
  "etl_quality_checks_failed": {
    "stage": "extract",
    "value": 0,
    "timestamp": "2024-01-15T14:30:27.345Z"
  },
  "etl_quality_checks_warnings": {
    "stage": "extract",
    "value": 1,
    "timestamp": "2024-01-15T14:30:27.345Z"
  }
}
```

#### Métricas de Performance por Tabela:
```json
{
  "etl_records_by_table": {
    "table": "movimento",
    "stage": "extract",
    "value": 15432,
    "timestamp": "2024-01-15T14:30:26.456Z"
  },
  "etl_quality_checks_by_table": {
    "table": "movimento",
    "stage": "extract",
    "value": 12,
    "timestamp": "2024-01-15T14:30:27.345Z"
  }
}
```

#### Métricas de Eficiência:
```json
{
  "etl_quality_success_rate_percent": {
    "stage": "extract",
    "value": 91.67,
    "timestamp": "2024-01-15T14:30:27.345Z"
  },
  "etl_transform_efficiency_percent": {
    "stage": "transform",
    "value": 100.0,
    "timestamp": "2024-01-15T14:30:28.789Z"
  }
}
```

#### Métricas de Throughput:
```json
{
  "etl_load_throughput_records_per_second": {
    "stage": "load",
    "value": 1064.27,
    "timestamp": "2024-01-15T14:30:30.012Z"
  }
}
```

#### Métricas de Arquivo:
```json
{
  "etl_output_file_size_mb": {
    "stage": "load",
    "format": "csv",
    "value": 4.2,
    "timestamp": "2024-01-15T14:30:30.012Z"
  }
}
```

### Monitoramento Externo

As métricas podem ser integradas com sistemas externos:

#### Prometheus:
- Métricas exportadas em formato Prometheus
- Dashboards no Grafana
- Alertas automáticos

#### ELK Stack:
- Logs estruturados enviados para Elasticsearch
- Dashboards no Kibana
- Análise de tendências

#### Cloud Monitoring:
- Integração com AWS CloudWatch, Azure Monitor, Google Cloud Monitoring
- Métricas customizadas por ambiente

```bash
# 1. Configurar secrets (conforme instruções acima)
cd docker
./setup_secrets.sh dev  # Linux/Mac
# Ou configure manualmente no Windows

# 2. Subir ambiente
docker-compose up -d

# 3. Aguardar MySQL (automático via healthcheck)
docker-compose ps

# 4. Executar pipeline
./run-pipeline.sh  # ou run-pipeline.bat no Windows

# 5. Verificar resultado
head ../output/movimento_flat.csv

# 6. Parar ambiente
docker-compose down
```

## 📚 Referências
- [MySQL Docker Image](https://hub.docker.com/_/mysql)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
