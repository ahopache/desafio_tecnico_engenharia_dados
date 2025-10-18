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
docker-compose exec mysql mysql -u root -proot_password sicooperative_db

# Acessar shell do Spark
docker-compose exec spark bash

# Executar query SQL
docker-compose exec mysql mysql -u root -proot_password sicooperative_db -e "SELECT COUNT(*) FROM movimento;"
```

### Rebuild

```bash
# Rebuild da imagem Spark (após mudanças no Dockerfile)
docker-compose build --no-cache spark

# Rebuild e restart
docker-compose up -d --build
```

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

## 🔐 Credenciais Padrão

**MySQL:**
- Host: `localhost` (ou `mysql` dentro da rede Docker)
- Port: `3306`
- Database: `sicooperative_db`
- Root User: `root`
- Root Password: `root_password`
- App User: `etl_user`
- App Password: `etl_password`

⚠️ **IMPORTANTE**: Estas são credenciais de desenvolvimento. **NUNCA** use em produção!

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

# Forçar execução manual
docker-compose exec mysql mysql -u root -proot_password sicooperative_db < ../sql/01_create_schema.sql
docker-compose exec mysql mysql -u root -proot_password sicooperative_db < ../sql/02_insert_data.sql
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
df = spark.read.format('jdbc').option('url', 'jdbc:mysql://mysql:3306/sicooperative_db').option('user', 'root').option('password', 'root_password').option('driver', 'com.mysql.cj.jdbc.Driver').option('dbtable', '(SELECT 1) AS test').load()
print(df.count())
"
```

### Problema: Permissões no Windows

```bash
# Executar PowerShell como Administrador
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

# Ou usar Docker Desktop com WSL2
```

## 📝 Notas

- **Primeira execução**: Pode demorar alguns minutos para baixar imagens e inicializar
- **Reinicializações**: MySQL preserva dados entre reinicializações (volume persistente)
- **Performance**: Ajuste memória do Spark em `docker-compose.yml` conforme necessário
- **Produção**: Este setup é para desenvolvimento/demonstração. Para produção, use secrets, SSL, etc.

## 🔄 Workflow Completo

```bash
# 1. Subir ambiente
cd docker
docker-compose up -d

# 2. Aguardar MySQL (automático via healthcheck)
docker-compose ps

# 3. Executar pipeline
./run-pipeline.sh  # ou run-pipeline.bat no Windows

# 4. Verificar resultado
head ../output/movimento_flat.csv

# 5. Parar ambiente
docker-compose down
```

## 📚 Referências

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [MySQL Docker Image](https://hub.docker.com/_/mysql)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
