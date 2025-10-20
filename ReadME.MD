# Desafio Técnico - Engenharia de Dados
## Data Lake POC - SiCooperative LTDA

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-blue.svg)](https://www.mysql.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

---

---

## 📋 Índice

- [Contexto do Desafio](#-contexto-do-desafio)
- [Arquitetura da Solução](#-arquitetura-da-solução)
- [Decisões Técnicas](#-decisões-técnicas)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação e Execução](#-instalação-e-execução)
- [Testes](#-testes)
- [Observabilidade e Métricas](#-observabilidade-e-métricas)
- [Melhorias Futuras](#-melhorias-futuras)
- [Dificuldades Encontradas](#-dificuldades-encontradas)
- [Modelo de Dados](#-modelo-de-dados)
- [Relatório de Validação](#-relatório-de-validação)

---

## 🎯 Contexto do Desafio

### Problema de Negócio

A **SiCooperative LTDA** é uma empresa que oferece soluções financeiras justas, mas enfrentava desafios na tomada de decisões devido à:

- **Fragmentação de dados**: Informações dispersas em múltiplos sistemas
- **Processos manuais**: Tempo perdido criando e correlacionando relatórios individuais
- **Necessidade de ML**: Nova equipe de Data Science precisa de dados consolidados para modelos preditivos

### Solução Proposta

Criar uma **POC de Data Lake** para:

1. Centralizar dados de movimentações de cartões
2. Agregar informações de cartões, contas e associados
3. Gerar visão única para análise e modelagem preditiva

### Objetivo do Pipeline

Extrair dados de 4 tabelas relacionais (MySQL) e gerar um arquivo CSV flat com visão consolidada de movimentações, incluindo:

- Dados do associado (cliente)
- Informações da conta
- Detalhes do cartão
- Transações realizadas

---

## 🏗️ Arquitetura da Solução

### Visão Geral

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   MySQL     │─────▶│   PySpark    │─────▶│  CSV File   │
│  (Source)   │      │ ETL Pipeline │      │  (Output)   │
└─────────────┘      └──────────────┘      └─────────────┘
     Bronze              Silver                 Gold
```

### Arquitetura Medalhão (Simplificada)

Embora o desafio solicite apenas um CSV final, a solução segue os princípios da **Arquitetura Medalhão**, amplamente adotada em Data Lakes modernos:

#### **🥉 Bronze Layer (Raw Data)**
- **Representação**: Tabelas no MySQL
- **Características**: Dados transacionais normalizados, exatamente como gerados pelo sistema operacional
- **Tabelas**: `movimento`, `cartao`, `conta`, `associado`

#### **🥈 Silver Layer (Cleansed & Joined)**
- **Representação**: Transformações no Spark (em memória)
- **Operações**:
  - JOINs entre tabelas relacionais
  - Renomeação de colunas para padrão consistente
  - Validação de integridade referencial
  - Conversão de tipos de dados

#### **🥇 Gold Layer (Curated & Aggregated)**
- **Representação**: Arquivo CSV final
- **Características**: Dados desnormalizados, prontos para consumo por analistas e cientistas de dados
- **Formato**: Flat file com todas as informações agregadas

### Fluxo de Dados

```
MySQL: Associado ──┐
MySQL: Conta ──────┼──▶ Spark JOIN ──▶ Transformações ──▶ CSV Output
MySQL: Cartão ─────┤
MySQL: Movimento ──┘
```

**Detalhamento do Fluxo:**

1. **Extração (Extract)**
   - Conexão JDBC com MySQL
   - Leitura paralela das 4 tabelas
   - Carregamento em DataFrames Spark

2. **Transformação (Transform)**
   - JOIN: `movimento` ← `cartao` ← `conta` ← `associado`
   - Seleção e renomeação de colunas
   - Conversão de tipos (timestamps, decimais)

3. **Carga (Load)**
   - Escrita em CSV com header
   - Diretório parametrizável via CLI
   - Encoding UTF-8

---

## 🎨 Decisões Técnicas

### 1. Stack Tecnológica

| Componente | Tecnologia | Justificativa |
|------------|------------|---------------|
| **Banco de Dados** | MySQL 8.0 | Já instalado no ambiente, robusto para dados transacionais, excelente suporte JDBC |
| **Framework Big Data** | Apache Spark (PySpark) | Requisito do desafio, processamento distribuído, escalável para grandes volumes |
| **Linguagem** | Python 3.10+ | Ecossistema rico para dados, sintaxe clara, integração nativa com Spark |
| **Orquestração** | Docker Compose | Ambiente reproduzível, isolamento de dependências, fácil setup |
| **Testes** | pytest + chispa | Framework padrão Python, chispa especializado em DataFrames Spark |

### 2. Modelagem de Dados

#### **Schema Relacional (Bronze)**

Mantive a modelagem normalizada (3FN) no MySQL:

- **Vantagens**:
  - Integridade referencial garantida
  - Redução de redundância
  - Reflete sistema transacional real
  - Facilita manutenção e evolução

- **Relacionamentos**:
  ```
  Associado (1) ──< (N) Conta
  Associado (1) ──< (N) Cartão
  Conta (1) ──< (N) Cartão
  Cartão (1) ──< (N) Movimento
  ```

#### **Schema Flat (Gold)**

Desnormalização completa no CSV final:

- **Vantagens**:
  - Queries simples (sem JOINs)
  - Performance para leitura analítica
  - Compatível com ferramentas de BI
  - Pronto para ingestão em modelos ML

- **Trade-off**: Redundância de dados (aceitável para camada Gold)

### 3. Pipeline ETL

#### **Estratégia de JOIN**

```python
# Ordem otimizada de JOINs
movimento → cartao → conta → associado
```

**Justificativa**:
- Começa pela tabela de fatos (`movimento`) - maior volume
- JOINs sequenciais com dimensões menores
- Spark otimiza automaticamente com broadcast para tabelas pequenas

#### **Configurações Spark**

```python
spark = SparkSession.builder \
    .appName("SiCooperative-ETL") \
    .config("spark.sql.adaptive.enabled", "true")  # Otimização adaptativa
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
```

**Por quê?**
- **Adaptive Query Execution**: Ajusta plano de execução em runtime
- **Coalesce Partitions**: Reduz partições pequenas após JOINs
- Melhora performance sem tuning manual

#### **Tratamento de Erros**

- **Validação de conexão**: Testa conectividade MySQL antes de iniciar
- **Logging estruturado**: Registra cada etapa do pipeline
- **Exceções customizadas**: Mensagens claras para troubleshooting

### 4. Containerização

#### **Docker Compose Multi-Container**

```yaml
services:
  mysql:      # Banco de dados
  spark:      # Ambiente PySpark
```

**Benefícios**:
- **Reprodutibilidade**: Mesmo ambiente em qualquer máquina
- **Isolamento**: Dependências não conflitam com sistema host
- **Automação**: Setup completo com um comando
- **Networking**: Containers se comunicam via rede interna

#### **Volumes Docker**

- `./sql:/docker-entrypoint-initdb.d`: Scripts SQL executados automaticamente
- `./output:/app/output`: Persistência do CSV gerado
- `./src:/app/src`: Hot-reload para desenvolvimento

### 5. Qualidade de Código

#### **Testes Unitários**

```python
# Cobertura de testes
- Transformações de dados
- Lógica de JOIN
- Validação de schema
- Tratamento de erros
```

**Framework**: pytest + chispa
- `chispa.assert_df_equality()`: Compara DataFrames Spark
- Fixtures para dados de teste
- Mocks para conexões externas

#### **Boas Práticas**

- **Type hints**: Anotações de tipo em todas as funções
- **Docstrings**: Documentação inline no código
- **Configuração externa**: `.env` para credenciais e parâmetros
- **Logging**: Rastreabilidade de execução

---

## 🔍 Tratamento de Tipos de Dados

Para garantir a máxima confiabilidade e consistência dos dados, foram implementadas as seguintes práticas de tipagem:

### 1. Valores Monetários (Decimal)
- **Formato**: `DECIMAL(20,2)` para todos os campos monetários
- **Benefícios**:
  - Precisão exata em cálculos financeiros
  - Evita erros de arredondamento em operações matemáticas
  - Consistência na representação de valores monetários
  - Compatibilidade com sistemas financeiros

### 2. Datas e Horários (ISO 8601)
- **Formato**: `YYYY-MM-DDTHH:MM:SSZ` (UTC)
- **Exemplo**: `2023-10-14T15:30:00Z`
- **Benefícios**:
  - Padrão internacional reconhecido
  - Ordenação cronológica correta em texto
  - Suporte a fuso horário (sempre UTC)
  - Evita ambiguidades (DD/MM/YYYY vs MM/DD/YYYY)
  - Compatibilidade com a maioria dos bancos de dados e ferramentas

### 3. Tipagem Explícita
- **Campos numéricos**: Conversão explícita para tipos apropriados (INT, BIGINT, DECIMAL)
- **Campos de texto**: Codificação UTF-8 para suporte a caracteres especiais
- **Validação de esquema**: Verificação de tipos em tempo de execução

### 4. Preservação de Tipos no Output
- **CSV**: Formatação explícita de datas e decimais
- **Parquet**: Manutenção dos tipos nativos do Spark
- **Metadados**: Schema JSON incluído para referência

Esta abordagem garante que os dados mantenham sua integridade em todo o pipeline, desde a extração até a análise final, reduzindo significativamente o risco de erros de interpretação ou perda de precisão.

## 🔐 Proteção de Dados Pessoais (PII)

O pipeline implementa estratégias rigorosas de proteção de dados pessoais identificáveis (PII) para garantir conformidade com leis de privacidade como LGPD e GDPR.

### Estratégia de Proteção

#### Dados Sensíveis Tratados

1. **Números de Cartão (PAN)**
   - **Mascaramento**: Mostra apenas primeiros 6 e últimos 4 dígitos
   - **Formato Final**: `123456******3456`
   - **Reversibilidade**: ❌ Irreversível

2. **Emails de Associados**
   - **Mascaramento**: Oculta nome de usuário, mantém domínio
   - **Formato Final**: `joa****@email.com`
   - **Hash SHA-256**: Para análise estatística sem revelar dados reais

#### Características Técnicas

- **Algoritmo de Hash**: SHA-256 (irreversível)
- **Salt Configurável**: Segurança adicional contra ataques de rainbow table
- **Validações Automáticas**: Garante que dados estejam adequadamente protegidos
- **Auditoria Completa**: Logs detalhados de todas as validações

#### Colunas no Output Final

| Coluna | Tipo | Proteção | Reversibilidade |
|--------|------|----------|-----------------|
| `numero_cartao_masked` | String | Mascarado | ❌ Irreversível |
| `numero_cartao_hash` | String | Hash SHA-256 | ❌ Irreversível |
| `email_masked` | String | Mascarado | ❌ Irreversível |
| `email_hash` | String | Hash SHA-256 | ❌ Irreversível |

### Conformidade Regulatória

- **LGPD (Brasil)**: Atende requisitos de anonimização
- **GDPR (Europa)**: Compatível com princípios de proteção de dados
- **PCI DSS**: Não armazena dados completos de cartão
- **SOX**: Mantém auditoria sem comprometer privacidade

### Trade-offs Avaliados

**Hash Irreversível (Implementado)**:
- ✅ Máxima proteção de privacidade
- ✅ Conformidade regulatória
- ✅ Simplicidade de implementação

**Token Reversível (Rejeitado)**:
- ❌ Complexidade adicional
- ❌ Risco de vazamento de chaves
- ❌ Não atende anonimização LGPD

> 📋 **Detalhes técnicos**: Para implementação técnica completa, consulte [`src/data_quality.md`](src/data_quality.md)

## 📁 Estrutura do Projeto

```
desafio_tecnico_engenharia_dados/
│
├── docker/
│   ├── docker-compose.yml          # Orquestração de containers
│   └── Dockerfile                  # Imagem customizada Spark + Python
│
├── sql/
│   ├── 01_create_schema.sql        # DDL: Criação de tabelas
│   ├── 02_insert_data.sql          # DML: Carga de dados fictícios
│   ├── generate_fake_data.py       # Gerador de dados com Faker
│   └── README.md                   # 📋 Documentação técnica detalhada dos scripts SQL
│
├── src/
│   ├── __init__.py
│   ├── etl_pipeline.py             # Pipeline principal ETL
│   ├── config.py                   # Configurações e variáveis de ambiente
│   ├── data_quality.py             # Sistema de verificações de qualidade
│   ├── data_quality.md             # 📋 Documentação técnica do sistema de qualidade
│   └── utils.py                    # Funções auxiliares (logging, validação)
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Fixtures compartilhadas
│   ├── test_config.py              # Testes de configuração
│   ├── test_utils.py               # Testes de utilitários
│   ├── test_etl_pipeline.py        # Testes do pipeline ETL
│   ├── test_integration.py         # Testes de integração
│   ├── validate_project.py         # Script de validação
│   └── README.md                   # 📋 Documentação técnica detalhada dos testes
│
├── output/                         # Diretório para CSV gerado (gitignored)
│
├── .env.example                    # Template de variáveis de ambiente
├── .gitignore                      # Arquivos ignorados pelo Git
├── requirements.txt                # Dependências Python
├── README.md                       # Este arquivo
└── desafio.txt                     # Descrição original do desafio
```

---

## 🔧 Pré-requisitos

### Opção 1: Execução com Docker (Recomendado)

- [Docker](https://docs.docker.com/get-docker/) >= 20.10
- [Docker Compose](https://docs.docker.com/compose/install/) >= 2.0

### Opção 2: Execução Local

- Python >= 3.10
- MySQL >= 8.0 (já instalado)
- Java JDK >= 11 (requerido pelo Spark)
- Apache Spark >= 3.5

---

## 🚀 Instalação e Execução

### Opção 1: Docker (Automatizado)

#### 1. Clone o repositório

```bash
git clone https://github.com/ahopache/desafio_tecnico_engenharia_dados.git
cd desafio_tecnico_engenharia_dados
```

#### 2. Configure variáveis de ambiente

```bash
cp .env.example .env
# Edite .env se necessário (valores padrão já funcionam)
```

#### 3. Suba o ambiente completo

**Método 1: Usando o script run.sh (Recomendado)**
```bash
# Na raiz do projeto
./run.sh
```

**Método 2: Comandos manuais**
```bash
# Navegar para o diretório docker
cd docker

# Iniciar com arquivo de ambiente específico
docker-compose --env-file ../.env up --build -d

# Ou usar variáveis de ambiente padrão
docker-compose up --build -d
```

**O que acontece:**
- MySQL é iniciado e populado automaticamente com dados fictícios
- Container Spark é criado com todas as dependências
- Rede interna conecta os serviços
- **Healthchecks garantem** que serviços estão prontos antes do uso

#### 4. Execute o pipeline ETL

```bash
# Executa dentro do container Spark
docker-compose exec spark python /app/src/etl_pipeline.py --output /app/output
```

#### 5. Verifique o resultado

```bash
# CSV gerado estará em ./output/movimento_flat.csv
ls -lh ../output/movimento_flat.csv
head ../output/movimento_flat.csv
```

#### 6. Encerre o ambiente

```bash
# Parar todos os serviços
docker-compose down

# Para remover volumes (dados do MySQL):
docker-compose down -v

# Para parar apenas serviços específicos:
docker-compose stop mysql spark
```

---

### Exemplos Práticos com `--env-file`

#### Exemplo 1: Ambiente de Desenvolvimento
```bash
# Criar arquivo .env específico para desenvolvimento
cat > .env.dev << EOF
MYSQL_ROOT_PASSWORD=dev_root_password
MYSQL_DATABASE=dev_sicooperative_db
MYSQL_USER=dev_user
MYSQL_PASSWORD=dev_password
OUTPUT_DIR=./output_dev
LOG_LEVEL=DEBUG
EOF

# Executar com configuração específica
docker-compose --env-file .env.dev -f docker/docker-compose.yml up --build -d
```

#### Exemplo 2: Ambiente de Produção
```bash
# Arquivo .env.prod com configurações de produção
cat > .env.prod << EOF
MYSQL_ROOT_PASSWORD=${MYSQL_PROD_PASSWORD}
MYSQL_DATABASE=prod_sicooperative_db
MYSQL_USER=${MYSQL_PROD_USER}
MYSQL_PASSWORD=${MYSQL_PROD_PASSWORD}
OUTPUT_DIR=/mnt/data/output
LOG_LEVEL=INFO
OBSERVABILITY_ENABLED=true
EOF

# Executar com configuração de produção
docker-compose --env-file .env.prod -f docker/docker-compose.yml up --build -d
```

#### Exemplo 3: Execução Rápida
```bash
# Usando o script run.sh com variáveis customizadas
MYSQL_ROOT_PASSWORD=meu_password ./run.sh
```

### Comandos Úteis para Desenvolvimento

```bash
# Ver status dos containers
docker-compose ps

# Ver logs em tempo real
docker-compose logs -f spark

# Executar comandos no container Spark
docker-compose exec spark bash

# Executar apenas os testes
docker-compose exec spark python -m pytest tests/ -v

# Limpar tudo (containers, redes, volumes)
docker-compose down --volumes --remove-orphans

# Reconstruir apenas o container Spark
docker-compose build --no-cache spark
docker-compose up -d spark

---

### Opção 2: Execução Local

#### 1. Configure o banco de dados

**Para usuários avançados que desejam configurar o MySQL manualmente:**

```bash
# Scripts SQL detalhados disponíveis em: sql/README.md
mysql -u root -p < sql/01_create_schema.sql
mysql -u root -p < sql/02_insert_data.sql
```

> 📋 **Nota**: Para documentação técnica completa dos scripts SQL, consulte [`sql/README.md`](sql/README.md)

#### 2. Instale dependências Python

```bash
pip install -r requirements.txt
```

#### 3. Configure variáveis de ambiente

```bash
cp .env.example .env
# Edite .env com suas credenciais MySQL
```

#### 4. Execute o pipeline

```bash
python src/etl_pipeline.py --output ./output
```

---

## 🧪 Testes

### Testes Unitários

```bash
# Executar todos os testes
pytest tests/ -v

# Com cobertura
pytest tests/ --cov=src --cov-report=html

# Apenas testes unitários
pytest tests/test_*.py -v
```

**Estrutura dos Testes Unitários:**

```python
tests/
├── test_etl_pipeline.py
│   ├── test_read_tables()           # Testa leitura do MySQL
│   ├── test_join_tables()           # Valida JOINs
│   ├── test_transform_columns()     # Verifica transformações
│   └── test_write_csv()             # Testa escrita do CSV
│
└── test_utils.py
    ├── test_validate_connection()   # Testa conectividade
    └── test_logger()                # Valida logging
```

**Cobertura de testes:**
- ✅ **44 testes unitários** distribuídos em 3 módulos
- ✅ **Testes de configuração** (URLs, propriedades, validações)
- ✅ **Testes de utilitários** (logger, validações, formatação)
- ✅ **Testes do pipeline** (JOINs, transformações, qualidade)

> 📋 **Detalhes técnicos**: Para documentação completa dos testes, consulte [`tests/README.md`](tests/README.md)

### Testes de Integração

**Sistema de Testes End-to-End com Docker Compose:**

```bash
# Teste completo de integração
pytest tests/test_integration.py::TestSiCooperativeIntegration::test_full_pipeline_integration -v -s

# Com diferentes configurações
pytest tests/test_integration.py -k "test_pipeline_with_different_configs" -v

# Ou usar script auxiliar
python run_integration_tests.sh --test-type integration
```

**Cenários Testados:**
- ✅ Ambiente Docker sobe corretamente
- ✅ MySQL fica disponível e responde
- ✅ Dados de teste são inseridos
- ✅ Pipeline ETL executa sem erros
- ✅ Arquivo CSV é gerado corretamente
- ✅ Dados têm estrutura e formato esperados
- ✅ Ambiente é limpo após teste

**Dados de Teste:**
- 5 associados com dados realistas
- 5 contas de diferentes tipos
- 5 cartões vinculados
- 25-75 movimentos distribuídos em 30 dias

**Validações Realizadas:**
- Existência do arquivo `movimento_flat.csv`
- Número mínimo de registros (≥ 10)
- Colunas obrigatórias presentes (11 colunas)
- Tipos de dados corretos
- Variedade de tipos de conta

### Arquivos de Teste

- **`tests/test_integration.py`**: Testes principais de integração
- **`setup_test_data.py`**: Script para popular banco com dados de teste
- **`.env.test`**: Configurações específicas para ambiente de teste
- **`run_integration_tests.sh`**: Script auxiliar para execução simplificada

### Troubleshooting de Testes

#### Problemas Comuns

1. **Docker não disponível**
   ```bash
   docker --version && docker-compose --version
   ```

2. **Dados de teste não inseridos**
   ```bash
   python setup_test_data.py
   ```

3. **Teste falhando por timeout**
   ```bash
   # Aumentar timeout no teste
   # Modificar _wait_for_mysql(max_attempts=60, delay=5)
   ```

---

## 🔮 Melhorias Futuras

## 📊 Observabilidade e Métricas

O pipeline implementa um sistema completo de observabilidade para monitoramento em tempo real e análise de performance.

### Métricas Coletadas

#### Métricas de Tempo
- **`etl_stage_duration_seconds`**: Duração de cada etapa (extract, transform, load)
- **`etl_pipeline_duration_seconds`**: Duração total do pipeline
- **`etl_quality_check_duration_seconds`**: Tempo gasto em verificações de qualidade

#### Métricas de Volume
- **`etl_records_input`**: Registros de entrada por etapa
- **`etl_records_output`**: Registros de saída por etapa
- **`etl_throughput_records_per_second`**: Taxa de processamento

#### Métricas de Qualidade
- **`etl_quality_checks_total/failed/warnings`**: Status das verificações de qualidade

### Configuração

```bash
# Arquivo .env
OBSERVABILITY_ENABLED=true
PROMETHEUS_GATEWAY_URL=http://localhost:9091  # Opcional
PROMETHEUS_JOB_NAME=sicooperative-etl
METRICS_DETAILED_LOGGING=false
METRICS_EXPORT_FILE=pipeline_metrics.json
```

### Sistema de Qualidade de Dados

O pipeline implementa verificações automáticas de qualidade durante a execução:

**Verificações implementadas:**
- ✅ **Valores NULL** em campos críticos (ex: `id_cartao` ≤ 1%)
- ✅ **Transações negativas** em valores financeiros (alerta)
- ✅ **Mudanças drásticas** no volume de dados (≤ 50% tolerância)
- ✅ **Completude** de colunas obrigatórias

**Política de rejeição:**
- ❌ **FAIL**: Campos críticos com NULL excessivo
- ❌ **FAIL**: Colunas obrigatórias ausentes
- ⚠️ **WARN**: Transações negativas detectadas
- ⚠️ **WARN**: Mudanças significativas no volume

> 📋 **Detalhes técnicos**: Para documentação completa do sistema de qualidade, consulte [`src/data_quality.md`](src/data_quality.md)

### Integração com Prometheus

O sistema suporta envio automático de métricas para o Prometheus via Pushgateway:

```bash
# As métricas são enviadas automaticamente se PROMETHEUS_GATEWAY_URL estiver configurado
# Verificar métricas no Prometheus:
# http://localhost:9090/graph?g0.expr=etl_stage_duration_seconds
```

### Logs Estruturados

```
📊 RESUMO DE MÉTRICAS DO PIPELINE
⏱️ Duração total: 125.432s
📈 Média por etapa: 17.919s
🔢 Total de métricas: 45
⏳ Timers executados: 7
```

### Dashboards Grafana (Exemplos)

```promql
# Tempo médio por etapa
avg(etl_stage_duration_seconds) by (stage)

# Volume de dados processados
sum(etl_records_output) by (stage)

# Qualidade dos dados
etl_quality_checks_failed / etl_quality_checks_total
```

---

## 🧪 Testes de Integração

### Sistema de Testes End-to-End

O projeto inclui testes de integração completos que validam todo o pipeline usando Docker Compose.

#### Cenários Testados

1. **Teste Completo de Integração**
   - ✅ Ambiente Docker sobe corretamente
   - ✅ MySQL fica disponível e responde
   - ✅ Dados de teste são inseridos
   - ✅ Pipeline ETL executa sem erros
   - ✅ Arquivo CSV é gerado corretamente
   - ✅ Dados têm estrutura e formato esperados
   - ✅ Ambiente é limpo após teste

2. **Teste de Saúde do Ambiente**
   - ✅ Containers estão rodando
   - ✅ MySQL responde a conexões
   - ✅ Rede Docker configurada corretamente

3. **Testes com Diferentes Configurações**
   - ✅ Configuração padrão
   - ✅ Com observabilidade habilitada
   - ✅ Com verificações de qualidade

#### Dados de Teste

- **5 associados** com dados realistas
- **5 contas** de diferentes tipos (CORRENTE, POUPANCA, INVESTIMENTO)
- **5 cartões** vinculados às contas e associados
- **25-75 movimentos** distribuídos nos últimos 30 dias
- **Transações variadas** (compras, combustível, restaurantes, etc.)

#### Execução dos Testes

```bash
# 1. Subir ambiente de teste
docker-compose -f docker/docker-compose.yml up -d mysql

# 2. Executar setup de dados
python setup_test_data.py

# 3. Executar teste
pytest tests/test_integration.py::TestSiCooperativeIntegration::test_full_pipeline_integration -v -s

# 4. Ou usar script auxiliar
python run_integration_tests.sh --test-type integration
```

#### Validações Realizadas

- ✅ Existência do arquivo `movimento_flat.csv`
- ✅ Número mínimo de registros (≥ 10)
- ✅ Colunas obrigatórias presentes (11 colunas)
- ✅ Tipos de dados corretos
- ✅ Variedade de tipos de conta
- ✅ Valores de transação positivos

### Arquivos de Teste

- **`tests/test_integration.py`**: Testes principais de integração
- **`setup_test_data.py`**: Script para popular banco com dados de teste
- **`.env.test`**: Configurações específicas para ambiente de teste
- **`run_integration_tests.sh`**: Script auxiliar para execução simplificada

### Troubleshooting de Testes

#### Problemas Comuns

1. **Docker não disponível**
   ```bash
   # Verificar instalação
   docker --version && docker-compose --version
   ```

2. **Dados de teste não inseridos**
   ```bash
   # Executar setup manualmente
   python setup_test_data.py
   ```

3. **Teste falhando por timeout**
   ```bash
   # Aumentar timeout no teste
   # Modificar _wait_for_mysql(max_attempts=60, delay=5)
   ```

### Métricas de Performance dos Testes

- ⏱️ **Tempo típico**: 2-5 minutos por teste completo
- 🔄 **Inicialização**: ~30s (MySQL) + ~60s (dados)
- ⚡ **Pipeline**: ~30-60s dependendo do volume
- 🧹 **Limpeza**: ~10s
- 🎯 **Taxa de sucesso**: >95% em ambiente limpo

---

## 🔧 Melhorias Futuras

### Com Mais Tempo, Implementaria:

#### **1. Persistência de Camadas Intermediárias**

```python
# Salvar Bronze e Silver em Parquet
df_bronze.write.parquet("s3://datalake/bronze/movimento/")
df_silver.write.parquet("s3://datalake/silver/movimento_joined/")
```

**Benefícios**:
- Reprocessamento mais rápido
- Auditoria de transformações
- Recuperação de falhas

#### **2. Data Quality Checks**

```python
# Validações entre camadas
- Contagem de registros (sem perda de dados)
- Valores nulos em campos obrigatórios
- Integridade referencial
- Ranges de valores (ex: idade > 0)
```

**Framework**: Great Expectations ou Deequ

#### **3. Particionamento Inteligente**

```python
# Particionar por data para queries eficientes
df.write.partitionBy("ano", "mes").parquet("output/")
```

**Benefícios**:
- Queries mais rápidas (partition pruning)
- Processamento incremental
- Menor custo de storage

#### **4. Orquestração com Airflow**

```python
# DAG para execução agendada
dag = DAG('sicooperative_etl', schedule_interval='@daily')

tasks:
  extract → transform → validate → load → notify
```

**Benefícios**:
- Agendamento automático
- Retry em falhas
- Monitoramento visual
- Alertas

#### **5. Monitoramento e Observabilidade Avançada**

**Sistema já implementado** (detalhado na seção [ Observabilidade e Métricas](#-observabilidade-e-métricas)):

```python
# Métricas já coletadas automaticamente:
- etl_stage_duration_seconds (por etapa)
- etl_records_output (volume de dados)
- etl_quality_checks_failed (qualidade)
- etl_pipeline_duration_seconds (total)
```

**Integrações futuras**:
- **Alertas automáticos**: Slack/Email em falhas críticas
- **Dashboards executivos**: Relatórios automatizados
- **SLA monitoring**: Controle de acordos de nível de serviço
- **Análise de tendências**: Detecção proativa de problemas

#### **7. CI/CD Pipeline**

```yaml
# GitHub Actions
- Lint (flake8, black)
- Testes unitários
- Testes de integração
- Build de imagem Docker
- Deploy automatizado
```

#### **8. Segurança**

- **Secrets Management**: AWS Secrets Manager ou Vault
- **Criptografia**: Dados em trânsito (TLS) e em repouso
- **IAM**: Controle de acesso granular
- **Audit Logs**: Rastreabilidade de acessos

#### **9. Performance**

- **Caching**: Spark cache para tabelas reutilizadas
- **Broadcast Joins**: Forçar broadcast para tabelas pequenas
- **Bucketing**: Pré-particionar por chave de JOIN
- **Z-Ordering**: Otimizar layout de arquivos Parquet

---

## 🐛 Dificuldades Encontradas

### 1. **Compatibilidade de Versões**

**Problema**: Conflito entre versões do MySQL Connector e PySpark

**Solução**:
- Fixei versões específicas no `requirements.txt`
- Testei compatibilidade: PySpark 3.5 + mysql-connector-java 8.0.33
- Documentei versões testadas

### 2. **Configuração JDBC no Spark**

**Problema**: Erro de ClassNotFoundException para driver MySQL

**Solução**:
```python
# Adicionar JAR do MySQL ao classpath do Spark
spark = SparkSession.builder \
    .config("spark.jars", "/path/to/mysql-connector.jar") \
    .getOrCreate()
```

### 3. **Networking Docker**

**Problema**: Container Spark não conseguia conectar ao MySQL

**Solução**:
- Configurei rede customizada no Docker Compose
- Usei nome do serviço como hostname (`mysql:3306`)
- Aguardei MySQL estar "healthy" antes de iniciar Spark

### 4. **Encoding de Caracteres**

**Problema**: Caracteres especiais (acentos) corrompidos no CSV

**Solução**:
```python
df.write.option("encoding", "UTF-8").csv(output_path)
```

### 5. **Geração de Dados Fictícios Consistentes**

**Problema**: Garantir integridade referencial entre tabelas

**Solução**:
- Usei Faker com seed fixo para reprodutibilidade
- Gerei IDs sequenciais
- Validei foreign keys antes de inserir

---

## 📊 Modelo de Dados

### Diagrama ER

```
┌─────────────────┐
│   Associado     │
├─────────────────┤
│ id (PK)         │
│ nome            │
│ sobrenome       │
│ idade           │
│ email           │
└────────┬────────┘
         │ 1
         │
         │ N
┌────────┴────────┐       ┌─────────────────┐
│     Conta       │       │     Cartão      │
├─────────────────┤       ├─────────────────┤
│ id (PK)         │◄──────┤ id (PK)         │
│ tipo            │ 1   N │ num_cartao      │
│ data_criacao    │       │ nom_impresso    │
│ id_associado(FK)│       │ id_conta (FK)   │
└─────────────────┘       │ id_associado(FK)│
                          └────────┬────────┘
                                   │ 1
                                   │
                                   │ N
                          ┌────────┴────────┐
                          │   Movimento     │
                          ├─────────────────┤
                          │ id (PK)         │
                          │ vlr_transacao   │
                          │ des_transacao   │
                          │ data_movimento  │
                          │ id_cartao (FK)  │
                          └─────────────────┘
```

### Schema do CSV Final

| Coluna | Tipo | Origem | Descrição |
|--------|------|--------|-----------|
| `nome_associado` | string | associado.nome | Nome do cliente |
| `sobrenome_associado` | string | associado.sobrenome | Sobrenome do cliente |
| `idade_associado` | string | associado.idade | Idade do cliente |
| `vlr_transacao_movimento` | string | movimento.vlr_transacao | Valor da transação |
| `des_transacao_movimento` | string | movimento.des_transacao | Descrição da transação |
| `data_movimento` | string | movimento.data_movimento | Data/hora da transação |
| `numero_cartao` | string | cartao.num_cartao | Número do cartão |
| `nome_impresso_cartao` | string | cartao.nom_impresso | Nome impresso no cartão |
| `data_criacao_cartao` | string | cartao.data_criacao | Data de criação do cartão |
| `tipo_conta` | string | conta.tipo | Tipo da conta (corrente/poupança) |
| `data_criacao_conta` | string | conta.data_criacao | Data de abertura da conta |

---

## ✨ Formatos de Saída
O pipeline suporta geração de dados em múltiplos formatos, configuráveis via variável de ambiente `OUTPUT_FORMAT`:
### 1. CSV (Padrão)
- **Vantagens**:
 - Compatível com qualquer ferramenta
 - Fácil inspeção manual
 - Único arquivo de saída
- **Uso recomendado**:
 - Compartilhamento de dados
 - Ferramentas que não suportam Parquet
### 2. Parquet (Recomendado para análise)
- **Vantagens**:
 - Formato colunar otimizado para consultas analíticas
 - Particionamento por data (`data_movimento_date`)
 - Compressão eficiente (Snappy por padrão)
 - Preservação de tipos de dados
- **Uso recomendado**:
 - Processamento com Spark/PySpark
 - Análises em grandes volumes de dados
 - Ambientes de Data Lake
### Configuração
No arquivo `.env`:
```ini
# Formatos de saída (csv, parquet ou ambos)
OUTPUT_FORMAT=csv,parquet
# Configurações do Parquet
PARQUET_COMPRESSION=snappy # Opções: none, snappy, gzip, lzo, brotli, lz4
```

## 📊 Exemplo de Saída

### Dados de Exemplo
```sql
-- Antes
numero_cartao: 1234567890123456
email: joao.silva@email.com

-- Depois
numero_cartao_masked: 123456******3456
numero_cartao_hash: a1b2c3... (hash SHA-256)
email_masked: joa****@email.com
email_hash: x9y8z7... (hash SHA-256)
```

### Como Usar os Dados Gerados

#### 1. Lendo o CSV
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Ler o arquivo CSV
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("output/csv/")

df_csv.show(5)
```

#### 2. Lendo o Parquet (Recomendado)
```python
# Ler Parquet particionado
df_parquet = spark.read.parquet("output/parquet/")

# Consulta otimizada com filtro de partição
df_jan_2023 = df_parquet.filter("data_movimento_date = '2023-01-01'")

# Análise de exemplo: total por categoria
df_parquet.groupBy("categoria") \
    .agg({"vlr_transacao_movimento": "sum"}) \
    .show()
```

#### 3. Convertendo para Pandas (para análise em memória)
```python
# Converter para Pandas (apenas se os dados couberem em memória)
import pandas as pd

# Para CSV
pdf_csv = pd.read_csv("output/csv/part-00000-*.csv")

# Para Parquet (usando pyarrow para melhor performance)
pdf_parquet = pd.read_parquet("output/parquet/")
```

#### 4. Consultas SQL
```python
# Registrar o DataFrame como uma view temporária
df_parquet.createOrReplaceTempView("movimentos")

# Executar consulta SQL
result = spark.sql("""
    SELECT 
        nome_associado,
        COUNT(*) as total_compras,
        SUM(CAST(vlr_transacao_movimento AS DECIMAL(10,2))) as valor_total
    FROM movimentos
    WHERE data_movimento_date BETWEEN '2023-01-01' AND '2023-01-31'
    GROUP BY nome_associado
    ORDER BY valor_total DESC
""")

result.show()
```

---

## 📋 Relatório de Validação

Para detalhes técnicos sobre validação do projeto, métricas de qualidade e análise de implementação, consulte o arquivo [`report/VALIDATION_REPORT.md`](report/VALIDATION_REPORT.md).

Este relatório inclui:
- ✅ **Validações técnicas** realizadas
- ✅ **Estatísticas do projeto** (linhas de código, testes, cobertura)
- ✅ **Verificação de requisitos** do desafio
- ✅ **Diferenciais implementados** em tabela detalhada
- ✅ **Próximos passos** para execução e deploy

---

## 📝 Licença

Este projeto foi desenvolvido como parte de um desafio técnico para vaga de Engenheiro de Dados Sênior.

---

## 👤 Autor

Assis Henrique Oliveira Pacheco
- GitHub: [@ahopache](https://github.com/ahopache)
- LinkedIn: [Assis Henrique](https://www.linkedin.com/in/assispacheco/)

---

## 🙏 Agradecimentos

Obrigado pela oportunidade de participar deste desafio técnico! Foi uma experiência enriquecedora aplicar conceitos de arquitetura de Data Lake e engenharia de dados em um cenário realista.

---

**Desenvolvido com muita atenção e cuidado para o desafio técnico de Engenharia de Dados**