# 📁 src/ - Código Principal do ETL

Esta pasta contém todo o código fonte do pipeline ETL da SiCooperative, incluindo processamento, qualidade de dados, observabilidade e configurações.

## 🏗️ Arquitetura Geral

### Módulos Principais

| Módulo | Responsabilidade | Funcionalidades |
|--------|-----------------|-----------------|
| **`etl_pipeline.py`** | Pipeline principal | Extração, transformação, carga e idempotência |
| **`config.py`** | Configurações | Variáveis de ambiente, validações e parâmetros |
| **`utils.py`** | Utilitários | Logging, validações, formatação e helpers |
| **`data_quality.py`** | Qualidade de dados | Verificações, métricas e proteção PII |
| **`observability.py`** | Monitoramento | Métricas, alertas e instrumentação |

## 🔄 Funcionalidades Implementadas

### 1. Processamento ETL Completo
- **Extração incremental** com watermark por tabela
- **Transformação** com JOINs otimizados e conversões de tipos
- **Carga** com atomic writes e formatos múltiplos (CSV/Parquet)
- **Idempotência** garantida em todas as etapas

### 2. Sistema de Qualidade de Dados
- **Verificações automáticas** de NULL em campos críticos
- **Detecção de anomalias** em volumes e valores negativos
- **Proteção PII** com mascaramento e hash irreversível
- **Histórico de métricas** para monitoramento contínuo

### 3. Observabilidade e Monitoramento
- **Métricas detalhadas** de performance e qualidade
- **Logs estruturados** para troubleshooting
- **Integração Prometheus** opcional
- **Alertas automáticos** para anomalias

### 4. Configuração Flexível
- **Variáveis de ambiente** para todos os parâmetros
- **Validações automáticas** de configurações
- **Ambientes múltiplos** (dev, test, prod)
- **Configurações de produção** seguras

## 🚀 Modos de Execução

### Incremental (Produção)
```bash
python src/etl_pipeline.py --run-mode incremental
```
- Processa apenas dados novos/modificados
- Usa watermark para rastreamento
- Otimizado para ambientes de produção

### Completo (Reconstrução)
```bash
python src/etl_pipeline.py --run-mode full
```
- Processa todos os dados do zero
- Útil para reconstruções completas
- Mantém atomicidade e consistência

## ⚙️ Configurações Principais

### Processamento
```bash
# Modo de execução
INCREMENTAL_PROCESSING=true
LOOKBACK_DAYS=7

# Atomic Writes
ATOMIC_WRITES_ENABLED=true
TEMP_DIR_SUFFIX=_temp

# Retry e Resiliência
RETRY_ATTEMPTS=3
RETRY_DELAY_SECONDS=60
```

### Qualidade de Dados
```bash
# Verificações habilitadas
DATA_QUALITY_CHECKS_ENABLED=true
NULL_CHECK_THRESHOLD_CARTAO=0.01
NEGATIVE_TRANSACTIONS_THRESHOLD=0.0
VOLUME_CHANGE_TOLERANCE=0.5
```

### Proteção PII
```bash
# Segurança de dados pessoais
HASH_SALT=s1c00p3r4t1v3_s3cur3_s4lt
PII_VALIDATION_ENABLED=true
PAN_DETECTION_ENABLED=true
```

### Observabilidade
```bash
# Métricas e monitoramento
OBSERVABILITY_ENABLED=true
PROMETHEUS_GATEWAY_URL=http://localhost:9091
METRICS_DETAILED_LOGGING=false
```

## 🔧 Características Técnicas

### Performance Otimizada
- **Spark adaptativo** para otimização automática de planos
- **Particionamento JDBC** para grandes volumes
- **Broadcast joins** para tabelas pequenas
- **Coalesce automático** de partições

### Confiabilidade Garantida
- **Idempotência completa** em todas as operações
- **Atomic writes** para prevenir arquivos corrompidos
- **Retry com backoff** para recuperação automática
- **Validações rigorosas** em todas as etapas

### Segurança e Compliance
- **Mascaramento de dados sensíveis** (cartões, emails)
- **Hash irreversível** para anonimização
- **Auditoria completa** sem exposição de dados
- **Conformidade LGPD/GDPR/PCI DSS**

### Monitoramento Abrangente
- **Métricas de negócio** (volumes, qualidade, performance)
- **Logs estruturados** para análise e troubleshooting
- **Alertas proativos** para anomalias
- **Dashboards executivos** opcionais

## 📊 Outputs Gerados

### Formatos Suportados
- **CSV**: Compatível com ferramentas de BI
- **Parquet**: Otimizado para análise com Spark

### Estrutura do Schema Final
```python
# 12 colunas consolidadas
{
    "nome_associado", "sobrenome_associado", "idade_associado",
    "id_movimento", "vlr_transacao_movimento", "des_transacao_movimento",
    "data_movimento", "numero_cartao_masked", "nome_impresso_cartao",
    "data_emissao_cartao", "tipo_conta", "data_criacao_conta"
}
```

## 🛠️ Desenvolvimento e Manutenção

### Arquitetura Modular
- **Separação clara** de responsabilidades
- **Dependências mínimas** entre módulos
- **Testabilidade** de cada componente
- **Extensibilidade** para novas funcionalidades

### Padrões de Código
- **Type hints** em todas as funções
- **Docstrings** completas e padronizadas
- **Tratamento robusto** de erros e exceções
- **Logging consistente** em todos os módulos

### Testes Abrangentes
- **Unitários** para funções individuais
- **Integração** para fluxo completo
- **Idempotência** para múltiplas execuções
- **Qualidade** para validações específicas

## 📈 Métricas e Performance

### Benchmarks Implementados
- **Redução** no tempo de processamento incremental
- **Redução** no uso de recursos computacionais
- **Eliminação total** de arquivos corrompidos
- **Detecção automática** de problemas de qualidade

### Monitoramento em Tempo Real
- Duração de cada etapa do pipeline
- Volume de dados processados por tabela
- Taxa de sucesso das verificações de qualidade
- Métricas de proteção de dados pessoais

## 🔗 Integrações

### Bancos de Dados
- **MySQL 8.0+** para fonte de dados
- **Suporte JDBC** com configuração otimizada

### Ferramentas de Monitoramento
- **Prometheus** para métricas (opcional)
- **Grafana** para dashboards (opcional)
- **Logs estruturados** para análise

### Formatos de Saída
- **CSV** para ferramentas tradicionais
- **Parquet** para processamento avançado
- **JSON** para metadados e configuração

---

**Pipeline ETL completo, robusto e escalável para ambientes empresariais.**
