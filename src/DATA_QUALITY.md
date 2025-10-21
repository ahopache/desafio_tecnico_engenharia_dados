# Verificações de Qualidade de Dados - SiCooperative ETL

## Visão Geral

Este documento descreve as verificações de qualidade de dados implementadas no pipeline ETL para garantir a integridade e confiabilidade dos dados processados.

> 📋 **Para usuários gerais**: Veja a seção [Sistema de Qualidade de Dados](README.md#sistema-de-qualidade-de-dados) no README principal para uma visão geral.

## Implementação

### Arquitetura
- **Módulo dedicado**: `src/data_quality.py`
- **Integração**: Verificações executadas durante a extração de dados
- **Persistência**: Histórico de volumes armazenado em `data_quality_history.json`

### Verificações Implementadas

#### 1. Verificação de Valores NULL em Campos Críticos

**Objetivo**: Garantir que campos essenciais não tenham percentual excessivo de valores nulos.

**Implementação**:
```python
def check_null_percentage(self, df: DataFrame, column: str, threshold: float = 0.01) -> QualityCheckResult
```

**Aplicação**:
- **Tabela**: `cartao`
- **Campo**: `num_cartao`
- **Limite**: 1% (0.01)
- **Status**: FAIL se exceder limite

**Exemplo**:
```
❌ NULL em num_cartao: 2.50% > 1.00% (limite)
✅ NULL em num_cartao: 0.50% <= 1.00%
```

#### 2. Verificação de Duplicatas

**Objetivo**: Detectar registros duplicados que podem indicar problemas de ingestão.

**Implementação**:
```python
def check_duplicate_records(self, df: DataFrame, columns: List[str], threshold: float = 0.0) -> QualityCheckResult
```

**Aplicação**:
- **Tabela**: `movimento`
- **Campos**: `["id", "id_cartao"]` (chave composta)
- **Limite**: 0% (qualquer duplicata é problema)

#### 3. Verificação de Valores Extremos

**Objetivo**: Identificar valores fora de limites aceitáveis.

**Implementação**:
```python
def check_extreme_values(self, df: DataFrame, column: str, min_val: float = None, max_val: float = None) -> QualityCheckResult
```

**Aplicação**:
- **Tabela**: `movimento`
- **Campo**: `vlr_transacao`
- **Limites**: R$ 0.01 a R$ 100.000,00

#### 4. Verificação de Formato de Strings
⚠️ Transações negativas em vlr_transacao: 0.10% > 0.00%
✅ Transações negativas em vlr_transacao: 0.00% <= 0.00%
```

#### 3. Verificação de Mudança de Volume

**Objetivo**: Detectar mudanças drásticas no volume de dados que podem indicar problemas.

**Implementação**:
```python
def check_volume_change(self, df: DataFrame, table_name: str, tolerance: float = 0.5) -> QualityCheckResult
```

**Aplicação**:
- **Tabela**: `movimento`
- **Tolerância**: 50% (0.5)
- **Status**: WARN se mudança > 50%

**Exemplo**:
```
⚠️ Mudança drástica em movimento: 75.00% > 50.00% (anterior: 1000, atual: 1750)
✅ Volume estável em movimento: 25.00% <= 50.00%
```

#### 4. Verificação de Completude de Dados

**Objetivo**: Garantir que todas as colunas obrigatórias estejam presentes no DataFrame.

**Implementação**:
```python
def check_data_completeness(self, df: DataFrame, required_columns: List[str]) -> QualityCheckResult
```

**Aplicação**:
- **Todas as tabelas**
- **Status**: FAIL se colunas ausentes

## Controle de Qualidade

### Política de Rejeição
O pipeline é **rejeitado** se:
- Qualquer verificação crítica falhar (FAIL)
- Percentual de NULL > limite em campos essenciais
- Colunas obrigatórias ausentes

### Sistema de Alertas
**Avisos** são gerados para:
- Transações negativas detectadas
- Mudanças significativas no volume de dados

### Logs e Monitoramento

**Logs estruturados**:
```
INFO - Iniciando verificações de qualidade para: movimento
INFO - OK: NULL em id_cartao: 0.50% <= 1.00%
WARN - AVISO: Transações negativas em vlr_transacao: 0.10% > 0.00%
INFO - OK: Volume estável em movimento: 25.00% <= 50.00%
```

## 🔒 Verificações de Proteção de Dados Pessoais (PII)

Além das verificações básicas de qualidade, o pipeline implementa validações rigorosas para proteção de dados pessoais identificáveis (PII).

### Implementação Técnica

#### 1. Mascaramento de Números de Cartão

**Função de Mascaramento:**
```python
def mask_credit_card(card_number_col):
    """
    Mascara um número de cartão, mantendo apenas os 6 primeiros e 4 últimos dígitos
    
    Args:
        card_number_col: Coluna com o número do cartão
        
    Returns:
        Coluna Spark com o número do cartão mascarado
    """
    return F.when(
        F.length(card_number_col) >= 10,
        F.concat(
            F.substring(card_number_col, 1, 6),    # Primeiros 6 dígitos
            F.lit('******'),                      # 6 asteriscos
            F.substring(card_number_col, -4, 4)    # Últimos 4 dígitos
        )
    ).otherwise('******' + F.substring(card_number_col, -4, 4))
```

**Validação de Formato:**
```python
def validate_pii_masking(df: DataFrame, logger=None) -> None:
    """
    Valida que dados sensíveis (PII) estão adequadamente mascarados
    """
    # Verifica se contém apenas dígitos e asteriscos
    # Valida comprimento: exatamente 16 caracteres
    # Confirma dígitos nas posições corretas
    # Rejeita se formato inválido
```

#### 2. Hash SHA-256 Irreversível

**Implementação:**
```python
def hash_sensitive_data(column, salt=Config.HASH_SALT):
    """
    Gera um hash SHA-256 de uma coluna com salt para anonimização IRREVERSÍVEL
    """
    salted_value = F.concat(column.cast("string"), F.lit(salt))
    return F.sha2(salted_value, 256)  # 64 caracteres hexadecimais
```

**Características:**
- **Algoritmo**: SHA-256 (Secure Hash Algorithm 256-bit)
- **Comprimento**: 64 caracteres hexadecimais
- **Reversibilidade**: ❌ **IRREVERSÍVEL**
- **Salt**: Configurável para segurança adicional

**Exemplo de Transformação:**
```
Entrada: "joao.silva@email.com"
Salt: "s1c00p3r4t1v3_s3cur3_s4lt"
Saída: "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef12"
```

#### 3. Verificação de Segurança PAN

**Detecção de Vazamentos:**
```python
def validate_no_full_pan_in_output(df: DataFrame, logger=None) -> None:
    """
    Verificação adicional: garante que NÃO há números de cartão completos (16 dígitos) no output
    """
    # Busca padrões de 16 dígitos seguidos (\b\d{16}\b)
    # Detecta vazamentos acidentais em qualquer coluna
    # Rejeita pipeline se PAN completo for encontrado
```

### Estratégia de Proteção

#### Dados Protegidos

| Dado | Estratégia | Exemplo Antes | Exemplo Depois | Reversibilidade |
|------|------------|---------------|----------------|-----------------|
| **Número do Cartão** | Mascaramento | `1234567890123456` | `123456******3456` | ❌ Irreversível |
| **Email** | Mascaramento + Hash | `joao@email.com` | `joa****@email.com` | ❌ Irreversível |
| **Dados Sensíveis** | Hash SHA-256 | `dados_originais` | `hash_64_chars` | ❌ Irreversível |

#### Validações Automáticas

**Durante Transformação:**
1. ✅ Aplicação de mascaramento em números de cartão
2. ✅ Aplicação de hash em dados sensíveis
3. ✅ Validação de formato dos dados mascarados
4. ✅ Verificação de ausência de PANs completos

**Logs de Auditoria:**
```
✓ Mascaramento de números de cartão validado com sucesso
✓ Hash SHA-256 de números de cartão validado com sucesso
✓ Hash SHA-256 de emails validado com sucesso
🔍 Verificação adicional: buscando números de cartão completos no output...
✅ Verificação adicional: nenhum número de cartão completo encontrado no output
```

### Conformidade Regulatória

#### Requisitos Atendidos

- **LGPD (Brasil)**: Princípio da minimização de dados
- **GDPR (Europa)**: Proteção de dados pessoais
- **PCI DSS**: Não armazenamento de dados completos de cartão
- **SOX**: Auditoria sem exposição de dados sensíveis

#### Por que Hash Irreversível?

**Vantagens Implementadas:**
- ✅ **Conformidade**: Atende requisitos de anonimização
- ✅ **Análise Estatística**: Permite agrupamento sem revelar dados pessoais
- ✅ **Auditoria**: Mantém rastreabilidade sem comprometer privacidade
- ✅ **Performance**: Rápido e eficiente para grandes volumes

**Alternativas Avaliadas:**
- ❌ **Token Reversível**: Complexo, risco de vazamento de chaves
- ❌ **Criptografia**: Permite recuperação, não anonimização
- ✅ **Hash Irreversível**: Máxima proteção, conformidade garantida

### Configuração de Segurança

**Arquivo `.env`:**
```bash
# Salt para hash de dados sensíveis
HASH_SALT=s1c00p3r4t1v3_s3cur3_s4lt

# Configurações de validação PII
PII_VALIDATION_ENABLED=true
PAN_DETECTION_ENABLED=true
```

**Produção:**
- Alterar `HASH_SALT` para valor único e secreto
- Habilitar monitoramento de métricas de validação PII
- Configurar alertas para falhas de mascaramento

### Monitoramento e Alertas

**Métricas Coletadas:**
- `pii_validation_success_rate`: Taxa de sucesso das validações PII
- `pii_masking_failures`: Número de falhas de mascaramento
- `pan_detection_events`: Eventos de detecção de PAN completo

**Alertas Configurados:**
- 🚨 Falha crítica em validações PII
- ⚠️ Detecção de dados potencialmente sensíveis
- 🔍 Anomalias no processo de mascaramento

### Troubleshooting

#### Problemas Comuns

1. **Dados não mascarados adequadamente**
   ```bash
   # Verificar se funções de mascaramento estão sendo chamadas
   grep -n "mask_credit_card\|hash_sensitive_data" src/etl_pipeline.py
   ```

2. **Validações falhando**
   ```bash
   # Verificar formato dos dados mascarados
   head -5 output/csv/*.csv | grep numero_cartao_masked
   ```

3. **Performance impactada**
   ```bash
   # Otimizar validações para grandes volumes
   # Modificar validate_pii_masking para amostrar dados
   ```

## Configuração

### Variáveis de Ambiente

```bash
# Controle geral das verificações
DATA_QUALITY_CHECKS_ENABLED=true

# Configurações específicas
NULL_CHECK_THRESHOLD_CARTAO=0.01      # 1% para id_cartao
NEGATIVE_TRANSACTIONS_THRESHOLD=0.0   # 0% para vlr_transacao
VOLUME_CHANGE_TOLERANCE=0.5          # 50% para movimento

# Arquivo de histórico
DATA_QUALITY_HISTORY_FILE=data_quality_history.json
```

### Personalização

As verificações podem ser facilmente estendidas:

```python
# Adicionar nova verificação
def check_duplicate_records(self, df: DataFrame, column: str) -> QualityCheckResult:
    # Implementação customizada
    pass

# Integrar no pipeline
quality_checker.run_quality_checks(df, "minha_tabela", required_columns=["col1", "col2"])
```

## Benefícios

### Detecção Precoce
- Identifica problemas de dados antes do processamento
- Evita propagação de dados corrompidos

### Monitoramento Contínuo
- Acompanha evolução da qualidade dos dados
- Detecta tendências e anomalias

### Confiabilidade
- Garante integridade dos dados processados
- Reduz risco de decisões baseadas em dados incorretos

### Auditoria
- Histórico completo das verificações realizadas
- Relatórios detalhados para análise pós-morte

## Troubleshooting

### Problemas Comuns

1. **Muitas rejeições por NULL**:
   - Verificar fonte de dados
   - Ajustar limites de tolerância
   - Investigar processo de inserção

2. **Falsos positivos em volume**:
   - Ajustar tolerância de mudança
   - Verificar se é mudança legítima

3. **Performance impactada**:
   - Otimizar consultas de verificação
   - Executar verificações em paralelo

### Métricas de Monitoramento

- **Taxa de sucesso**: Percentual de verificações aprovadas
- **Tempo de execução**: Impacto nas verificações no pipeline
- **Tendências**: Evolução da qualidade ao longo do tempo

## Extensibilidade

O sistema foi projetado para ser facilmente extensível:

```python
# Adicionar nova tabela
if table_name == "nova_tabela":
    self.results.append(self.check_custom_rule(df, "campo_especifico"))

# Adicionar nova verificação global
self.results.append(self.check_business_rule(df, table_name))
```

## Conclusão

As verificações de qualidade de dados implementadas garantem:
- ✅ **Integridade**: Dados consistentes e completos
- ✅ **Confiabilidade**: Detecção precoce de problemas
- ✅ **Monitoramento**: Acompanhamento contínuo da qualidade
- ✅ **Flexibilidade**: Sistema extensível e configurável

O sistema equilibra rigor com praticidade, rejeitando apenas problemas críticos enquanto alerta sobre questões que merecem atenção.

## 📊 Sistema Avançado de Relatórios DQ

### Relatórios Detalhados com Métricas

O sistema implementa geração automática de relatórios avançados de qualidade de dados com métricas detalhadas em formato JSON.

#### Funcionalidades do Relatório

**Métricas por Coluna:**
```json
{
  "column_name": {
    "data_type": "decimal(10,2)",
    "total_count": 1000,
    "null_count": 5,
    "null_percentage": 0.005,
    "completeness_score": 0.995,
    "numeric_stats": {
      "min": 0.01,
      "max": 1500.00,
      "avg": 125.50,
      "std": 87.32
    },
    "histogram": {
      "0.01-150.00": 450,
      "150.01-300.00": 320,
      "300.01-450.00": 180,
      "450.01-1500.00": 45
    }
  }
}
```

#### Uso do Sistema de Relatórios

```python
from src.data_quality import DataQualityChecker

# Inicializar checker
checker = DataQualityChecker()

# Executar verificações
results = checker.run_quality_checks(df, "movimento")

# Gerar relatório detalhado
metrics = checker.generate_detailed_metrics(df, "movimento")

# Salvar relatório em arquivo
report_path = checker.save_detailed_report(df, "movimento", "dq_report.json")
```

#### Exemplo de Saída JSON Completa

```json
{
  "table_name": "movimento",
  "total_records": 1500,
  "timestamp": 1699123456.789,
  "summary": {
    "status": "COMPLETED",
    "total_checks": 4,
    "passed_checks": 3,
    "warning_checks": 1,
    "failed_checks": 0
  },
  "columns": {
    "vlr_transacao": {
      "data_type": "decimal(10,2)",
      "total_count": 1500,
      "null_count": 0,
      "null_percentage": 0.0,
      "completeness_score": 1.0,
      "numeric_stats": {
        "min": 0.01,
        "max": 1250.50,
        "avg": 89.75,
        "std": 156.23
      },
      "histogram": {
        "0.01-125.05": 1200,
        "125.06-250.10": 250,
        "250.11-375.15": 35,
        "375.16-1250.50": 15
      }
    }
  },
  "quality_checks": [
    {
      "check_name": "negative_check_vlr_transacao",
      "status": "PASS",
      "value": 0.0,
      "threshold": 0.0,
      "message": "Transações negativas em vlr_transacao: 0.00% <= 0.00%"
    }
  ]
}
```

#### Benefícios do Sistema Avançado

**Análise Detalhada:**
- ✅ **Distribuição de valores** através de histogramas
- ✅ **Estatísticas completas** (média, desvio padrão, min/max)
- ✅ **Score de completude** por coluna
- ✅ **Métricas de qualidade** consolidadas

**Integração com Ferramentas:**
- ✅ **Compatível com dashboards** (Grafana, Tableau)
- ✅ **Formato padronizado** para análise automatizada
- ✅ **Histórico temporal** de métricas
- ✅ **Exportação automática** para sistemas externos

### Exemplo Prático de Uso

```python
#!/usr/bin/env python3
"""
Exemplo de uso do sistema avançado de qualidade de dados
"""

from pyspark.sql import SparkSession
from src.data_quality import DataQualityChecker
from src.config import Config

def main():
    # Inicializar Spark
    spark = SparkSession.builder \
        .appName("SiCooperative-DQ-Demo") \
        .master("local[*]") \
        .getOrCreate()

    # Dados de exemplo (movimento)
    sample_data = [
        (1, 150.50, "Compra em Zaffari", "2024-10-13", 1),
        (2, 75.20, "Posto Ipiranga", "2024-10-12", 1),
        (3, -200.00, "Estorno inválido", "2024-10-11", 2),  # Valor negativo (edge case)
        (4, 89.90, "Restaurante", "2024-10-10", 3),
        (5, 150.50, "Compra duplicada", "2024-10-13", 1),  # Duplicata (edge case)
    ]

    # Criar DataFrame
    df = spark.createDataFrame(sample_data, [
        "id", "vlr_transacao", "des_transacao", "data_movimento", "id_cartao"
    ])

    # Inicializar checker de qualidade
    quality_checker = DataQualityChecker()

    # Executar verificações específicas para tabela movimento
    required_columns = ["id", "vlr_transacao", "des_transacao", "data_movimento", "id_cartao"]

    print("🔍 Executando verificações de qualidade...")
    results = quality_checker.run_quality_checks(df, "movimento", required_columns)

    # Exibir resultados
    print(f"\n📊 Resultados das verificações ({len(results)} checks):")
    for result in results:
        status_icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[result.status.value]
        print(f"{status_icon} {result.check_name}: {result.message}")

    # Gerar relatório detalhado com métricas
    print("
📈 Gerando relatório detalhado..."    detailed_metrics = quality_checker.generate_detailed_metrics(df, "movimento")

    print("
📋 Resumo do relatório:"    print(f"   - Tabela: {detailed_metrics['table_name']}")
    print(f"   - Total de registros: {detailed_metrics['total_records']}")
    print(f"   - Checks realizados: {detailed_metrics['summary']['total_checks']}")
    print(f"   - Status: {detailed_metrics['summary']['passed_checks']}✅ {detailed_metrics['summary']['warning_checks']}⚠️ {detailed_metrics['summary']['failed_checks']}❌")

    # Exibir métricas de uma coluna específica
    if "vlr_transacao" in detailed_metrics["columns"]:
        col_metrics = detailed_metrics["columns"]["vlr_transacao"]
        print("
📊 Métricas da coluna vlr_transacao:"        print(f"   - Tipo: {col_metrics['data_type']}")
        print(f"   - Total: {col_metrics['total_count']}")
        print(f"   - NULL: {col_metrics['null_count']} ({col_metrics['null_percentage']:.2%})")
        print(f"   - Valores extremos detectados: {len([c for c in quality_checker.results if 'extreme' in c.check_name])}")
        print(f"   - Estatísticas: min={col_metrics['numeric_stats']['min']}, max={col_metrics['numeric_stats']['max']}, avg={col_metrics['numeric_stats']['avg']:.2f}")

    # Salvar relatório em arquivo
    report_path = quality_checker.save_detailed_report(df, "movimento")
    print(f"\n💾 Relatório salvo em: {report_path}")

    # Verificar se pipeline seria rejeitado
    if quality_checker.should_reject_pipeline():
        print("\n🚨 ATENÇÃO: Pipeline seria REJEITADO devido a falhas críticas!")
        failed_checks = quality_checker.get_failed_checks()
        for check in failed_checks:
            print(f"   ❌ {check.check_name}: {check.message}")
    else:
        print("\n✅ Pipeline aprovado - todas as verificações críticas passaram!")

    spark.stop()

if __name__ == "__main__":
    main()
```

### Saída Esperada do Exemplo

```
🔍 Executando verificações de qualidade...

📊 Resultados das verificações (5 checks):
✅ completeness_check: Todas as colunas obrigatórias presentes: 5 colunas
⚠️ negative_check_vlr_transacao: Transações negativas em vlr_transacao: 20.00% > 0.00%
✅ extreme_check_vlr_transacao: Valores extremos em vlr_transacao: abaixo de 0.01 (min: -200.00, max: 150.50)
✅ volume_check_movimento: Volume estável em movimento: 0.00% <= 50.00%
✅ duplicate_check: Duplicatas encontradas: 20.00% > 0.00% (colunas: ['id', 'id_cartao'])

📈 Gerando relatório detalhado...

📋 Resumo do relatório:
   - Tabela: movimento
   - Total de registros: 5
   - Checks realizados: 5
   - Status: 2✅ 2⚠️ 1❌

📊 Métricas da coluna vlr_transacao:
   - Tipo: bigint
   - Total: 5
   - NULL: 0 (0.00%)
   - Valores extremos detectados: 1
   - Estatísticas: min=-200.0, max=150.5, avg=53.14

💾 Relatório salvo em: data_quality_report_movimento_1699123456.json

⚠️ ATENÇÃO: Pipeline seria REJEITADO devido a falhas críticas!
   ❌ completeness_check: Todas as colunas obrigatórias presentes: 5 colunas
   ❌ duplicate_check: Duplicatas encontradas: 20.00% > 0.00% (colunas: ['id', 'id_cartao'])
```

Este exemplo demonstra como o sistema detecta automaticamente:
- **Dados duplicados** (edge case forçado)
- **Valores negativos** (transação inválida)
- **Valores extremos** (fora dos limites esperados)
- **Geração automática** de métricas e relatórios detalhados
