# Verificações de Qualidade de Dados - SiCooperative ETL

## Visão Geral

Este documento descreve as verificações de qualidade de dados implementadas no pipeline ETL para garantir a integridade e confiabilidade dos dados processados.

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
- **Campo**: `id_cartao`
- **Limite**: 1% (0.01)
- **Status**: FAIL se exceder limite

**Exemplo**:
```
❌ NULL em id_cartao: 2.50% > 1.00% (limite)
✅ NULL em id_cartao: 0.50% <= 1.00%
```

#### 2. Verificação de Transações Negativas

**Objetivo**: Alertar sobre presença de valores negativos em campos financeiros.

**Implementação**:
```python
def check_negative_transactions(self, df: DataFrame, column: str, threshold: float = 0.0) -> QualityCheckResult
```

**Aplicação**:
- **Tabela**: `movimento`
- **Campo**: `vlr_transacao`
- **Limite**: 0% (qualquer valor negativo)
- **Status**: WARN se detectar negativos

**Exemplo**:
```
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
