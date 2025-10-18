# Testes Unitários - SiCooperative Data Lake POC

Este diretório contém os testes unitários para validar o pipeline ETL.

## 📋 Estrutura

```
tests/
├── __init__.py                 # Inicialização do pacote de testes
├── conftest.py                 # Fixtures compartilhadas (sessão Spark, dados de exemplo)
├── test_config.py              # Testes do módulo config.py
├── test_utils.py               # Testes do módulo utils.py
├── test_etl_pipeline.py        # Testes do pipeline ETL principal
└── README.md                   # Este arquivo
```

## 🧪 Cobertura de Testes

### **test_config.py** (15 testes)
Valida configurações e métodos de configuração:
- ✅ Formato da URL JDBC
- ✅ Propriedades de conexão MySQL
- ✅ Configurações do Spark
- ✅ Construção de caminhos de output
- ✅ Validação de configurações obrigatórias
- ✅ Nomes e tipos de colunas

### **test_utils.py** (13 testes)
Valida funções auxiliares:
- ✅ Setup de logger
- ✅ Validação de DataFrames (linhas mínimas, colunas obrigatórias)
- ✅ Formatação de duração (segundos, minutos, horas)
- ✅ Exceções customizadas

### **test_etl_pipeline.py** (16 testes)
Valida o pipeline ETL completo:
- ✅ JOINs individuais (movimento+cartao, cartao+conta, conta+associado)
- ✅ Cadeia completa de JOINs
- ✅ Renomeação de colunas
- ✅ Conversão de tipos de dados
- ✅ Formatação de datas
- ✅ Integridade de dados (sem perda em JOINs)
- ✅ Qualidade de dados (sem nulos, valores positivos)
- ✅ Criação de instância do ETL
- ✅ Método transform_and_join

**Total: 44 testes**

## 🚀 Executar Testes

### Todos os testes

```bash
# No diretório raiz do projeto
pytest

# Ou com mais detalhes
pytest -v

# Com output colorido
pytest --color=yes
```

### Testes específicos

```bash
# Apenas testes de config
pytest tests/test_config.py

# Apenas testes de utils
pytest tests/test_utils.py

# Apenas testes do pipeline
pytest tests/test_etl_pipeline.py

# Teste específico
pytest tests/test_config.py::TestConfig::test_mysql_jdbc_url_format
```

### Com cobertura

```bash
# Executar com relatório de cobertura
pytest --cov=src --cov-report=html

# Ver relatório
# Windows: start htmlcov/index.html
# Linux/Mac: open htmlcov/index.html

# Relatório no terminal
pytest --cov=src --cov-report=term-missing
```

### Dentro do Docker

```bash
# Executar testes no container Spark
docker-compose exec spark pytest

# Com cobertura
docker-compose exec spark pytest --cov=src --cov-report=term-missing

# Testes específicos
docker-compose exec spark pytest tests/test_etl_pipeline.py -v
```

## 📊 Fixtures Disponíveis

### Sessão Spark
- **`spark`**: Sessão Spark configurada para testes (scope: session)

### Dados de Exemplo
- **`sample_associado_data`**: Lista com 3 associados
- **`sample_conta_data`**: Lista com 3 contas
- **`sample_cartao_data`**: Lista com 3 cartões
- **`sample_movimento_data`**: Lista com 4 movimentos

### DataFrames
- **`df_associado`**: DataFrame de associados
- **`df_conta`**: DataFrame de contas
- **`df_cartao`**: DataFrame de cartões
- **`df_movimento`**: DataFrame de movimentos

### Outros
- **`expected_output_columns`**: Lista com as 11 colunas esperadas no output

## 🔧 Configuração

### pytest.ini

Configurações do pytest no arquivo `pytest.ini` na raiz do projeto:
- Diretórios de teste: `tests/`
- Padrões de arquivos: `test_*.py`
- Markers customizados: `slow`, `integration`, `unit`
- Configuração de cobertura

### conftest.py

Fixtures compartilhadas entre todos os testes:
- Sessão Spark única para todos os testes (performance)
- Dados de exemplo consistentes
- Schemas definidos para cada tabela

## 📝 Boas Práticas Implementadas

### Organização
- ✅ Testes agrupados em classes por funcionalidade
- ✅ Nomes descritivos de testes
- ✅ Fixtures reutilizáveis

### Cobertura
- ✅ Testes unitários para funções individuais
- ✅ Testes de integração para fluxo completo
- ✅ Testes de qualidade de dados

### Assertions
- ✅ Uso de `assert_df_equality` do chispa para DataFrames
- ✅ Validações específicas (tipos, valores, contagens)
- ✅ Mensagens de erro claras

### Performance
- ✅ Sessão Spark compartilhada (scope: session)
- ✅ Dados de exemplo pequenos
- ✅ Configurações otimizadas (2 partitions, 2 cores)

## 🐛 Troubleshooting

### Erro: "No module named 'src'"

```bash
# Adicionar src ao PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"  # Linux/Mac
set PYTHONPATH=%PYTHONPATH%;%cd%\src          # Windows CMD
$env:PYTHONPATH += ";$(pwd)\src"              # Windows PowerShell

# Ou executar do diretório raiz
cd desafio_tecnico_engenharia_dados
pytest
```

### Erro: "Java not found"

```bash
# Instalar Java (requerido pelo Spark)
# Ubuntu/Debian
sudo apt-get install openjdk-17-jre-headless

# Windows: Baixar e instalar Java JDK 17
# https://www.oracle.com/java/technologies/downloads/

# Verificar instalação
java -version
```

### Erro: "MySQL Connector not found"

```bash
# Instalar dependências
pip install -r requirements.txt

# Ou especificamente
pip install mysql-connector-python
```

### Testes lentos

```bash
# Executar apenas testes rápidos (excluir marcados como slow)
pytest -m "not slow"

# Executar em paralelo (requer pytest-xdist)
pip install pytest-xdist
pytest -n auto
```

## 📚 Frameworks Utilizados

- **pytest**: Framework de testes principal
- **chispa**: Comparação de DataFrames Spark
- **pytest-cov**: Relatórios de cobertura
- **PySpark**: Testes de transformações Spark

## 🎯 Próximos Passos

### Melhorias Futuras
- [ ] Testes de integração com MySQL real
- [ ] Testes de performance (grandes volumes)
- [ ] Testes de falhas e recuperação
- [ ] Mocks para conexões externas
- [ ] Testes de concorrência
- [ ] Property-based testing (hypothesis)

### CI/CD
- [ ] Integração com GitHub Actions
- [ ] Execução automática em PRs
- [ ] Relatórios de cobertura no PR
- [ ] Badge de status no README

## 📖 Referências

- [pytest Documentation](https://docs.pytest.org/)
- [chispa Documentation](https://github.com/MrPowers/chispa)
- [PySpark Testing Best Practices](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)
