# Tools - Scripts de Ambiente

Esta pasta contém scripts utilitários para configurar e otimizar o ambiente de desenvolvimento no Windows.

## Arquivos

### `setup_hadoop_windows.py`
- **Propósito:** Configura Hadoop para Windows
- **Quando usar:** Se precisar de funcionalidades específicas do Hadoop
- **Executar:** `python tools/setup_hadoop_windows.py`

### `setup_spark_windows.py`
- **Propósito:** Otimiza ambiente Spark para Windows
- **Quando usar:** Para reduzir warnings de limpeza de arquivos temporários
- **Executar:** `python tools/setup_spark_windows.py`

## Uso

Esses scripts são **opcionais** e só devem ser executados se você encontrar problemas específicos no ambiente Windows:

1. **Problemas com Hadoop:** Execute `setup_hadoop_windows.py`
2. **Warnings de arquivos temporários:** Execute `setup_spark_windows.py`

## Nota

- Esses scripts não são necessários para execução normal do pipeline
- São ferramentas de desenvolvimento/troubleshooting
- Não afetam o código de produção
