# ============================================================================
# Dockerfile - Spark Environment
# ============================================================================
# Imagem customizada com PySpark e dependências para o pipeline ETL
# ============================================================================

FROM python:3.11-slim

LABEL maintainer="Desafio Técnico - Engenharia de Dados"
LABEL description="Ambiente Spark para SiCooperative Data Lake POC"

# ============================================================================
# VARIÁVEIS DE AMBIENTE
# ============================================================================

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# ============================================================================
# INSTALAÇÃO DE DEPENDÊNCIAS DO SISTEMA
# ============================================================================

RUN apt-get update && apt-get install -y --no-install-recommends \
    # Java (requerido pelo Spark)
    openjdk-17-jre-headless \
    # Utilitários
    curl \
    wget \
    procps \
    net-tools \
    # Limpeza
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ============================================================================
# CONFIGURAÇÃO DO JAVA
# ============================================================================

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# ============================================================================
# DIRETÓRIO DE TRABALHO
# ============================================================================

WORKDIR /app

# ============================================================================
# INSTALAÇÃO DE DEPENDÊNCIAS PYTHON
# ============================================================================

# Copiar requirements.txt
COPY ../requirements.txt /app/requirements.txt

# Instalar dependências Python
RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

# ============================================================================
# VERIFICAÇÃO DA INSTALAÇÃO
# ============================================================================

RUN python --version && \
    java -version && \
    pip list | grep pyspark

# ============================================================================
# CRIAR DIRETÓRIOS
# ============================================================================

RUN mkdir -p /app/src /app/output /app/logs

# ============================================================================
# HEALTHCHECK
# ============================================================================

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import pyspark; print('OK')" || exit 1

# ============================================================================
# COMANDO PADRÃO
# ============================================================================

# Mantém o container ativo para execução de comandos
CMD ["tail", "-f", "/dev/null"]
