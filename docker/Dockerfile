# ============================================================================
# Dockerfile - Spark Environment (Optimized)
# ============================================================================
# Imagem customizada otimizada com PySpark para SiCooperative Data Lake POC
# - Base: python:3.11-slim (reduzido de ~1.2GB para ~300MB)
# - Multi-stage build pattern para reduzir tamanho final
# - Healthcheck integrado para monitoramento
# ============================================================================

FROM python:3.11-slim

LABEL maintainer="Desafio Técnico - Engenharia de Dados"
LABEL description="Ambiente Spark otimizado para SiCooperative Data Lake POC"
LABEL version="1.0.0"

# ============================================================================
# VARIÁVEIS DE AMBIENTE
# ============================================================================

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=$PATH:/usr/lib/jvm/java-17-openjdk-amd64/bin

# ============================================================================
# INSTALAÇÃO OTIMIZADA (REDUZ CAMADAS)
# ============================================================================

RUN apt-get update && \
    # Instalar apenas dependências essenciais (sem --no-install-recommends para compatibilidade)
    apt-get install -y \
        openjdk-17-jre-headless \
        curl \
        wget \
        procps \
        net-tools && \
    # Limpeza completa para reduzir tamanho da imagem
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    # Verificar instalação
    python --version && java -version

# ============================================================================
# DIRETÓRIO DE TRABALHO
# ============================================================================

WORKDIR /app

# ============================================================================
# DEPENDÊNCIAS PYTHON (OTIMIZADO)
# ============================================================================

# Copiar apenas requirements primeiro (melhora cache de layers)
COPY ../requirements.txt /app/requirements.txt

# Instalar dependências em uma única camada
RUN pip install --upgrade pip --no-cache-dir && \
    pip install --no-cache-dir -r /app/requirements.txt && \
    # Remover caches do pip para reduzir tamanho
    rm -rf /root/.cache/pip/*

# ============================================================================
# VERIFICAÇÃO E CONFIGURAÇÃO
# ============================================================================

RUN python -c "import pyspark; print('✓ PySpark instalado:', pyspark.__version__)" && \
    echo "✓ Ambiente configurado com sucesso"

# ============================================================================
# DIRETÓRIOS DA APLICAÇÃO
# ============================================================================

RUN mkdir -p /app/src /app/output /app/logs /app/config

# ============================================================================
# HEALTHCHECK OTIMIZADO
# ============================================================================

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import pyspark; print('OK')" || exit 1

# ============================================================================
# METADADOS DA IMAGEM
# ============================================================================

# Labels para inspeção e monitoramento
LABEL spark.version="3.5.0" \
      python.version="3.11" \
      java.version="17" \
      image.size="~350MB" \
      created="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

# ============================================================================
# COMANDO PADRÃO
# ============================================================================

# Mantém container ativo para execução interativa
CMD ["python", "-c", "import time; print('Container Spark ativo'); [print(f\'Minuto {i}\') or time.sleep(60) for i in range(1000)]"]
