#!/bin/bash

# ============================================================================
# Script de Execução do Pipeline ETL
# ============================================================================
# Este script facilita a execução do pipeline dentro do container Docker
# ============================================================================

set -e

echo "============================================================================"
echo "  SiCooperative Data Lake POC - Pipeline ETL"
echo "============================================================================"
echo ""

# Cores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Função para log
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Verificar se Docker está rodando
if ! docker info > /dev/null 2>&1; then
    log_error "Docker não está rodando. Por favor, inicie o Docker."
    exit 1
fi

# Verificar se containers estão rodando
if ! docker-compose ps | grep -q "Up"; then
    log_warn "Containers não estão rodando. Iniciando..."
    docker-compose up -d
    
    log_info "Aguardando MySQL estar pronto..."
    sleep 10
fi

# Verificar saúde do MySQL
log_info "Verificando saúde do MySQL..."
until docker-compose exec -T mysql mysqladmin ping -h localhost -u root -proot_password --silent; do
    log_warn "MySQL ainda não está pronto. Aguardando..."
    sleep 5
done

log_info "✓ MySQL está pronto!"
echo ""

# Executar pipeline
log_info "Executando pipeline ETL..."
echo ""

docker-compose exec spark python src/etl_pipeline.py "$@"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    log_info "============================================================================"
    log_info "  Pipeline executado com sucesso!"
    log_info "============================================================================"
    log_info "Arquivo gerado: output/movimento_flat.csv"
    echo ""
    
    # Mostrar primeiras linhas do CSV
    if [ -f "../output/movimento_flat.csv" ]; then
        log_info "Primeiras 5 linhas do CSV:"
        head -n 6 ../output/movimento_flat.csv
    fi
else
    log_error "============================================================================"
    log_error "  Pipeline falhou com código de erro: $EXIT_CODE"
    log_error "============================================================================"
    exit $EXIT_CODE
fi
