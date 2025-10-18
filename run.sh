#!/bin/bash

# ============================================================================
# Script de Execução do Projeto SiCooperative Data Lake POC
# ============================================================================
# Este script facilita a execução completa do ambiente e pipeline
# ============================================================================

set -e

echo "============================================================================"
echo "  SiCooperative Data Lake POC - Script de Execução"
echo "============================================================================"
echo ""

# Cores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
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

log_header() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Verificar se Docker está rodando
if ! docker info > /dev/null 2>&1; then
    log_error "Docker não está rodando. Por favor, inicie o Docker e tente novamente."
    exit 1
fi

log_info "Docker está disponível ✓"

# Verificar se arquivo .env existe
if [ ! -f ".env" ]; then
    log_warn "Arquivo .env não encontrado. Copiando de .env.example..."
    cp .env.example .env
    log_info "Arquivo .env criado a partir do template ✓"
fi

echo ""
log_header "1. Construindo e iniciando ambiente completo"
echo ""
echo "Comandos equivalentes:"
echo "  cd docker && docker-compose up --build -d"
echo "  OU"
echo "  docker-compose --env-file .env -f docker/docker-compose.yml up --build -d"
echo ""

# Navegar para o diretório docker e executar
cd docker
log_info "Navegando para diretório docker..."

if docker-compose up --build -d; then
    log_info "Ambiente iniciado com sucesso ✓"

    echo ""
    log_header "2. Aguardando serviços ficarem prontos"
    log_info "Aguardando MySQL estar disponível..."

    # Aguardar MySQL ficar saudável
    until docker-compose exec -T mysql mysqladmin ping -h localhost -u root -proot_password --silent; do
        log_warn "MySQL ainda não está pronto. Aguardando..."
        sleep 5
    done

    log_info "MySQL está pronto ✓"

    log_info "Aguardando Spark estar disponível..."
    sleep 10  # Tempo adicional para Spark inicializar completamente

    # Verificar saúde do Spark
    if docker-compose exec -T spark python -c "import pyspark; print('Spark OK')" 2>/dev/null; then
        log_info "Spark está pronto ✓"
    else
        log_warn "Spark ainda inicializando. Continuando..."
    fi

    echo ""
    log_header "3. Executando pipeline ETL"
    echo ""
    echo "Comandos equivalentes:"
    echo "  docker-compose exec spark python /app/src/etl_pipeline.py --output /app/output"
    echo "  OU (com parâmetros personalizados)"
    echo "  docker-compose exec spark python /app/src/etl_pipeline.py --output /app/output --format csv"
    echo ""

    if docker-compose exec spark python /app/src/etl_pipeline.py --output /app/output; then
        log_info "Pipeline executado com sucesso ✓"

        echo ""
        log_header "4. Verificando resultado"
        if [ -f "../output/movimento_flat.csv" ]; then
            log_info "Arquivo CSV gerado com sucesso:"
            ls -lh ../output/movimento_flat.csv
            echo ""
            log_info "Primeiras linhas do arquivo:"
            head -n 5 ../output/movimento_flat.csv
        else
            log_warn "Arquivo CSV não encontrado em ../output/"
        fi
    else
        log_error "Pipeline falhou ❌"
        exit 1
    fi

    echo ""
    log_header "5. Ambiente disponível para uso"
    log_info "Ambiente está rodando. Você pode:"
    echo "  - Executar testes: docker-compose exec spark python -m pytest tests/ -v"
    echo "  - Acessar MySQL: docker-compose exec mysql mysql -u root -p"
    echo "  - Ver logs: docker-compose logs -f spark"
    echo ""
    log_info "Para parar o ambiente: docker-compose down"

else
    log_error "Falha ao iniciar ambiente ❌"
    exit 1
fi

echo ""
log_header "Execução completa!"
log_info "Projeto disponível em: $(pwd)/../"
log_info "Logs disponíveis com: docker-compose logs -f"
echo ""
log_info "Para executar novamente apenas o pipeline (sem rebuild):"
echo "  docker-compose exec spark python /app/src/etl_pipeline.py --output /app/output"
