#!/bin/bash

# ============================================================================
# Script de Configuração de Secrets - SiCooperative Data Lake POC
# ============================================================================
# Este script facilita a configuração inicial dos secrets para desenvolvimento
#
# Uso: ./setup_secrets.sh [dev|prod]
#
# dev:  Configuração básica para desenvolvimento
# prod: Gera senhas seguras automaticamente
# ============================================================================

set -e

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

generate_secure_password() {
    # Gera senha segura de 32 caracteres
    openssl rand -hex 16
}

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_DIR="$SCRIPT_DIR/secrets"
ENVIRONMENT="${1:-dev}"

log "Configurando secrets para ambiente: $ENVIRONMENT"

# ============================================================================
# CRIAÇÃO DE DIRETÓRIOS
# ============================================================================

if [ ! -d "$SECRETS_DIR" ]; then
    log "Criando diretório de secrets..."
    mkdir -p "$SECRETS_DIR"
else
    log "Diretório de secrets já existe"
fi

# ============================================================================
# CONFIGURAÇÃO POR AMBIENTE
# ============================================================================

case "$ENVIRONMENT" in
    "dev")
        log "Configuração para desenvolvimento..."

        # Senhas simples para desenvolvimento
        MYSQL_ROOT_PASSWORD="root_password"
        MYSQL_USER_PASSWORD="etl_password"
        ;;

    "prod")
        log "Configuração para produção..."

        # Gerar senhas seguras
        MYSQL_ROOT_PASSWORD=$(generate_secure_password)
        MYSQL_USER_PASSWORD=$(generate_secure_password)
        ;;

    *)
        log "ERRO: Ambiente inválido. Use 'dev' ou 'prod'"
        exit 1
        ;;
esac

# ============================================================================
# CRIAÇÃO DOS ARQUIVOS DE SECRETS
# ============================================================================

log "Criando arquivos de secrets..."

echo "$MYSQL_ROOT_PASSWORD" > "$SECRETS_DIR/mysql_root_password"
echo "$MYSQL_USER_PASSWORD" > "$SECRETS_DIR/mysql_password"

# ============================================================================
# DEFINIR PERMISSÕES
# ============================================================================

log "Definindo permissões restritivas..."
chmod 600 "$SECRETS_DIR/mysql_root_password"
chmod 600 "$SECRETS_DIR/mysql_password"

# ============================================================================
# VALIDAÇÃO
# ============================================================================

log "Validando configuração..."

if [ -f "$SECRETS_DIR/mysql_root_password" ] && [ -f "$SECRETS_DIR/mysql_password" ]; then
    log "Secrets configurados com sucesso!"
    log ""
    log "Resumo da configuração:"
    log "  Ambiente: $ENVIRONMENT"
    log "  Arquivos criados:"
    ls -la "$SECRETS_DIR/"
    log ""
    log "Próximos passos:"
    log "1. Configure o arquivo .env baseado no .env.example"
    log "2. Execute: docker-compose up -d"
    log ""
    log "IMPORTANTE:"
    log "- Nunca commite arquivos de secrets no repositório"
    log "- Mantenha backups seguros das senhas"
    log "- Em produção, use sistemas de gerenciamento de segredos"

    if [ "$ENVIRONMENT" = "prod" ]; then
        log ""
        log "CREDENCIAIS DE PRODUÇÃO GERADAS:"
        log "  MySQL Root Password: $MYSQL_ROOT_PASSWORD"
        log "  MySQL User Password: $MYSQL_USER_PASSWORD"
        log ""
        log "Anote essas senhas em local seguro!"
    fi

else
    log "ERRO: Falha na criação dos arquivos de secrets"
    exit 1
fi
