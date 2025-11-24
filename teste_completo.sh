#!/bin/bash
"""
Script de exemplo para executar o sistema completo de testes
Este script demonstra como usar o ambiente de teste end-to-end
"""

echo "🚀 Iniciando demonstração completa do ambiente de teste..."
echo "=================================================="

# 1. Verificar se Docker está disponível
echo "🔍 Verificando Docker..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker não encontrado. Instale o Docker primeiro."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose não encontrado. Instale o Docker Compose primeiro."
    exit 1
fi

echo "✅ Docker e Docker Compose disponíveis"

# 2. Subir ambiente Docker
echo ""
echo "🏗️ Iniciando ambiente Docker Compose..."
cd "$(dirname "$0")"

# Subir apenas o MySQL primeiro (para inicialização)
docker-compose -f docker/docker-compose.yml up -d mysql

echo "⏳ Aguardando MySQL ficar disponível..."
sleep 30

# 3. Verificar se o banco foi criado
echo ""
echo "🔍 Verificando banco de dados..."
if docker-compose exec mysql mysql -u root -proot_password -e "USE sicooperative_test; SELECT COUNT(*) FROM associado;" 2>/dev/null | grep -q "0"; then
    echo "⚠️ Banco vazio, executando script de dados..."

    # Executar script SQL de inicialização
    docker-compose exec -T mysql mysql -u root -proot_password sicooperative_test < sql/init_test_db.sql

    echo "✅ Dados básicos inseridos"
else
    echo "✅ Banco já contém dados"
fi

# 4. Executar pipeline de teste
echo ""
echo "🏃 Executando pipeline ETL de teste..."

# Usar configurações de teste
export MYSQL_HOST=mysql
export MYSQL_PORT=3306
export MYSQL_DATABASE=sicooperative_test
export MYSQL_USER=test_user
export MYSQL_PASS=test_password
export OUTPUT_DIR=./test_output
export LOG_LEVEL=INFO
export OBSERVABILITY_ENABLED=true
export DATA_QUALITY_CHECKS_ENABLED=true

# Executar pipeline
python src/etl_pipeline.py

if [ $? -eq 0 ]; then
    echo "✅ Pipeline executado com sucesso!"
else
    echo "❌ Pipeline falhou!"
    exit 1
fi

# 5. Verificar output
echo ""
echo "📊 Verificando arquivo de saída..."

if [ -f "test_output/csv/movimento_flat.csv" ]; then
    echo "✅ Arquivo CSV gerado:"
    ls -la test_output/csv/movimento_flat.csv

    # Mostrar primeiras linhas
    echo ""
    echo "📋 Primeiras 5 linhas do arquivo:"
    head -5 test_output/csv/movimento_flat.csv

    # Estatísticas básicas
    echo ""
    echo "📊 Estatísticas do arquivo:"
    wc -l test_output/csv/movimento_flat.csv
    echo "Colunas encontradas:"
    head -1 test_output/csv/movimento_flat.csv | tr ',' '\n' | wc -l
else
    echo "❌ Arquivo CSV não encontrado!"
    exit 1
fi

# 6. Executar teste de integração (opcional)
echo ""
echo "🧪 Executando teste de integração..."
if python -m pytest tests/test_integration.py::TestSiCooperativeIntegration::test_full_pipeline_integration -v --tb=short; then
    echo "✅ Teste de integração passou!"
else
    echo "❌ Teste de integração falhou!"
    exit 1
fi

# 7. Limpeza
echo ""
echo "🧹 Limpando ambiente..."
docker-compose down -v

echo ""
echo "🎉 Demonstração concluída com sucesso!"
echo "================================================"
echo ""
echo "📋 Resumo:"
echo "   ✅ Ambiente Docker iniciado"
echo "   ✅ Banco de dados configurado"
echo "   ✅ Dados de teste inseridos"
echo "   ✅ Pipeline ETL executado"
echo "   ✅ Arquivo CSV gerado e validado"
echo "   ✅ Teste de integração passou"
echo "   ✅ Ambiente limpo"
