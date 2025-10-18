@echo off
REM ============================================================================
REM Script de Execução do Pipeline ETL (Windows)
REM ============================================================================
REM Este script facilita a execução do pipeline dentro do container Docker
REM ============================================================================

setlocal enabledelayedexpansion

echo ============================================================================
echo   SiCooperative Data Lake POC - Pipeline ETL
echo ============================================================================
echo.

REM Verificar se Docker está rodando
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker nao esta rodando. Por favor, inicie o Docker Desktop.
    exit /b 1
)

REM Verificar se containers estão rodando
docker-compose ps | findstr "Up" >nul 2>&1
if errorlevel 1 (
    echo [WARN] Containers nao estao rodando. Iniciando...
    docker-compose up -d
    
    echo [INFO] Aguardando MySQL estar pronto...
    timeout /t 15 /nobreak >nul
)

REM Verificar saúde do MySQL
echo [INFO] Verificando saude do MySQL...
:wait_mysql
docker-compose exec -T mysql mysqladmin ping -h localhost -u root -proot_password --silent >nul 2>&1
if errorlevel 1 (
    echo [WARN] MySQL ainda nao esta pronto. Aguardando...
    timeout /t 5 /nobreak >nul
    goto wait_mysql
)

echo [INFO] MySQL esta pronto!
echo.

REM Executar pipeline
echo [INFO] Executando pipeline ETL...
echo.

docker-compose exec spark python src/etl_pipeline.py %*

if errorlevel 1 (
    echo.
    echo [ERROR] ============================================================================
    echo [ERROR]   Pipeline falhou!
    echo [ERROR] ============================================================================
    exit /b 1
) else (
    echo.
    echo [INFO] ============================================================================
    echo [INFO]   Pipeline executado com sucesso!
    echo [INFO] ============================================================================
    echo [INFO] Arquivo gerado: output\movimento_flat.csv
    echo.
    
    REM Mostrar primeiras linhas do CSV se existir
    if exist "..\output\movimento_flat.csv" (
        echo [INFO] Primeiras 5 linhas do CSV:
        powershell -Command "Get-Content '..\output\movimento_flat.csv' -Head 6"
    )
)

endlocal
