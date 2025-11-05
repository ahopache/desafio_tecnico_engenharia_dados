@echo off
REM ============================================================================
REM Script de Execução do Projeto SiCooperative Data Lake POC (Windows)
REM ============================================================================
REM Este script facilita a execução completa do ambiente e pipeline no Windows
REM ============================================================================

setlocal enabledelayedexpansion

:: Configuração de cores (Windows 10+)
if not defined PUSHD (
    for /f "usebackq tokens=1,2 delims=#" %%a in (`"prompt #$H#$E# & echo on & for %%b in (1) do rem"`) do (
        set "DEL=%%a"
    )
)

:: Cores ANSI (funcionam no Windows 10+ com suporte a cores ativado)
set "GREEN=!DEL! [32m"
set "YELLOW=!DEL! [33m"
set "RED=!DEL! [31m"
set "BLUE=!DEL! [34m"
set "NC=!DEL! [0m"

:: Função para log
:log_info
echo %GREEN%[INFO]%NC% %*
goto :eof

:log_warn
echo %YELLOW%[WARN]%NC% %*
goto :eof

:log_error
echo %RED%[ERROR]%NC% %*
goto :eof

:log_success
echo %GREEN%[SUCCESS]%NC% %*
goto :eof

:main
    echo %BLUE%============================================================================%NC%
    echo   SiCooperative Data Lake POC - Script de Execucao (Windows)
    echo %BLUE%============================================================================%NC%
    echo.

    call :log_info "Verificando pre-requisitos..."
    
    :: Verifica se o Docker está em execução
    docker info >nul 2>&1
    if %ERRORLEVEL% NEQ 0 (
        call :log_error "Docker nao esta em execucao. Por favor, inicie o Docker Desktop e tente novamente."
        exit /b 1
    )

    call :log_info "Iniciando ambiente com Docker Compose..."
    docker-compose up -d
    
    if %ERRORLEVEL% NEQ 0 (
        call :log_error "Falha ao iniciar os containers Docker."
        exit /b 1
    )
    
    call :log_info "Aguardando inicializacao dos servicos..."
    timeout /t 15 /nobreak >nul
    
    call :log_info "Executando o pipeline ETL..."
    python -m src.etl_pipeline --output ./output
    
    if %ERRORLEVEL% EQU 0 (
        call :log_success "Pipeline executado com sucesso!"
    ) else (
        call :log_error "Erro na execucao do pipeline."
    )
    
    echo.
    call :log_info "Status dos containers:"
    docker-compose ps
    
    echo.
    call :log_info "Para encerrar os containers, execute: docker-compose down"
    
goto :eof

:: Ponto de entrada principal
call :main

endlocal
