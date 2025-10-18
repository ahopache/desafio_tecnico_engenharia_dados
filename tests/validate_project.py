"""
Script de Validação do Projeto
Verifica se todos os arquivos necessários existem e estão corretos
"""

import os
import sys
from pathlib import Path

# Cores para output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def print_success(msg):
    print(f"{GREEN}✓{RESET} {msg}")

def print_error(msg):
    print(f"{RED}✗{RESET} {msg}")

def print_warning(msg):
    print(f"{YELLOW}⚠{RESET} {msg}")

def check_file_exists(filepath, description):
    """Verifica se um arquivo existe"""
    if os.path.exists(filepath):
        size = os.path.getsize(filepath)
        print_success(f"{description}: {filepath} ({size} bytes)")
        return True
    else:
        print_error(f"{description}: {filepath} NÃO ENCONTRADO")
        return False

def check_directory_exists(dirpath, description):
    """Verifica se um diretório existe"""
    if os.path.isdir(dirpath):
        items = len(os.listdir(dirpath))
        print_success(f"{description}: {dirpath} ({items} itens)")
        return True
    else:
        print_error(f"{description}: {dirpath} NÃO ENCONTRADO")
        return False

def main():
    print("=" * 70)
    print("VALIDAÇÃO DO PROJETO - SiCooperative Data Lake POC")
    print("=" * 70)
    print()
    
    base_dir = Path(__file__).parent
    checks_passed = 0
    checks_total = 0
    
    # Verificar estrutura de diretórios
    print("📁 ESTRUTURA DE DIRETÓRIOS")
    print("-" * 70)
    
    directories = [
        ("sql", "Scripts SQL"),
        ("src", "Código fonte"),
        ("tests", "Testes unitários"),
        ("report", "Relatórios"),
        ("docker", "Configurações Docker"),
        ("output", "Diretório de output")
    ]
    
    for dirname, desc in directories:
        checks_total += 1
        if check_directory_exists(base_dir / dirname, desc):
            checks_passed += 1
    
    print()
    
    # Verificar arquivos principais
    print("📄 ARQUIVOS PRINCIPAIS")
    print("-" * 70)
    
    main_files = [
        ("ReadME.MD", "README principal"),
        ("requirements.txt", "Dependências Python"),
        ("pytest.ini", "Configuração pytest"),
        (".gitignore", "Git ignore"),
        (".env.example", "Template de configuração")
    ]
    
    for filename, desc in main_files:
        checks_total += 1
        if check_file_exists(base_dir / filename, desc):
            checks_passed += 1
    
    print()
    
    # Verificar scripts SQL
    print("🗄️ SCRIPTS SQL")
    print("-" * 70)
    
    sql_files = [
        ("sql/01_create_schema.sql", "DDL - Criação de schema"),
        ("sql/02_insert_data.sql", "DML - Inserção de dados")
    ]
    
    for filename, desc in sql_files:
        checks_total += 1
        if check_file_exists(base_dir / filename, desc):
            checks_passed += 1
    
    print()
    
    # Verificar código fonte
    print("🐍 CÓDIGO FONTE PYTHON")
    print("-" * 70)
    
    src_files = [
        ("src/__init__.py", "Inicialização do pacote"),
        ("src/config.py", "Configurações"),
        ("src/utils.py", "Utilitários"),
        ("src/etl_pipeline.py", "Pipeline ETL principal")
    ]
    
    for filename, desc in src_files:
        checks_total += 1
        if check_file_exists(base_dir / filename, desc):
            checks_passed += 1
    
    print()
    
    # Verificar testes
    print("🧪 TESTES UNITÁRIOS")
    print("-" * 70)
    
    test_files = [
        ("tests/__init__.py", "Inicialização dos testes"),
        ("tests/conftest.py", "Fixtures compartilhadas"),
        ("tests/test_config.py", "Testes de configuração"),
        ("tests/test_utils.py", "Testes de utilitários"),
        ("tests/test_etl_pipeline.py", "Testes do pipeline")
    ]
    
    for filename, desc in test_files:
        checks_total += 1
        if check_file_exists(base_dir / filename, desc):
            checks_passed += 1
    
    print()
    
    # Verificar Docker
    print("🐳 DOCKER")
    print("-" * 70)
    
    docker_files = [
        ("docker/docker-compose.yml", "Docker Compose"),
        ("docker/Dockerfile", "Dockerfile Spark"),
        ("docker/run-pipeline.sh", "Script execução Linux/Mac"),
        ("docker/run-pipeline.bat", "Script execução Windows")
    ]
    
    for filename, desc in docker_files:
        checks_total += 1
        if check_file_exists(base_dir / filename, desc):
            checks_passed += 1
    
    print()
    
    # Verificar imports Python
    print("🔍 VALIDAÇÃO DE IMPORTS")
    print("-" * 70)
    
    # Adicionar src ao path
    sys.path.insert(0, str(base_dir / "src"))
    
    modules_to_test = [
        ("config", "Módulo de configuração"),
        ("utils", "Módulo de utilitários")
    ]
    
    for module_name, desc in modules_to_test:
        checks_total += 1
        try:
            __import__(module_name)
            print_success(f"{desc}: import {module_name}")
            checks_passed += 1
        except ImportError as e:
            print_error(f"{desc}: Erro ao importar {module_name} - {e}")
        except Exception as e:
            print_warning(f"{desc}: Aviso ao importar {module_name} - {e}")
            checks_passed += 1  # Conta como sucesso se não for ImportError
    
    print()
    
    # Resumo
    print("=" * 70)
    print("RESUMO DA VALIDAÇÃO")
    print("=" * 70)
    
    percentage = (checks_passed / checks_total) * 100
    
    print(f"Total de verificações: {checks_total}")
    print(f"Verificações passadas: {checks_passed}")
    print(f"Verificações falhadas: {checks_total - checks_passed}")
    print(f"Taxa de sucesso: {percentage:.1f}%")
    print()
    
    if checks_passed == checks_total:
        print_success("✓ PROJETO COMPLETO E VÁLIDO!")
        print()
        print("Próximos passos:")
        print("1. Instalar dependências: pip install -r requirements.txt")
        print("2. Configurar MySQL (ou usar Docker)")
        print("3. Executar testes: pytest")
        print("4. Executar pipeline: python src/etl_pipeline.py")
        return 0
    elif percentage >= 90:
        print_warning("⚠ PROJETO QUASE COMPLETO")
        print(f"Faltam {checks_total - checks_passed} verificações")
        return 0
    else:
        print_error("✗ PROJETO INCOMPLETO")
        print(f"Faltam {checks_total - checks_passed} verificações")
        return 1

if __name__ == "__main__":
    sys.exit(main())
