#!/bin/bash
"""
Script auxiliar para execução de testes de integração
Uso: ./run_integration_tests.sh [opções]
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd, cwd=None):
    """Executa comando e retorna resultado"""
    print(f"Executando: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)

    if result.stdout:
        print(f"STDOUT: {result.stdout}")
    if result.stderr:
        print(f"STDERR: {result.stderr}")

    return result


def main():
    parser = argparse.ArgumentParser(description="Executa testes de integração")
    parser.add_argument("--test-type", choices=["unit", "integration", "all"],
                       default="integration", help="Tipo de teste")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Modo verboso")
    parser.add_argument("--coverage", action="store_true",
                       help="Executar com cobertura")
    parser.add_argument("--keep-output", action="store_true",
                       help="Manter arquivos de output após teste")

    args = parser.parse_args()

    project_root = Path(__file__).parent.parent

    print(f"🚀 Iniciando testes no diretório: {project_root}")

    try:
        # 1. Verificar dependências
        print("🔍 Verificando dependências...")
        result = run_command([sys.executable, "-m", "pip", "list"])
        if result.returncode != 0:
            print("❌ Erro ao verificar dependências")
            return 1

        # 2. Executar testes específicos
        if args.test_type in ["unit", "all"]:
            print("🧪 Executando testes unitários...")
            cmd = [sys.executable, "-m", "pytest", "tests/", "-v"]

            if args.coverage:
                cmd.extend(["--cov=src", "--cov-report=html"])

            if not args.verbose:
                cmd.append("-q")

            result = run_command(cmd, cwd=project_root)

            if result.returncode != 0:
                print("❌ Testes unitários falharam")
                return result.returncode

        if args.test_type in ["integration", "all"]:
            print("🔗 Executando testes de integração...")

            # Verificar se Docker está disponível
            result = run_command(["docker", "--version"])
            if result.returncode != 0:
                print("❌ Docker não está disponível. Pulando testes de integração.")
                return 0

            # Verificar se Docker Compose está disponível
            result = run_command(["docker-compose", "--version"])
            if result.returncode != 0:
                print("❌ Docker Compose não está disponível. Pulando testes de integração.")
                return 0

            # Executar teste de integração
            cmd = [sys.executable, "-m", "pytest", "tests/test_integration.py",
                  "-v", "-s"]

            if not args.keep_output:
                cmd.append("--tb=short")

            result = run_command(cmd, cwd=project_root)

            if result.returncode != 0:
                print("❌ Testes de integração falharam")
                return result.returncode

        print("✅ Todos os testes executados com sucesso!")
        return 0

    except KeyboardInterrupt:
        print("\n⚠️ Testes interrompidos pelo usuário")
        return 1
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
