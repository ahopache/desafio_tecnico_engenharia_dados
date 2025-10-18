#!/usr/bin/env python3
"""
Teste de Integração - SiCooperative ETL
Testa o pipeline completo usando Docker Compose
"""

import os
import time
import subprocess
import pytest
import pandas as pd
from pathlib import Path
import shutil
import docker
import requests


class TestSiCooperativeIntegration:
    """
    Teste de integração completo do pipeline ETL

    Cenário:
    1. Sobe ambiente Docker Compose (MySQL + Spark)
    2. Aguarda inicialização dos serviços
    3. Executa pipeline ETL
    4. Valida output CSV
    5. Limpa ambiente
    """

    def setup_method(self):
        """Configuração inicial do teste"""
        self.project_root = Path(__file__).parent.parent
        self.output_dir = self.project_root / "test_output"
        self.docker_compose_file = self.project_root / "docker" / "docker-compose.yml"

        # Configurações específicas para teste
        self.test_config = {
            "MYSQL_HOST": "localhost",
            "MYSQL_PORT": "3306",
            "MYSQL_DATABASE": "sicooperative_test",
            "MYSQL_USER": "test_user",
            "MYSQL_PASSWORD": "test_password",
            "OUTPUT_DIR": str(self.output_dir),
            "LOG_LEVEL": "INFO"
        }

        # Arquivo CSV esperado
        self.expected_csv = self.output_dir / "csv" / "movimento_flat.csv"

        # Limpar diretório de teste anterior
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)

    def teardown_method(self):
        """Limpeza após o teste"""
        # Parar e remover containers
        try:
            self._stop_docker_compose()
        except Exception as e:
            print(f"Aviso durante limpeza: {e}")

        # Limpar diretório de output
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)

    def _start_docker_compose(self):
        """Inicia ambiente Docker Compose"""
        print("🚀 Iniciando ambiente Docker Compose...")

        # Comando para iniciar em modo detached
        cmd = [
            "docker-compose",
            "-f", str(self.docker_compose_file),
            "up",
            "-d"
        ]

        result = subprocess.run(cmd, cwd=self.project_root, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Falha ao iniciar Docker Compose: {result.stderr}")

        print("✅ Ambiente Docker iniciado")

    def _stop_docker_compose(self):
        """Para ambiente Docker Compose"""
        print("🛑 Parando ambiente Docker Compose...")

        cmd = [
            "docker-compose",
            "-f", str(self.docker_compose_file),
            "down"
        ]

        result = subprocess.run(cmd, cwd=self.project_root, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Aviso ao parar Docker Compose: {result.stderr}")

        print("✅ Ambiente Docker parado")

    def _wait_for_mysql(self, max_attempts: int = 30, delay: int = 2):
        """Aguarda MySQL ficar disponível"""
        print("⏳ Aguardando MySQL ficar disponível...")

        for attempt in range(max_attempts):
            try:
                # Tentar conectar no MySQL
                import mysql.connector

                conn = mysql.connector.connect(
                    host=self.test_config["MYSQL_HOST"],
                    port=int(self.test_config["MYSQL_PORT"]),
                    user=self.test_config["MYSQL_USER"],
                    password=self.test_config["MYSQL_PASSWORD"],
                    database=self.test_config["MYSQL_DATABASE"],
                    connection_timeout=5
                )

                conn.close()
                print(f"✅ MySQL disponível após {attempt + 1} tentativas")
                return True

            except Exception as e:
                print(f"⏳ Tentativa {attempt + 1}/{max_attempts}: {e}")
                time.sleep(delay)

        raise Exception("MySQL não ficou disponível dentro do tempo limite")

    def _wait_for_spark(self, max_attempts: int = 30, delay: int = 5):
        """Aguarda Spark ficar disponível (opcional, para testes futuros)"""
        # Por enquanto, apenas aguarda um tempo fixo
        print(f"⏳ Aguardando Spark... ({delay * max_attempts}s)")
        time.sleep(delay * max_attempts)

    def _run_pipeline(self):
        """Executa o pipeline ETL"""
        print("🏃 Executando pipeline ETL...")

        # Comando para executar o pipeline
        cmd = [
            "python",
            str(self.project_root / "src" / "etl_pipeline.py"),
            "--output", self.test_config["OUTPUT_DIR"],
            "--log-level", self.test_config["LOG_LEVEL"]
        ]

        # Configurar variáveis de ambiente
        env = os.environ.copy()
        env.update(self.test_config)

        result = subprocess.run(
            cmd,
            cwd=self.project_root,
            env=env,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            raise Exception(f"Pipeline falhou com código {result.returncode}")

        print("✅ Pipeline executado com sucesso")
        return result.stdout, result.stderr

    def _validate_csv_output(self):
        """Valida arquivo CSV de saída"""
        print("🔍 Validando arquivo CSV de saída...")

        # Verificar se arquivo existe
        if not self.expected_csv.exists():
            raise Exception(f"Arquivo CSV não encontrado: {self.expected_csv}")

        # Carregar CSV
        try:
            df = pd.read_csv(self.expected_csv)
        except Exception as e:
            raise Exception(f"Erro ao ler CSV: {e}")

        # Validações básicas
        expected_columns = [
            "nome_associado", "sobrenome_associado", "idade_associado",
            "vlr_transacao_movimento", "des_transacao_movimento", "data_movimento",
            "numero_cartao", "nome_impresso_cartao", "data_emissao_cartao",
            "tipo_conta", "data_criacao_conta"
        ]

        # Verificar colunas
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            raise Exception(f"Colunas ausentes: {missing_columns}")

        # Verificar número mínimo de linhas (espera pelo menos alguns registros)
        min_rows = 10
        if len(df) < min_rows:
            raise Exception(f"Número insuficiente de registros: {len(df)} < {min_rows}")

        # Verificar tipos de dados básicos
        if df["vlr_transacao_movimento"].dtype not in ['float64', 'int64']:
            raise Exception("Tipo incorreto para vlr_transacao_movimento")

        if df["idade_associado"].dtype not in ['int64']:
            raise Exception("Tipo incorreto para idade_associado")

        print(f"✅ CSV válido: {len(df)} linhas, {len(df.columns)} colunas")
        return df

    def _get_csv_statistics(self, df: pd.DataFrame) -> dict:
        """Calcula estatísticas do CSV para validação adicional"""
        stats = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns},
            "null_counts": df.isnull().sum().to_dict(),
            "sample_values": {}
        }

        # Adicionar alguns valores de exemplo (sem dados sensíveis)
        for col in ["vlr_transacao_movimento", "idade_associado", "tipo_conta"]:
            if col in df.columns:
                stats["sample_values"][col] = df[col].dropna().head(3).tolist()

        return stats

    def test_full_pipeline_integration(self):
        """
        Teste completo de integração: Docker Compose -> Pipeline -> Validação
        """
        print("🧪 Iniciando teste de integração completo...")

        try:
            # 1. Iniciar ambiente Docker
            self._start_docker_compose()

            # 2. Aguardar serviços ficarem disponíveis
            self._wait_for_mysql()
            self._wait_for_spark()

            # 3. Executar pipeline
            stdout, stderr = self._run_pipeline()

            # 4. Validar output
            df = self._validate_csv_output()
            stats = self._get_csv_statistics(df)

            # 5. Validações adicionais
            print("📊 Estatísticas do CSV:")
            for key, value in stats.items():
                print(f"   {key}: {value}")

            # Validação específica: verificar se há dados de diferentes tipos de conta
            if "tipo_conta" in df.columns:
                tipos_conta = df["tipo_conta"].unique()
                if len(tipos_conta) < 2:
                    raise Exception(f"Pouca variedade de tipos de conta: {tipos_conta}")

            # Validação específica: verificar se valores de transação são positivos
            if "vlr_transacao_movimento" in df.columns:
                valores_negativos = (df["vlr_transacao_movimento"] < 0).sum()
                if valores_negativos > 0:
                    print(f"⚠️ Aviso: {valores_negativos} transações negativas encontradas")

            # 6. Log de sucesso
            print("🎉 Teste de integração concluído com sucesso!")
            print(f"📄 Arquivo gerado: {self.expected_csv}")
            print(f"📏 Shape: {df.shape}")
            print(f"📋 Colunas: {', '.join(df.columns)}")

            # Assertivas finais
            assert self.expected_csv.exists(), "Arquivo CSV deve existir"
            assert len(df) >= 10, "Deve haver pelo menos 10 registros"
            assert len(df.columns) == 11, "Deve haver 11 colunas"
            assert "vlr_transacao_movimento" in df.columns, "Coluna vlr_transacao_movimento obrigatória"

        except Exception as e:
            print(f"❌ Teste falhou: {e}")
            # Log adicional para debug
            if os.path.exists(self.expected_csv):
                print(f"Arquivo existe mas pode estar corrompido: {self.expected_csv}")
            raise

    def test_docker_compose_health(self):
        """Teste básico de saúde do Docker Compose"""
        print("🏥 Testando saúde do ambiente Docker...")

        try:
            # Verificar se containers estão rodando
            client = docker.from_env()

            # Verificar container MySQL
            mysql_container = None
            for container in client.containers.list():
                if "mysql" in container.name.lower():
                    mysql_container = container
                    break

            if not mysql_container:
                raise Exception("Container MySQL não encontrado")

            # Verificar se container está rodando
            if mysql_container.status != "running":
                raise Exception(f"Container MySQL não está rodando: {mysql_container.status}")

            # Verificar se MySQL está respondendo
            self._wait_for_mysql(max_attempts=10, delay=1)

            print("✅ Ambiente Docker saudável")

        except Exception as e:
            print(f"❌ Ambiente Docker com problemas: {e}")
            raise

    @pytest.mark.parametrize("config_scenario", [
        "basic_config",
        "with_observability",
        "with_quality_checks"
    ])
    def test_pipeline_with_different_configs(self, config_scenario):
        """Testa pipeline com diferentes configurações"""
        print(f"🔧 Testando configuração: {config_scenario}")

        # Ajustar configuração baseada no cenário
        if config_scenario == "with_observability":
            self.test_config["OBSERVABILITY_ENABLED"] = "true"
            self.test_config["PROMETHEUS_GATEWAY_URL"] = ""
        elif config_scenario == "with_quality_checks":
            self.test_config["DATA_QUALITY_CHECKS_ENABLED"] = "true"

        try:
            # Executar teste básico
            self.test_full_pipeline_integration()

            print(f"✅ Configuração {config_scenario} testada com sucesso")

        except Exception as e:
            print(f"❌ Configuração {config_scenario} falhou: {e}")
            raise


# Função para executar teste manualmente (fora do pytest)
def run_integration_test():
    """Executa teste de integração manualmente"""
    print("🚀 Executando teste de integração manualmente...")

    test_instance = TestSiCooperativeIntegration()

    try:
        test_instance.setup_method()
        test_instance.test_full_pipeline_integration()
        print("🎉 Teste manual concluído com sucesso!")

    except Exception as e:
        print(f"❌ Teste manual falhou: {e}")
        raise

    finally:
        test_instance.teardown_method()


if __name__ == "__main__":
    run_integration_test()
