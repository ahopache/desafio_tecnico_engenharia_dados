"""
Pipeline ETL Principal
Extrai dados do MySQL, transforma e gera CSV flat conforme especificação
"""
import argparse
import datetime
import glob
import json
import locale
import os
import sys
import tempfile
import shutil
import time

from typing import Tuple
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import DecimalType, TimestampType

from config import Config
from utils import (
    setup_logger,
    validate_mysql_connection,
    validate_dataframe,
    log_dataframe_info,
    format_duration,
    create_output_directory,
    print_banner,
    print_statistics,
    ETLException,
    mask_credit_card,
    hash_sensitive_data,
    validate_pii_masking,
    validate_no_full_pan_in_output
)
from data_quality import DataQualityChecker, QualityCheckStatus
from observability import observability_manager


class SiCooperativeETL:
    """
    Pipeline ETL para Data Lake POC da SiCooperative
    
    Implementa arquitetura Medalhão simplificada:
    - Bronze: Dados brutos do MySQL
    - Silver: Transformações e JOINs
    - Gold: CSV flat final
    """
    
    def __init__(self, output_dir: str = None, run_mode: str = "full"):
        """
        Inicializa o pipeline ETL
        
        Args:
            output_dir: Diretório de saída (opcional, usa Config se não fornecido)
        """
        self.logger = setup_logger(__name__)
        self.spark: SparkSession = None
        self.output_dir = output_dir or Config.OUTPUT_DIR
        
        # Estatísticas do pipeline
        self.stats = {
            "inicio": None,
            "fim": None,
            "duracao": None,
            "registros_associado": 0,
            "registros_conta": 0,
            "registros_cartao": 0,
            "registros_movimento": 0,
            "registros_final": 0
        }
    
    def log_structured(self, level: str, message: str, **kwargs) -> None:
        """
        Gera log estruturado em formato JSON

        Args:
            level: Nível do log (INFO, WARNING, ERROR, DEBUG)
            message: Mensagem principal do log
            **kwargs: Campos adicionais para incluir no log estruturado
        """

        log_entry = {
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "level": level,
            "logger": "etl_pipeline",
            "message": message,
            "run_id": f"run_{int(time.time())}",
            **kwargs
        }

        # Log no formato estruturado (JSON)
        self.logger.info(f"[STRUCTURED] {json.dumps(log_entry, default=str)}")

        # Também mantém o log tradicional para compatibilidade
        if level == "ERROR":
            self.logger.error(message)
        elif level == "WARNING":
            self.logger.warning(message)
        else:
            self.logger.info(message)
    
    def create_spark_session(self) -> SparkSession:
        """
        Cria e configura a sessão Spark
        
        Returns:
            SparkSession: Sessão Spark configurada
        """
        # Configurar HADOOP_HOME para Windows se necessário
        if sys.platform.startswith('win') and not os.environ.get('HADOOP_HOME'):
            hadoop_home = r"C:\hadoop"
            if os.path.exists(os.path.join(hadoop_home, "bin", "winutils.exe")):
                os.environ['HADOOP_HOME'] = hadoop_home
                print(f"✅ HADOOP_HOME configurado automaticamente: {hadoop_home}")
            else:
                print("⚠️ winutils.exe não encontrado. Baixe de: https://github.com/steveloughran/winutils")
                print("   E coloque em C:\\hadoop\\bin\\winutils.exe")

        self.logger.info("Criando sessão Spark...")
        
        try:
            builder = SparkSession.builder
            
            # Aplicar configurações
            for key, value in Config.get_spark_config().items():
                builder = builder.config(key, value)
            
            # Adicionar suporte ao MySQL JDBC
            # Nota: O JAR do MySQL deve estar no classpath
            builder = builder.config(
                "spark.jars.packages",
                "mysql:mysql-connector-java:8.0.33"
            )
            
            # Configuração específica para Windows
            if sys.platform.startswith('win'):
                builder = builder.config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
            
            self.spark = builder.getOrCreate()
            
            # Configurar nível de log do Spark
            self.spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info(f"✓ Sessão Spark criada: {Config.SPARK_APP_NAME}")
            self.logger.info(f"  Spark Version: {self.spark.version}")
            self.logger.info(f"  Master: {Config.SPARK_MASTER}")
            
            return self.spark
            
        except Exception as e:
            self.logger.error(f"✗ Erro ao criar sessão Spark: {str(e)}")
            raise ETLException(f"Falha ao criar sessão Spark: {str(e)}")
    
    def extract_table(self, table_name: str) -> DataFrame:
        """
        Extrai uma tabela do MySQL (Bronze Layer)

        Args:
            table_name: Nome da tabela no MySQL

        Returns:
            DataFrame: Dados da tabela
        """
        self.logger.info(f"Extraindo tabela: {table_name}")

        try:
            # Configurar opções básicas de JDBC
            jdbc_options = {
                "url": Config.get_mysql_jdbc_url(),
                "dbtable": table_name,
                "user": Config.MYSQL_USER,
                "password": Config.MYSQL_PASSWORD,
                "driver": "com.mysql.cj.jdbc.Driver"
            }

            # Adicionar opções de particionamento se aplicável
            partition_options = Config.get_jdbc_partition_options(table_name)
            if partition_options:
                jdbc_options.update(partition_options)
                self.logger.info(f"✓ Usando particionamento JDBC: {partition_options}")

                # Log dos parâmetros de particionamento usados
                self.logger.info(
                    "Parâmetros de particionamento JDBC:\n"
                    f"  - partitionColumn: {partition_options.get('partitionColumn', 'N/A')}\n"
                    f"  - lowerBound: {partition_options.get('lowerBound', 'N/A')}\n"
                    f"  - upperBound: {partition_options.get('upperBound', 'N/A')}\n"
                    f"  - numPartitions: {partition_options.get('numPartitions', 'N/A')}"
                )
            else:
                self.logger.info("✓ Usando leitura JDBC simples (sem particionamento)")

            # Construir o DataFrame com as opções apropriadas
            df = self.spark.read \
                .format("jdbc") \
                .options(**jdbc_options) \
                .load()

            count = df.count()
            self.logger.info(f"✓ Tabela '{table_name}' extraída: {count} registros")

            return df

        except Exception as e:
            self.logger.error(f"✗ Erro ao extrair tabela '{table_name}': {str(e)}")
            raise ETLException(f"Falha na extração da tabela '{table_name}': {str(e)}")
    
    def extract_all_tables(self) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Extrai todas as tabelas necessárias (Bronze Layer)
        
        Returns:
            Tuple: (df_associado, df_conta, df_cartao, df_movimento)
        """
        print_banner("ETAPA 1: EXTRAÇÃO (Bronze Layer)")

        # Extrair tabelas
        df_associado = self.extract_table(Config.TABLE_ASSOCIADO)
        df_conta = self.extract_table(Config.TABLE_CONTA)
        df_cartao = self.extract_table(Config.TABLE_CARTAO)
        df_movimento = self.extract_table(Config.TABLE_MOVIMENTO)

        # Verificações de qualidade de dados
        print_banner("ETAPA 1.1: VERIFICAÇÕES DE QUALIDADE DE DADOS")

        quality_timer_id = observability_manager.get_collector().start_timer("quality_checks")
        quality_checker = DataQualityChecker()

        # Verificar qualidade dos dados extraídos
        quality_checker.run_quality_checks(df_associado, "associado")
        quality_checker.run_quality_checks(df_conta, "conta")
        quality_checker.run_quality_checks(df_cartao, "cartao")
        quality_checker.run_quality_checks(df_movimento, "movimento")

        quality_result = observability_manager.get_collector().stop_timer(quality_timer_id)

        # Registrar métricas de qualidade
        if quality_result and observability_manager.is_enabled():
            total_checks = len(quality_checker.results)
            failed_checks = len(quality_checker.get_failed_checks())
            warning_checks = len(quality_checker.get_warning_checks())

            observability_manager.get_collector().record_gauge(
                "etl_quality_checks_total", total_checks, {"stage": "extract"}
            )
            observability_manager.get_collector().record_gauge(
                "etl_quality_checks_failed", failed_checks, {"stage": "extract"}
            )
            observability_manager.get_collector().record_gauge(
                "etl_quality_checks_warnings", warning_checks, {"stage": "extract"}
            )

        # Verificar se pipeline deve ser rejeitado
        if quality_checker.should_reject_pipeline():
            error_msg = "Pipeline rejeitado devido a falhas críticas de qualidade de dados"
            self.logger.error(f"❌ {error_msg}")

            # Gerar relatório de qualidade
            quality_report = quality_checker.generate_quality_report()
            self.logger.error("Relatório de qualidade:\n" + quality_report)

            raise ETLException(error_msg)

        # Log avisos de qualidade
        warnings = quality_checker.get_warning_checks()
        if warnings:
            self.logger.warning(f"⚠️ Avisos de qualidade detectados: {len(warnings)}")
            for warning in warnings:
                self.logger.warning(f"  - {warning.message}")

        # Atualizar estatísticas de qualidade
        quality_stats = {
            'total': len(quality_checker.results),
            'passed': len([r for r in quality_checker.results if r.status.value == 'PASS']),
            'warnings': len(warnings),
            'failed': len(quality_checker.get_failed_checks())
        }
        self.stats['quality_checks'] = quality_stats

        # Validar DataFrames
        validate_dataframe(df_associado, "associado", min_rows=1)
        validate_dataframe(df_conta, "conta", min_rows=1)
        validate_dataframe(df_cartao, "cartao", min_rows=1)
        validate_dataframe(df_movimento, "movimento", min_rows=1)

        # Atualizar estatísticas
        self.stats["registros_associado"] = df_associado.count()
        self.stats["registros_conta"] = df_conta.count()
        self.stats["registros_cartao"] = df_cartao.count()
        self.stats["registros_movimento"] = df_movimento.count()

    def extract_all_tables_incremental(self) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Extrai todas as tabelas necessárias usando processamento incremental (Bronze Layer)

        Returns:
            Tuple: (df_associado, df_conta, df_cartao, df_movimento)
        """
        print_banner("ETAPA 1: EXTRAÇÃO INCREMENTAL (Bronze Layer)")

        # Extrair tabelas incrementalmente
        df_associado = self._extract_table_incremental("associado")
        df_conta = self._extract_table_incremental("conta")
        df_cartao = self._extract_table_incremental("cartao")
        df_movimento = self._extract_table_incremental("movimento")

        # Verificações de qualidade de dados
        print_banner("ETAPA 1.1: VERIFICAÇÕES DE QUALIDADE DE DADOS")

        quality_timer_id = observability_manager.get_collector().start_timer("quality_checks")
        quality_checker = DataQualityChecker()

        # Verificar qualidade dos dados extraídos
        quality_checker.run_quality_checks(df_associado, "associado")
        quality_checker.run_quality_checks(df_conta, "conta")
        quality_checker.run_quality_checks(df_cartao, "cartao")
        quality_checker.run_quality_checks(df_movimento, "movimento")

        quality_result = observability_manager.get_collector().stop_timer(quality_timer_id)

        # Registrar métricas de qualidade
        if quality_result and observability_manager.is_enabled():
            total_checks = len(quality_checker.results)
            failed_checks = len(quality_checker.get_failed_checks())
            warning_checks = len(quality_checker.get_warning_checks())

            observability_manager.get_collector().record_gauge(
                "etl_quality_checks_total", total_checks, {"stage": "extract"}
            )
            observability_manager.get_collector().record_gauge(
                "etl_quality_checks_failed", failed_checks, {"stage": "extract"}
            )
            observability_manager.get_collector().record_gauge(
                "etl_quality_checks_warnings", warning_checks, {"stage": "extract"}
            )

        # Verificar se pipeline deve ser rejeitado
        if quality_checker.should_reject_pipeline():
            error_msg = "Pipeline rejeitado devido a falhas críticas de qualidade de dados"
            self.logger.error(f"❌ {error_msg}")

            # Gerar relatório de qualidade
            quality_report = quality_checker.generate_quality_report()
            self.logger.error("Relatório de qualidade:\n" + quality_report)

            raise ETLException(error_msg)

        # Log avisos de qualidade
        warnings = quality_checker.get_warning_checks()
        if warnings:
            self.logger.warning(f"⚠️ Avisos de qualidade detectados: {len(warnings)}")
            for warning in warnings:
                self.logger.warning(f"  - {warning.message}")

        # Atualizar estatísticas de qualidade
        quality_stats = {
            'total': len(quality_checker.results),
            'passed': len([r for r in quality_checker.results if r.status.value == 'PASS']),
            'warnings': len(warnings),
            'failed': len(quality_checker.get_failed_checks())
        }
        self.stats['quality_checks'] = quality_stats

        # Validar DataFrames
        validate_dataframe(df_associado, "associado", min_rows=0)  # Pode ser 0 no modo incremental
        validate_dataframe(df_conta, "conta", min_rows=0)
        validate_dataframe(df_cartao, "cartao", min_rows=0)
        validate_dataframe(df_movimento, "movimento", min_rows=0)

        # Atualizar estatísticas
        self.stats["registros_associado"] = df_associado.count()
        self.stats["registros_conta"] = df_conta.count()
        self.stats["registros_cartao"] = df_cartao.count()
        self.stats["registros_movimento"] = df_movimento.count()

        return df_associado, df_conta, df_cartao, df_movimento

    def _extract_table_incremental(self, table_name: str) -> DataFrame:
        """
        Extrai dados de uma tabela usando processamento incremental baseado em watermark

        Args:
            table_name: Nome da tabela a ser extraída

        Returns:
            DataFrame: Dados extraídos incrementalmente
        """
        try:
            # Obter informações do watermark
            watermark_info = self._get_watermark_info(table_name)
            last_processed_ts = watermark_info.get("last_processed_ts")

            # Construir query incremental usando abordagem simplificada
            incremental_query = self._get_incremental_query_simplified(table_name, last_processed_ts)

            self.logger.info(f"📊 Extraindo tabela '{table_name}' incrementalmente desde: {last_processed_ts}")
            self.logger.info(f"📋 Query: {incremental_query}")

            # Usar o método existente extract_table com query personalizada
            jdbc_options = {
                "url": Config.get_mysql_jdbc_url(),
                "dbtable": incremental_query,
                "user": Config.MYSQL_USER,
                "password": Config.MYSQL_PASSWORD,
                "driver": "com.mysql.cj.jdbc.Driver"
            }

            # Adicionar opções de particionamento se aplicável
            partition_options = Config.get_jdbc_partition_options(table_name, self.spark)
            if partition_options:
                jdbc_options.update(partition_options)
                self.logger.info(f"✓ Usando particionamento JDBC otimizado para tabela '{table_name}':")
                self.logger.info(f"  - Coluna: {partition_options['partitionColumn']}")
                self.logger.info(f"  - Range: {partition_options['lowerBound']} a {partition_options['upperBound']}")
                self.logger.info(f"  - Partições: {partition_options['numPartitions']}")
            else:
                self.logger.info(f"⚠️ Sem particionamento JDBC para tabela '{table_name}' (tabela pequena ou sem dados)")

            # Construir o DataFrame com as opções apropriadas
            df = self.spark.read \
                .format("jdbc") \
                .options(**jdbc_options) \
                .load()

            count = df.count()
            self.logger.info(f"✓ Tabela '{table_name}' extraída incrementalmente: {count} registros")

            return df

        except Exception as e:
            self.logger.error(f"✗ Erro ao extrair tabela '{table_name}' incrementalmente: {str(e)}")
            raise ETLException(f"Falha na extração incremental da tabela '{table_name}': {str(e)}")

    def _get_incremental_query_simplified(self, table_name: str, last_processed_ts: str) -> str:
        """
        Constrói query SQL simplificada para extração incremental

        Args:
            table_name: Nome da tabela
            last_processed_ts: Último timestamp processado

        Returns:
            str: Query SQL para extração incremental
        """
        table_config = {
            "associado": {"timestamp_col": "data_criacao"},
            "conta": {"timestamp_col": "data_criacao"},
            "cartao": {"timestamp_col": "data_criacao"},
            "movimento": {"timestamp_col": "data_movimento"}
        }

        if table_name not in table_config:
            raise ETLException(f"Tabela '{table_name}' não configurada para processamento incremental")

        timestamp_col = table_config[table_name]["timestamp_col"]

        if last_processed_ts is None:
            # Primeira execução - buscar todos os dados
            query = table_name
        else:
            # Extração incremental com lookback window
            lookback_days = Config.LOOKBACK_DAYS
            query = f"{table_name} WHERE {timestamp_col} >= DATE_SUB('{last_processed_ts}', INTERVAL {lookback_days} DAY)"

        return query

    def _get_watermark_info(self, table_name: str) -> dict:
        """
        Obtém informações do watermark para uma tabela específica

        Args:
            table_name: Nome da tabela

        Returns:
            dict: Informações do watermark (last_processed_ts, last_processed_id, etc.)
        """
        try:
            # Usar query direta simples para verificar tabela
            schema_name = Config.MYSQL_DATABASE
            table_name_check = Config.WATERMARK_TABLE

            check_query = f"(SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name_check}') as query_count"

            self.logger.info(f"🔍 Verificando existência da tabela de watermark com query: {check_query}")

            result = self.spark.read \
                .format("jdbc") \
                .option("url", Config.get_mysql_jdbc_url()) \
                .option("dbtable", check_query) \
                .option("user", Config.MYSQL_USER) \
                .option("password", Config.MYSQL_PASSWORD) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()

            if result.first()["count"] == 0:
                # Tabela não existe, criar primeira entrada
                self.logger.info(f"🏗️ Criando primeira entrada de watermark para tabela '{table_name}'")
                return {
                    "table_name": table_name,
                    "last_processed_ts": None,
                    "last_processed_id": None,
                    "records_processed": 0,
                    "execution_status": "pending"
                }

            # Buscar informações do watermark
            watermark_query = f"(SELECT table_name, last_processed_ts, last_processed_id, records_processed, execution_status, started_at, completed_at FROM {table_name_check} WHERE table_name = '{table_name}') as query_watermark"

            self.logger.info(f"🔍 Buscando watermark com query: {watermark_query}")

            watermark_df = self.spark.read \
                .format("jdbc") \
                .option("url", Config.get_mysql_jdbc_url()) \
                .option("dbtable", watermark_query) \
                .option("user", Config.MYSQL_USER) \
                .option("password", Config.MYSQL_PASSWORD) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()

            if watermark_df.count() == 0:
                # Não há entrada para esta tabela, criar
                return {
                    "table_name": table_name,
                    "last_processed_ts": None,
                    "last_processed_id": None,
                    "records_processed": 0,
                    "execution_status": "pending"
                }
            else:
                # Retornar informações existentes
                row = watermark_df.first()
                return {
                    "table_name": row["table_name"],
                    "last_processed_ts": row["last_processed_ts"],
                    "last_processed_id": row["last_processed_id"],
                    "records_processed": row["records_processed"],
                    "execution_status": row["execution_status"],
                    "started_at": row["started_at"],
                    "completed_at": row["completed_at"]
                }

        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao obter informações do watermark: {str(e)}")
            # Em caso de erro, assumir primeira execução
            return {
                "table_name": table_name,
                "last_processed_ts": None,
                "last_processed_id": None,
                "records_processed": 0,
                "execution_status": "pending"
            }

    def _update_watermark(self, table_name: str, records_processed: int, last_timestamp: str) -> None:
        """
        Atualiza informações do watermark para uma tabela

        Args:
            table_name: Nome da tabela
            records_processed: Número de registros processados
            last_timestamp: Último timestamp processado
        """
        try:
            # Verificar se a tabela etl_metadata existe
            schema_name = Config.MYSQL_DATABASE
            table_name_check = Config.WATERMARK_TABLE

            check_query = f"(SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name_check}') as query_count"

            self.logger.info(f"🔍 Verificando tabela de watermark com query: {check_query}")

            result = self.spark.read \
                .format("jdbc") \
                .option("url", Config.get_mysql_jdbc_url()) \
                .option("dbtable", check_query) \
                .option("user", Config.MYSQL_USER) \
                .option("password", Config.MYSQL_PASSWORD) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()

            if result.first()["count"] == 0:
                self.logger.warning(f"⚠️ Tabela de watermark '{table_name_check}' não existe")
                return

            # Atualizar ou inserir informações do watermark
            update_query = f"INSERT INTO {table_name_check} (table_name, last_processed_ts, records_processed, execution_status, completed_at) VALUES ('{table_name}', '{last_timestamp}', {records_processed}, 'completed', NOW()) ON DUPLICATE KEY UPDATE last_processed_ts = VALUES(last_processed_ts), records_processed = VALUES(records_processed), execution_status = VALUES(execution_status), completed_at = VALUES(completed_at)"

            self.logger.info(f"🔄 Atualizando watermark com query: {update_query}")

            # Executar atualização usando JDBC
            self.spark.read \
                .format("jdbc") \
                .option("url", Config.get_mysql_jdbc_url()) \
                .option("dbtable", update_query) \
                .option("user", Config.MYSQL_USER) \
                .option("password", Config.MYSQL_PASSWORD) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()

            self.logger.info(f"✓ Watermark atualizado para tabela '{table_name}': {records_processed} registros, último timestamp: {last_timestamp}")

        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao atualizar status do watermark: {str(e)}")

    def transform_and_join(self, df_associado, df_conta, df_cartao, df_movimento):
        print_banner("ETAPA 2: TRANSFORMAÇÃO (Silver Layer)")
        
        self.logger.info("Iniciando JOINs entre tabelas...")
        
        try:
            # JOIN 1: movimento + cartao
            self.logger.info("JOIN 1/3: movimento ← cartao")

            df_joined = df_movimento.join(
                df_cartao,
                df_movimento.id_cartao == df_cartao.id,
                "inner"
            )
            
            # JOIN 2: + conta
            self.logger.info("JOIN 2/3: + conta")
            df_joined = df_joined.join(
                df_conta,
                df_cartao.id_conta == df_conta.id,
                "inner"
            )
            
            # JOIN 3: + associado
            self.logger.info("JOIN 3/3: + associado")
            df_joined = df_joined.join(
                df_associado,
                df_cartao.id_associado == df_associado.id,
                "inner"
            )
            
            self.logger.info(f"✓ JOINs concluídos: {df_joined.count()} registros")
            
            # Registrar métricas intermediárias de transformação
            if observability_manager.is_enabled():
                # Métricas após JOINs
                observability_manager.get_collector().record_gauge(
                    "etl_records_after_joins", df_joined.count(), {"stage": "transform", "step": "joins"}
                )
                observability_manager.get_collector().record_gauge(
                    "etl_join_success_rate", 100.0, {"stage": "transform"}
                )
            
            # Selecionar e renomear colunas conforme especificação
            self.logger.info("Selecionando e renomeando colunas...")
            
            # Converter data_movimento para timestamp com timezone UTC (convertendo do timezone America/Sao_Paulo)
            # Documentação: Todas as datas são convertidas para UTC considerando que o banco está em America/Sao_Paulo
            df_joined = df_joined.withColumn(
                "data_movimento_utc",
                F.to_utc_timestamp("data_movimento", Config.SOURCE_TIMEZONE)
            )
            
            # Aplicar formatação de valores decimais
            df_joined = df_joined.withColumn(
                "vlr_transacao_decimal",
                F.col("vlr_transacao").cast(DecimalType(20, 2))
            )
            
            df_transformed = df_joined.select(
                # Dados do associado (com email mascarado e hash)
                df_associado.nome.alias("nome_associado"),
                df_associado.sobrenome.alias("sobrenome_associado"),
                df_associado.idade.cast("integer").alias("idade_associado"),
                F.when(
                    F.col("email").isNotNull(),
                    F.concat(
                        F.substring(F.col("email"), 1, 3),
                        F.lit("****@"),
                        F.split(F.col("email"), "@").getItem(1)
                    )
                ).otherwise("").alias("email_masked"),
                hash_sensitive_data(df_associado.email).alias("email_hash"),
                
                # Dados do movimento
                df_movimento.id.alias("id_movimento"),
                F.col("vlr_transacao_decimal").alias("vlr_transacao_movimento"),
                df_movimento.des_transacao.alias("des_transacao_movimento"),
                F.date_format("data_movimento_utc", Config.DATETIME_FORMAT).alias("data_movimento"),
                
                # Dados do cartão (mascarado e hash)
                mask_credit_card(df_cartao.num_cartao).alias("numero_cartao_masked"),
                hash_sensitive_data(df_cartao.num_cartao).alias("numero_cartao_hash"),
                
                df_cartao.nom_impresso.alias("nome_impresso_cartao"),
                df_cartao.data_criacao.alias("dt_emissao_cartao"),
                F.date_format(
                    F.to_utc_timestamp("dt_emissao_cartao", Config.SOURCE_TIMEZONE), 
                    Config.DATETIME_FORMAT
                ).alias("data_emissao_cartao"),
                
                # Dados da conta
                df_conta.tipo.alias("tipo_conta"),
                df_conta.data_criacao.alias("dt_criacao_conta"),
                F.date_format(
                    F.to_utc_timestamp("dt_criacao_conta", Config.SOURCE_TIMEZONE),
                    Config.DATETIME_FORMAT
                ).alias("data_criacao_conta")
            )
            
            # Validar colunas finais
            validate_dataframe(
                df_transformed,
                "transformed",
                min_rows=1,
                required_columns=Config.OUTPUT_COLUMNS
            )
            
            # Validar que todas as datas estão em formato ISO8601 UTC
            self._validate_datetime_formats(df_transformed)
            
            # Validar mascaramento de dados sensíveis (PII)
            validate_pii_masking(df_transformed, self.logger)
            
            # Verificação adicional de segurança: garantir que não há PANs completos no output
            validate_no_full_pan_in_output(df_transformed, self.logger)
            
            self.logger.info("✓ Transformações concluídas")
            self.logger.info("✓ Todas as datas convertidas para UTC com timezone America/Sao_Paulo")
            
            # Registrar métricas finais de transformação
            if observability_manager.is_enabled():
                # Métricas finais de transformação
                observability_manager.get_collector().record_gauge(
                    "etl_records_after_transform", df_transformed.count(), {"stage": "transform"}
                )
                observability_manager.get_collector().record_gauge(
                    "etl_columns_final_count", len(df_transformed.columns), {"stage": "transform"}
                )
                
                # Verificar se houve perda de dados na transformação
                input_records = df_movimento.count() if 'df_movimento' in locals() else 0
                output_records = df_transformed.count()
                if input_records > 0:
                    transform_efficiency = (output_records / input_records) * 100
                    observability_manager.get_collector().record_gauge(
                        "etl_transform_efficiency_percent", transform_efficiency, {"stage": "transform"}
                    )
            
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"✗ Erro nas transformações: {str(e)}")
            raise ETLException(f"Falha nas transformações: {str(e)}")
    
    def _cleanup_temp_directory(self, pattern: str) -> None:
        """
        Remove arquivos temporários baseado no padrão

        Args:
            pattern: Padrão de arquivo a ser removido
        """
        try:
            temp_files = glob.glob(pattern)
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    if os.path.isdir(temp_file):
                        shutil.rmtree(temp_file)
                    else:
                        os.remove(temp_file)
                    self.logger.info(f"Arquivo temporário removido: {temp_file}")
        except Exception as e:
            self.logger.warning(f"Erro ao remover arquivos temporários {pattern}: {e}")

    def _create_temp_output_directory(self, final_path: str) -> str:
        """
        Cria diretório temporário baseado no caminho final

        Args:
            final_path: Caminho do diretório final

        Returns:
            str: Caminho do diretório temporário criado
        """
        try:
            # Criar diretório temporário único
            temp_dir = tempfile.mkdtemp(suffix=Config.TEMP_DIR_SUFFIX, dir=os.path.dirname(final_path))

            self.logger.info(f"📁 Diretório temporário criado: {temp_dir}")
            return temp_dir

        except Exception as e:
            self.logger.error(f"✗ Erro ao criar diretório temporário: {str(e)}")
            raise ETLException(f"Falha ao criar diretório temporário: {str(e)}")

    def _atomic_write_with_cleanup(self, df: DataFrame, temp_path: str, final_path: str, format_type: str) -> None:
        """
        Escreve dados atomicamente com cleanup automático

        Args:
            df: DataFrame a ser salvo
            temp_path: Caminho temporário para escrita inicial
            final_path: Caminho final para atomic move
            format_type: Tipo de formato ('csv' ou 'parquet')
        """
        try:
            # Escrever para diretório temporário
            if format_type == "csv":
                df.write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .option("encoding", "UTF-8") \
                    .csv(temp_path)
            elif format_type == "parquet":
                df.write \
                    .mode("overwrite") \
                    .parquet(temp_path)

            # Atomic move: renomear diretório temporário para final
            if os.path.exists(final_path):
                shutil.rmtree(final_path)

            os.rename(temp_path, final_path)

            self.logger.info(f"✅ Arquivo {format_type} escrito atomicamente: {final_path}")

        except Exception as e:
            # Cleanup em caso de erro
            if os.path.exists(temp_path):
                shutil.rmtree(temp_path)
            self.logger.error(f"✗ Erro na escrita atômica {format_type}: {str(e)}")
            raise ETLException(f"Falha na escrita atômica {format_type}: {str(e)}")

    def load_to_csv_atomic(self, df: DataFrame) -> dict:
        """
        Carrega dados para CSV usando atomic write pattern com processamento incremental
        
        Args:
            df: DataFrame a ser salvo
            
        Returns:
            dict: Caminhos dos arquivos gerados
        """
        output_paths = {}
        total_registros = df.count()
        
        try:
            # Preparar dados para escrita
            df_ordered = df.orderBy("data_movimento", "id_movimento")
            df_with_precision = df_ordered.withColumn(
                "vlr_transacao_movimento_final",
                F.col("vlr_transacao_movimento").cast(DecimalType(10, 2))
            )
            
            # Validar que o cast foi aplicado corretamente
            self._validate_final_decimal_precision(df_with_precision)
            
            df_coalesced = df_with_precision.coalesce(1)
            
            # Criar caminhos temporários e finais
            csv_final_path = f"{self.output_dir}/csv"
            csv_temp_path = self._create_temp_output_directory(csv_final_path)
            
            # Escrever para temporário
            self._atomic_write_with_cleanup(df_coalesced, csv_temp_path, csv_final_path, "csv")
            
            output_paths["csv"] = csv_final_path
            
            # Gerar Parquet se solicitado
            if "parquet" in Config.OUTPUT_FORMAT:
                parquet_final_path = f"{self.output_dir}/parquet"
                parquet_temp_path = self._create_temp_output_directory(parquet_final_path)
                
                # Preparar dados para Parquet
                df_with_date = df_with_precision.withColumn(
                    "data_movimento_date",
                    F.date_format("data_movimento", "yyyy-MM-dd")
                )
                
                # Escrever para temporário
                self._atomic_write_with_cleanup(df_with_date, parquet_temp_path, parquet_final_path, "parquet")
                
                output_paths["parquet"] = parquet_final_path
            
            # Atualizar watermarks após sucesso
            if total_registros > 0:
                # Encontrar último timestamp para atualizar watermark
                last_timestamp_df = df.agg(F.max("data_movimento")).first()
                last_timestamp = last_timestamp_df[0] if last_timestamp_df[0] else None
                
                self._update_watermark("movimento", total_registros, last_timestamp)
            
            # Registrar métricas de sucesso no carregamento
            if observability_manager.is_enabled():
                # Métricas de sucesso
                observability_manager.get_collector().record_gauge(
                    "etl_records_loaded", total_registros, {"stage": "load", "format": "csv"}
                )
                
                # Métricas de arquivo
                csv_size_mb = 0.0
                if os.path.exists(f"{self.output_dir}/csv/movimento_flat.csv"):
                    csv_size_mb = os.path.getsize(f"{self.output_dir}/csv/movimento_flat.csv") / (1024 * 1024)
                
                observability_manager.get_collector().record_gauge(
                    "etl_output_file_size_mb", csv_size_mb, {"stage": "load", "format": "csv"}
                )
                
                # Métricas de throughput
                if load_result and load_result.duration_seconds > 0:
                    throughput = total_registros / load_result.duration_seconds if total_registros > 0 else 0
                    observability_manager.get_collector().record_gauge(
                        "etl_load_throughput_records_per_second", throughput, {"stage": "load"}
                    )
            
            self.logger.info(f"  Total de registros processados: {total_registros}")
            return output_paths
            
        except Exception as e:
            self.logger.error(f"✗ Erro no load_to_csv_atomic: {str(e)}")
            
            # Registrar métricas de erro
            if observability_manager.is_enabled():
                observability_manager.get_collector().record_error("load_failure")
                observability_manager.get_collector().record_gauge(
                    "etl_load_error_count", 1.0, {"stage": "load", "error_type": "atomic_write"}
                )
            
            # Limpar arquivos temporários em caso de erro geral
            if Config.CLEANUP_TEMP_FILES:
                temp_patterns = [f"{self.output_dir}/csv{Config.TEMP_DIR_SUFFIX}", 
                               f"{self.output_dir}/parquet{Config.TEMP_DIR_SUFFIX}"]
                for pattern in temp_patterns:
                    self._cleanup_temp_directory(pattern)
            
            raise ETLException(f"Falha no atomic write: {str(e)}")
    
    def load_to_csv(self, df: DataFrame) -> dict:
        """
        Método legado - redireciona para load_to_csv_atomic
        """
        return self.load_to_csv_atomic(df)
            
    def _validate_decimal_types(self, df: DataFrame) -> None:
        """
        Valida que os tipos decimais estão corretos antes da escrita
        
        Args:
            df: DataFrame a ser validado
            
        Raises:
            ETLException: Se tipos não estiverem corretos
        """
        try:
            # Verificar se a coluna vlr_transacao_movimento existe
            if "vlr_transacao_movimento" not in df.columns:
                raise ETLException("Coluna vlr_transacao_movimento não encontrada no DataFrame")
            
            # Obter schema da coluna
            schema = df.select("vlr_transacao_movimento").schema
            
            if not schema:
                raise ETLException("Não foi possível obter schema da coluna vlr_transacao_movimento")
            
            # Verificar se é do tipo correto
            field = schema[0]  # Primeiro (e único) campo
            actual_type = field.dataType
            
            # Verificar se é DecimalType
            if not isinstance(actual_type, DecimalType):
                raise ETLException(
                    f"Tipo incorreto para vlr_transacao_movimento: esperado DecimalType, "
                    f"obtido {type(actual_type).__name__}"
                )
            
            # Verificar precisão e escala
            precision = actual_type.precision
            scale = actual_type.scale
            
            self.logger.info(
                f"✓ Tipo validado: vlr_transacao_movimento = DecimalType({precision}, {scale})"
            )
            
            # Log adicional para debug se necessário
            if precision != 20 or scale != 2:
                self.logger.warning(
                    f"⚠️ Atenção: precisão atual é ({precision}, {scale}), "
                    f"esperado era (20, 2) do cast inicial"
                )
            
        except Exception as e:
            self.logger.error(f"✗ Erro na validação de tipos decimais: {str(e)}")
            raise ETLException(f"Falha na validação de tipos decimais: {str(e)}")
    
    def _validate_final_decimal_precision(self, df: DataFrame) -> None:
        """
        Valida que o cast final para DecimalType(10,2) foi aplicado corretamente
        
        Args:
            df: DataFrame com a coluna vlr_transacao_movimento_final
            
        Raises:
            ETLException: Se o cast não foi aplicado corretamente
        """
        try:
            # Verificar se a coluna final existe
            if "vlr_transacao_movimento_final" not in df.columns:
                raise ETLException("Coluna vlr_transacao_movimento_final não encontrada após cast")
            
            # Obter schema da coluna final
            schema = df.select("vlr_transacao_movimento_final").schema
            
            if not schema:
                raise ETLException("Não foi possível obter schema da coluna vlr_transacao_movimento_final")
            
            # Verificar se é do tipo correto
            field = schema[0]
            actual_type = field.dataType
            
            # Verificar se é DecimalType
            if not isinstance(actual_type, DecimalType):
                raise ETLException(
                    f"Tipo incorreto após cast final: esperado DecimalType, "
                    f"obtido {type(actual_type).__name__}"
                )
            
            # Verificar precisão e escala específicas do cast final
            precision = actual_type.precision
            scale = actual_type.scale
            
            # Validar que temos exatamente DecimalType(10, 2)
            if precision != 10 or scale != 2:
                raise ETLException(
                    f"Precisão incorreta após cast: esperado DecimalType(10, 2), "
                    f"obtido DecimalType({precision}, {scale})"
                )
            
            self.logger.info(
                f"✓ Cast final validado: vlr_transacao_movimento_final = DecimalType({precision}, {scale})"
            )
            
        except Exception as e:
            self.logger.error(f"✗ Erro na validação do cast final: {str(e)}")
            raise ETLException(f"Falha na validação do cast final: {str(e)}")
    
    def _validate_datetime_formats(self, df: DataFrame) -> None:
        """
        Valida que todas as colunas de data estão no formato ISO8601 UTC correto
        
        Args:
            df: DataFrame a ser validado
            
        Raises:
            ETLException: Se formatos de data não estiverem corretos
        """
        try:
            # Colunas de data que devem estar em formato ISO8601 com Z
            datetime_columns = [
                "data_movimento",
                "data_emissao_cartao", 
                "data_criacao_conta"
            ]
            
            # Verificar se todas as colunas de data existem
            missing_columns = [col for col in datetime_columns if col not in df.columns]
            if missing_columns:
                raise ETLException(f"Colunas de data não encontradas: {missing_columns}")
            
            # Amostrar alguns valores para validar formato
            sample_df = df.select(datetime_columns).limit(5)
            
            for row in sample_df.collect():
                for col in datetime_columns:
                    value = row[col]
                    if value:
                        # Verificar se termina com 'Z' (UTC)
                        if not str(value).endswith('Z'):
                            raise ETLException(
                                f"Data não está em formato UTC: {col} = {value}. "
                                f"Esperado formato ISO8601 terminando com 'Z'"
                            )
        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao atualizar status do watermark: {str(e)}")
    
    def run(self) -> None:
        """
        Executa o pipeline ETL completo (versão original mantida para compatibilidade)
        """
        # Usar processamento incremental se habilitado
        if Config.INCREMENTAL_PROCESSING:
            self.run_incremental()
        else:
            self.run_full()
    
    def run_incremental(self) -> None:
        """
        Executa pipeline em modo incremental
        """
        print_banner("INICIANDO PIPELINE ETL - MODO INCREMENTAL", "=")
        
        # Inicializar observabilidade
        if observability_manager.is_enabled():
            pipeline_timer_id = observability_manager.get_collector().start_timer("pipeline_total")
            self.logger.info("📊 Observabilidade habilitada - métricas serão coletadas")

        self.stats["inicio"] = time.time()

        # Log estruturado de início do pipeline
        self.log_structured(
            "INFO",
            "Iniciando pipeline ETL",
            pipeline_mode="incremental",
            observability_enabled=observability_manager.is_enabled(),
            spark_master=Config.SPARK_MASTER,
            output_dir=self.output_dir
        )

        try:
            # Validar configurações
            Config.validate()
            Config.print_config()

            # Criar sessão Spark
            spark_timer_id = observability_manager.get_collector().start_timer("spark_session_creation")
            self.create_spark_session()
            observability_manager.get_collector().stop_timer(spark_timer_id)

            # Validar conexão MySQL
            mysql_timer_id = observability_manager.get_collector().start_timer("mysql_connection_validation")
            validate_mysql_connection(self.spark)
            observability_manager.get_collector().stop_timer(mysql_timer_id)

            # BRONZE: Extrair dados incrementalmente
            extract_timer_id = observability_manager.get_collector().start_timer("extract_stage")
            df_associado, df_conta, df_cartao, df_movimento = self.extract_all_tables_incremental()
            extract_result = observability_manager.get_collector().stop_timer(extract_timer_id)
            
            # Verificar se há dados novos para processar
            if df_movimento.count() == 0:
                self.logger.info("📭 Nenhum dado novo encontrado - execução incremental concluída")
                
                # Ainda assim, finalizar métricas
                self.stats["fim"] = time.time()
                self.stats["duracao"] = format_duration(self.stats["fim"] - self.stats["inicio"])
                
                if observability_manager.is_enabled():
                    observability_manager.get_collector().stop_timer(pipeline_timer_id)
                
                print_banner("PIPELINE INCREMENTAL CONCLUÍDO - NENHUM DADO NOVO", "=")
                return
            
            # SILVER: Transformar e unir
            transform_timer_id = observability_manager.get_collector().start_timer("transform_stage")
            df_transformed = self.transform_and_join(df_associado, df_conta, df_cartao, df_movimento)
            transform_result = observability_manager.get_collector().stop_timer(transform_timer_id)
            
            # GOLD: Carregar com atomic write
            load_timer_id = observability_manager.get_collector().start_timer("load_stage")
            output_paths = self.load_to_csv_atomic(df_transformed)
            load_result = observability_manager.get_collector().stop_timer(load_timer_id)
            
            # Finalizar métricas
            self.stats["fim"] = time.time()
            self.stats["duracao"] = format_duration(self.stats["fim"] - self.stats["inicio"])
            
            # Parar timer total do pipeline
            if observability_manager.is_enabled():
                total_result = observability_manager.get_collector().stop_timer(pipeline_timer_id)
                if total_result:
                    observability_manager.get_collector().record_gauge(
                        "etl_pipeline_duration_seconds",
                        total_result.duration_seconds,
                        {"status": "success", "mode": "incremental"}
                    )
            
            print_banner("PIPELINE INCREMENTAL CONCLUÍDO COM SUCESSO!", "=")
            print_statistics(self.stats)
            
            # Log estruturado de sucesso
            self.log_structured(
                "INFO",
                "Pipeline concluído com sucesso",
                status="success",
                mode="incremental",
                total_records=self.stats.get("registros_final", 0),
                duration_seconds=self.stats.get("duracao_seconds", 0),
                quality_checks_passed=self.stats.get("quality_checks", {}).get("passed", 0),
                quality_checks_warnings=self.stats.get("quality_checks", {}).get("warnings", 0),
                output_paths=str(output_paths)
            )
            
            # Log de observabilidade
            if observability_manager.is_enabled():
                observability_manager.log_metrics_summary()
                if observability_manager.push_to_gateway():
                    self.logger.info("✅ Métricas enviadas para sistema de monitoramento externo")
            
        except Exception as e:
            self.logger.error(f"✗ Pipeline incremental falhou: {str(e)}")
            
            # Log estruturado de erro
            self.log_structured(
                "ERROR",
                "Pipeline falhou",
                status="error",
                mode="incremental",
                error_message=str(e),
                error_type=type(e).__name__,
                duration_seconds=time.time() - self.stats.get("inicio", time.time())
            )
            
            # Registrar erro nas métricas
            if observability_manager.is_enabled():
                observability_manager.get_collector().record_error("pipeline_failure")
                observability_manager.get_collector().stop_timer(pipeline_timer_id)
            
            raise
        
        finally:
            # Encerrar sessão Spark
            if self.spark:
                self.spark.stop()
                self.logger.info("Sessão Spark encerrada")
    
    def run_full(self) -> None:
        """
        Executa pipeline em modo completo (legacy)
        """
        print_banner("INICIANDO PIPELINE ETL - MODO COMPLETO", "=")
        
        # Inicializar observabilidade
        if observability_manager.is_enabled():
            pipeline_timer_id = observability_manager.get_collector().start_timer("pipeline_total")
            self.logger.info("📊 Observabilidade habilitada - métricas serão coletadas")
        
        self.stats["inicio"] = time.time()
        
        # Log estruturado de início do pipeline completo
        self.log_structured(
            "INFO",
            "Iniciando pipeline ETL - MODO COMPLETO",
            pipeline_mode="full",
            observability_enabled=observability_manager.is_enabled(),
            spark_master=Config.SPARK_MASTER,
            output_dir=self.output_dir
        )

        try:
            # Validar configurações
            Config.validate()
            Config.print_config()
            
            # Criar sessão Spark
            spark_timer_id = observability_manager.get_collector().start_timer("spark_session_creation")
            self.create_spark_session()
            observability_manager.get_collector().stop_timer(spark_timer_id)
            
            # Validar conexão MySQL
            mysql_timer_id = observability_manager.get_collector().start_timer("mysql_connection_validation")
            validate_mysql_connection(self.spark)
            observability_manager.get_collector().stop_timer(mysql_timer_id)
            
            # BRONZE: Extrair dados (modo completo)
            extract_timer_id = observability_manager.get_collector().start_timer("extract_stage")
            df_associado, df_conta, df_cartao, df_movimento = self.extract_all_tables()
            extract_result = observability_manager.get_collector().stop_timer(extract_timer_id)
            
            # SILVER: Transformar e unir
            transform_timer_id = observability_manager.get_collector().start_timer("transform_stage")
            df_transformed = self.transform_and_join(df_associado, df_conta, df_cartao, df_movimento)
            transform_result = observability_manager.get_collector().stop_timer(transform_timer_id)
            
            # GOLD: Carregar
            load_timer_id = observability_manager.get_collector().start_timer("load_stage")
            output_paths = self.load_to_csv(df_transformed)
            load_result = observability_manager.get_collector().stop_timer(load_timer_id)
            
            # Finalizar métricas
            self.stats["fim"] = time.time()
            self.stats["duracao"] = format_duration(self.stats["fim"] - self.stats["inicio"])
            
            # Parar timer total do pipeline
            if observability_manager.is_enabled():
                total_result = observability_manager.get_collector().stop_timer(pipeline_timer_id)
                if total_result:
                    observability_manager.get_collector().record_gauge(
                        "etl_pipeline_duration_seconds",
                        total_result.duration_seconds,
                        {"status": "success", "mode": "full"}
                    )
            
            print_banner("PIPELINE COMPLETO CONCLUÍDO COM SUCESSO!", "=")
            print_statistics(self.stats)
            
            # Log estruturado de sucesso
            self.log_structured(
                "INFO",
                "Pipeline completo concluído com sucesso",
                status="success",
                mode="full",
                total_records=self.stats.get("registros_final", 0),
                duration_seconds=self.stats.get("duracao_seconds", 0),
                quality_checks_passed=self.stats.get("quality_checks", {}).get("passed", 0),
                quality_checks_warnings=self.stats.get("quality_checks", {}).get("warnings", 0),
                output_paths=str(output_paths)
            )
            
            # Log de observabilidade
            if observability_manager.is_enabled():
                observability_manager.log_metrics_summary()
                if observability_manager.push_to_gateway():
                    self.logger.info("✅ Métricas enviadas para sistema de monitoramento externo")
            
        except Exception as e:
            self.logger.error(f"✗ Pipeline completo falhou: {str(e)}")
            
            # Log estruturado de erro
            self.log_structured(
                "ERROR",
                "Pipeline completo falhou",
                status="error",
                mode="full",
                error_message=str(e),
                error_type=type(e).__name__,
                duration_seconds=time.time() - self.stats.get("inicio", time.time())
            )
            
            # Registrar erro nas métricas
            if observability_manager.is_enabled():
                observability_manager.get_collector().record_error("pipeline_failure")
                observability_manager.get_collector().stop_timer(pipeline_timer_id)
            
            raise
        
        finally:
            # Encerrar sessão Spark
            if self.spark:
                self.spark.stop()
                self.logger.info("Sessão Spark encerrada")

            # Registrar métricas de extração
            if extract_result:
                observability_manager.get_collector().record_pipeline_stage(
                    stage="extract",
                    duration_seconds=extract_result.duration_seconds,
                    records_input=0,  # Dados de entrada não medidos nesta etapa
                    records_output=self.stats.get("registros_movimento", 0)  # Total de movimentos como saída
                )


def main():
    """Função principal com argumentos de linha de comando"""
    
    parser = argparse.ArgumentParser(
        description="Pipeline ETL - SiCooperative Data Lake POC",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Modo completo (padrão)
  python etl_pipeline.py
  
  # Modo incremental (processa apenas registros novos)
  python etl_pipeline.py --run-mode incremental
  
  # Especificar diretório de saída
  python etl_pipeline.py --output ./output
  
  # Modo incremental com nível de log DEBUG
  python etl_pipeline.py --run-mode incremental --output /app/output --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=Config.OUTPUT_DIR,
        help=f"Diretório de saída para o CSV (padrão: {Config.OUTPUT_DIR})"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=Config.LOG_LEVEL,
        help=f"Nível de log (padrão: {Config.LOG_LEVEL})"
    )
    
    parser.add_argument(
        "--run-mode",
        type=str,
        choices=["full", "incremental"],
        default="full",
        help="Modo de execução: 'full' (processa todos os dados) ou 'incremental' (apenas dados novos) (padrão: full)"
    )
    
    args = parser.parse_args()
    
    # Atualizar configurações com argumentos
    Config.OUTPUT_DIR = args.output
    Config.LOG_LEVEL = args.log_level
    
    # Executar pipeline no modo especificado
    etl = SiCooperativeETL(
        output_dir=args.output,
        run_mode=args.run_mode
    )
    etl.run()


if __name__ == "__main__":
    main()
