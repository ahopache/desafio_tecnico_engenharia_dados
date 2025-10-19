"""
Pipeline ETL Principal
Extrai dados do MySQL, transforma e gera CSV flat conforme especificação
"""
import os
import sys
import time
import locale
import argparse
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
    hash_sensitive_data
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

        return df_associado, df_conta, df_cartao, df_movimento
    
    def transform_and_join(
        self,
        df_associado: DataFrame,
        df_conta: DataFrame,
        df_cartao: DataFrame,
        df_movimento: DataFrame
    ) -> DataFrame:
        """
        Realiza transformações e JOINs (Silver Layer)
        
        Estratégia de JOIN:
        movimento → cartao → conta → associado
        
        Args:
            df_associado: DataFrame de associados
            df_conta: DataFrame de contas
            df_cartao: DataFrame de cartões
            df_movimento: DataFrame de movimentos
        
        Returns:
            DataFrame: Dados consolidados
        """
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
            
            # Selecionar e renomear colunas conforme especificação
            self.logger.info("Selecionando e renomeando colunas...")
            
            # Converter data_movimento para timestamp com timezone UTC
            df_joined = df_joined.withColumn(
                "data_movimento_utc",
                F.to_utc_timestamp("data_movimento", "UTC")
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
                F.date_format("data_movimento_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("data_movimento"),
                
                # Dados do cartão (mascarado e hash)
                mask_credit_card(df_cartao.num_cartao).alias("numero_cartao_masked"),
                hash_sensitive_data(df_cartao.num_cartao).alias("numero_cartao_hash"),
                
                df_cartao.nom_impresso.alias("nome_impresso_cartao"),
                df_cartao.data_criacao.alias("dt_emissao_cartao"),
                F.date_format(
                    F.to_utc_timestamp("dt_emissao_cartao", "UTC"), 
                    "yyyy-MM-dd'T'HH:mm:ss'Z'"
                ).alias("data_emissao_cartao"),
                
                # Dados da conta
                df_conta.tipo.alias("tipo_conta"),
                df_conta.data_criacao.alias("dt_criacao_conta"),
                F.date_format(
                    F.to_utc_timestamp("dt_criacao_conta", "UTC"),
                    "yyyy-MM-dd'T'HH:mm:ss'Z'"
                ).alias("data_criacao_conta")
            )
            
            # Validar colunas finais
            validate_dataframe(
                df_transformed,
                "transformed",
                min_rows=1,
                required_columns=Config.OUTPUT_COLUMNS
            )
            
            self.logger.info("✓ Transformações concluídas")
            
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"✗ Erro nas transformações: {str(e)}")
            raise ETLException(f"Falha nas transformações: {str(e)}")
    
    def load_to_csv(self, df: DataFrame) -> dict:
        """
        Carrega dados nos formatos configurados (CSV e/ou Parquet)
        
        Args:
            df: DataFrame transformado
            
        Returns:
            dict: Dicionário com caminhos dos arquivos gerados
        """
        try:
            # Criar diretório de saída
            create_output_directory(self.output_dir)
            
            # Adicionar coluna de data para particionamento
            df_with_date = df.withColumn(
                "data_movimento_date",
                F.to_date("data_movimento", "yyyy-MM-dd")
            )
            
            # Contar registros uma única vez
            total_registros = df.count()
            self.stats["registros_final"] = total_registros
            
            # Dicionário para armazenar os caminhos de saída
            output_paths = {}
            
            # Verificar quais formatos de saída usar
            output_formats = [f.strip().lower() for f in Config.OUTPUT_FORMAT.split(",")]
            
            # Validação de tipos antes da escrita
            self._validate_decimal_types(df)
            
            # Gerar CSV se solicitado
            if "csv" in output_formats:
                csv_path = f"{self.output_dir}/csv"
                create_output_directory(csv_path)
                
                # Aplicar ordenação determinística antes do coalesce
                # Ordena por data_movimento (ASC) e id_movimento (ASC) para garantir ordem consistente
                df_ordered = df.orderBy("data_movimento", "id_movimento")
                
                # Garantir precisão numérica com cast adicional para DecimalType(10,2)
                # antes da escrita - isso garante que mesmo após transformações, temos exatamente 2 casas decimais
                df_with_precision = df_ordered.withColumn(
                    "vlr_transacao_movimento_final",
                    F.col("vlr_transacao_movimento").cast(DecimalType(10, 2))
                )
                
                # Validar que o cast foi aplicado corretamente
                self._validate_final_decimal_precision(df_with_precision)
                
                # Coalescer para um único arquivo apenas para CSV
                df_coalesced = df_with_precision.coalesce(1)
                
                # Configurar locale para garantir separador decimal ponto (.)
                locale.setlocale(locale.LC_NUMERIC, 'C')
                
                df_coalesced.write \
                    .mode(Config.OUTPUT_MODE) \
                    .option("header", str(Config.OUTPUT_HEADER).lower()) \
                    .option("delimiter", Config.OUTPUT_DELIMITER) \
                    .option("encoding", Config.OUTPUT_ENCODING) \
                    .option("charset", "UTF-8") \
                    .option("quoteAll", False) \
                    .option("quoteMode", "MINIMAL") \
                    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'") \
                    .option("decimalFormat", "########.00") \
                    .csv(csv_path)
                
                output_paths["csv"] = csv_path
                self.logger.info(f"✓ CSV gerado com sucesso em: {csv_path}")
                self.logger.info("✓ Ordenação determinística aplicada (data_movimento, id_movimento)")
                self.logger.info("✓ Precisão numérica garantida: DecimalType(10,2) aplicado antes da escrita")
            
            # Gerar Parquet se solicitado
            if "parquet" in output_formats:
                parquet_path = f"{self.output_dir}/parquet"
                create_output_directory(parquet_path)
                
                # Escrever Parquet particionado por data
                df_with_date.write \
                    .mode(Config.OUTPUT_MODE) \
                    .partitionBy("data_movimento_date") \
                    .option("compression", Config.PARQUET_COMPRESSION) \
                    .parquet(parquet_path)
                
                output_paths["parquet"] = parquet_path
                self.logger.info(f"✓ Parquet particionado gerado em: {parquet_path}")
            
            self.logger.info(f"  Total de registros processados: {total_registros}")
            
            return output_paths
            
        except Exception as e:
            self.logger.error(f"✗ Erro ao salvar arquivos de saída: {str(e)}")
            raise ETLException(f"Falha ao salvar arquivos de saída: {str(e)}")
    
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
    
    def run(self) -> None:
        """
        Executa o pipeline ETL completo
        """
        print_banner("INICIANDO PIPELINE ETL - SICOOPERATIVE", "=")

        # Inicializar observabilidade
        if observability_manager.is_enabled():
            pipeline_timer_id = observability_manager.get_collector().start_timer("pipeline_total")
            self.logger.info("📊 Observabilidade habilitada - métricas serão coletadas")

        self.stats["inicio"] = time.time()

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

            # BRONZE: Extrair dados
            extract_timer_id = observability_manager.get_collector().start_timer("extract_stage")
            df_associado, df_conta, df_cartao, df_movimento = self.extract_all_tables()
            extract_result = observability_manager.get_collector().stop_timer(extract_timer_id)

            # Registrar métricas de extração
            if extract_result:
                observability_manager.get_collector().record_pipeline_stage(
                    stage="extract",
                    duration_seconds=extract_result.duration_seconds,
                    records_input=0,  # Dados de entrada não medidos nesta etapa
                    records_output=self.stats.get("registros_movimento", 0)  # Total de movimentos como saída
                )

            # SILVER: Transformar e unir
            transform_timer_id = observability_manager.get_collector().start_timer("transform_stage")
            df_transformed = self.transform_and_join(
                df_associado, df_conta, df_cartao, df_movimento
            )
            transform_result = observability_manager.get_collector().stop_timer(transform_timer_id)

            # Registrar métricas de transformação
            if transform_result:
                observability_manager.get_collector().record_pipeline_stage(
                    stage="transform",
                    duration_seconds=transform_result.duration_seconds,
                    records_input=self.stats.get("registros_movimento", 0),
                    records_output=df_transformed.count()
                )

            # GOLD: Carregar CSV
            load_timer_id = observability_manager.get_collector().start_timer("load_stage")
            output_paths = self.load_to_csv(df_transformed)
            load_result = observability_manager.get_collector().stop_timer(load_timer_id)

            # Registrar métricas de load
            if load_result:
                observability_manager.get_collector().record_pipeline_stage(
                    stage="load",
                    duration_seconds=load_result.duration_seconds,
                    records_input=df_transformed.count(),
                    records_output=df_transformed.count()  # Mesmo número, apenas formato diferente
                )

            # Finalizar
            self.stats["fim"] = time.time()
            self.stats["duracao"] = format_duration(
                self.stats["fim"] - self.stats["inicio"]
            )

            # Parar timer total do pipeline
            if observability_manager.is_enabled():
                total_result = observability_manager.get_collector().stop_timer(pipeline_timer_id)
                if total_result:
                    observability_manager.get_collector().record_gauge(
                        "etl_pipeline_duration_seconds",
                        total_result.duration_seconds,
                        {"status": "success"}
                    )

            print_banner("PIPELINE CONCLUÍDO COM SUCESSO!", "=")
            print_statistics(self.stats)

            # Log de observabilidade
            if observability_manager.is_enabled():
                observability_manager.log_metrics_summary()

                # Tentar enviar métricas para Prometheus se configurado
                if observability_manager.push_to_gateway():
                    self.logger.info("✅ Métricas enviadas para sistema de monitoramento externo")

        except Exception as e:
            self.logger.error(f"✗ Pipeline falhou: {str(e)}")

            # Registrar erro nas métricas
            if observability_manager.is_enabled():
                observability_manager.get_collector().record_error("pipeline_failure")
                observability_manager.get_collector().stop_timer(pipeline_timer_id)  # Parar timer mesmo com erro

            raise

        finally:
            # Encerrar sessão Spark
            if self.spark:
                self.spark.stop()
                self.logger.info("Sessão Spark encerrada")


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
