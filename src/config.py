"""
Módulo de Configuração
Gerencia variáveis de ambiente e configurações do pipeline ETL
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()


class Config:
    """Classe de configuração centralizada para o pipeline ETL"""
    
    # ========================================================================
    # CONFIGURAÇÕES DO MYSQL
    # ========================================================================
    
    MYSQL_HOST: str     = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT: int     = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_DATABASE: str = os.getenv("MYSQL_DATABASE", "sicooperative_db")
    MYSQL_USER: str     = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD: str = os.getenv("MYSQL_PASS", "")
    
    @classmethod
    def get_mysql_jdbc_url(cls) -> str:
        """
        Retorna a URL JDBC para conexão com MySQL
        
        Returns:
            str: URL JDBC formatada
        """
        return (
            f"jdbc:mysql://{cls.MYSQL_HOST}:{cls.MYSQL_PORT}/"
            f"{cls.MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true"
        )
    
    @classmethod
    def get_mysql_properties(cls) -> Dict[str, str]:
        """
        Retorna as propriedades de conexão JDBC
        
        Returns:
            Dict[str, str]: Dicionário com propriedades de conexão
        """
        return {
            "user": cls.MYSQL_USER,
            "password": cls.MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    
    # ========================================================================
    # CONFIGURAÇÕES DO SPARK
    # ========================================================================
    
    SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "SiCooperative-ETL")
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
    
    # Configurações de memória
    SPARK_DRIVER_MEMORY: str = os.getenv("SPARK_DRIVER_MEMORY", "2g")
    SPARK_EXECUTOR_MEMORY: str = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    
    # Configurações de otimização
    SPARK_SQL_ADAPTIVE_ENABLED: str = os.getenv("SPARK_SQL_ADAPTIVE_ENABLED", "true")
    SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS: str = os.getenv(
        "SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS", "true"
    )
    
    # ========================================================================
    # CONFIGURAÇÕES DE PARTICIONAMENTO JDBC
    # ========================================================================

    # Controle de particionamento JDBC (otimização para grandes volumes de dados)
    JDBC_PARTITIONING_ENABLED: bool = os.getenv("JDBC_PARTITIONING_ENABLED", "true").lower() == "true"

    # Configurações de particionamento para tabelas grandes
    JDBC_PARTITION_COLUMN: str = os.getenv("JDBC_PARTITION_COLUMN", "id")
    JDBC_LOWER_BOUND: int = int(os.getenv("JDBC_LOWER_BOUND", "1"))
    JDBC_UPPER_BOUND: int = int(os.getenv("JDBC_UPPER_BOUND", "1000000"))
    JDBC_NUM_PARTITIONS: int = int(os.getenv("JDBC_NUM_PARTITIONS", "8"))

    # Tabelas que devem usar particionamento (tabelas com muitos registros)
    JDBC_PARTITIONED_TABLES: list = os.getenv("JDBC_PARTITIONED_TABLES", "movimento,cartao,conta").split(",")

    @classmethod
    def should_use_partitioning(cls, table_name: str) -> bool:
        """
        Verifica se uma tabela deve usar particionamento JDBC

        Args:
            table_name: Nome da tabela

        Returns:
            bool: True se deve usar particionamento
        """
        return (
            cls.JDBC_PARTITIONING_ENABLED and
            table_name in cls.JDBC_PARTITIONED_TABLES
        )

    @classmethod
    def get_jdbc_partition_options(cls, table_name: str) -> Dict[str, Any]:
        """
        Retorna opções de particionamento JDBC para uma tabela

        Args:
            table_name: Nome da tabela

        Returns:
            Dict[str, Any]: Opções de particionamento
        """
        if not cls.should_use_partitioning(table_name):
            return {}

        return {
            "partitionColumn": cls.JDBC_PARTITION_COLUMN,
            "lowerBound": cls.JDBC_LOWER_BOUND,
            "upperBound": cls.JDBC_UPPER_BOUND,
            "numPartitions": cls.JDBC_NUM_PARTITIONS
        }
    
    # ========================================================================
    # CONFIGURAÇÕES DE OUTPUT
    # ========================================================================
    
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "./output")
    OUTPUT_FILENAME: str = os.getenv("OUTPUT_FILENAME", "movimento_flat.csv")
    OUTPUT_MODE: str = os.getenv("OUTPUT_MODE", "overwrite")  # overwrite, append
    OUTPUT_HEADER: bool = os.getenv("OUTPUT_HEADER", "true").lower() == "true"
    OUTPUT_DELIMITER: str = os.getenv("OUTPUT_DELIMITER", ",")
    OUTPUT_ENCODING: str = os.getenv("OUTPUT_ENCODING", "UTF-8")
    
    # Configurações do Parquet
    PARQUET_COMPRESSION: str = os.getenv("PARQUET_COMPRESSION", "snappy")  # none, snappy, gzip, lzo, brotli, lz4
    
    @classmethod
    def get_output_path(cls) -> str:
        """
        Retorna o caminho completo do arquivo de saída
        
        Returns:
            str: Caminho completo do CSV de saída
        """
        return os.path.join(cls.OUTPUT_DIR, cls.OUTPUT_FILENAME)
    
    # ========================================================================
    # CONFIGURAÇÕES DE LOGGING
    # ========================================================================
    # Configurações de Segurança
    HASH_SALT: str = os.getenv("HASH_SALT", "s1c00p3r4t1v3_s3cur3_s4lt")  # Deve ser alterado em produção!
    
    # Configurações de Formato de Saída
    OUTPUT_FORMAT: str = os.getenv("OUTPUT_FORMAT", "csv,parquet")  # csv,parquet ou ambos separados por vírgula
    
    # Configurações de Timezone
    SOURCE_TIMEZONE: str = os.getenv("SOURCE_TIMEZONE", "America/Sao_Paulo")  # Timezone do banco de dados
    OUTPUT_TIMEZONE: str = os.getenv("OUTPUT_TIMEZONE", "UTC")  # Timezone de saída (UTC por padrão)
    DATETIME_FORMAT: str = os.getenv("DATETIME_FORMAT", "yyyy-MM-dd'T'HH:mm:ss'Z'")  # Formato ISO8601
    
    # Configurações de Processamento Incremental
    INCREMENTAL_PROCESSING: bool = os.getenv("INCREMENTAL_PROCESSING", "true").lower() == "true"  # Habilitar processamento incremental
    WATERMARK_TABLE: str = "etl_metadata"  # Tabela de controle de watermark
    LOOKBACK_DAYS: int = int(os.getenv("LOOKBACK_DAYS", "7"))  # Dias para buscar dados antigos (para late arrivals)
    RETRY_ATTEMPTS: int = int(os.getenv("RETRY_ATTEMPTS", "3"))  # Número máximo de tentativas de retry
    RETRY_DELAY_SECONDS: int = int(os.getenv("RETRY_DELAY_SECONDS", "60"))  # Delay entre retries (segundos)
    
    # Configurações de Atomic Writes
    ATOMIC_WRITES_ENABLED: bool = os.getenv("ATOMIC_WRITES_ENABLED", "true").lower() == "true"  # Habilitar atomic writes
    TEMP_DIR_SUFFIX: str = os.getenv("TEMP_DIR_SUFFIX", "_temp")  # Sufixo para diretórios temporários
    CLEANUP_TEMP_FILES: bool = os.getenv("CLEANUP_TEMP_FILES", "true").lower() == "true"  # Limpar arquivos temporários em caso de erro
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")  # DEBUG, INFO, WARNING, ERROR
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
    
    # ========================================================================
    # SCHEMA DAS TABELAS
    # ========================================================================
    
    # Nomes das tabelas no MySQL
    TABLE_ASSOCIADO: str = "associado"
    TABLE_CONTA: str     = "conta"
    TABLE_CARTAO: str    = "cartao"
    TABLE_MOVIMENTO: str = "movimento"
    
    # Colunas do CSV final (conforme especificação do desafio)
    OUTPUT_COLUMNS: list = [
        "nome_associado",
        "sobrenome_associado",
        "idade_associado",
        "id_movimento",
        "vlr_transacao_movimento",
        "des_transacao_movimento",
        "data_movimento",
        "numero_cartao_masked",
        "nome_impresso_cartao",
        "data_emissao_cartao",
        "tipo_conta",
        "data_criacao_conta"
    ]
    
    # ========================================================================
    # VALIDAÇÕES
    # ========================================================================
    
    @classmethod
    def validate(cls) -> None:
        """
        Valida as configurações obrigatórias
        
        Raises:
            ValueError: Se alguma configuração obrigatória estiver faltando
        """
        required_configs = {
            "MYSQL_HOST": cls.MYSQL_HOST,
            "MYSQL_DATABASE": cls.MYSQL_DATABASE,
            "MYSQL_USER": cls.MYSQL_USER,
            "OUTPUT_DIR": cls.OUTPUT_DIR
        }
        
        missing = [key for key, value in required_configs.items() if not value]
        
        if missing:
            raise ValueError(
                f"Configurações obrigatórias faltando: {', '.join(missing)}"
            )
    
    # ========================================================================
    # CONFIGURAÇÕES DE OBSERVABILIDADE
    # ========================================================================

    # Controle de observabilidade e métricas
    OBSERVABILITY_ENABLED: bool = os.getenv("OBSERVABILITY_ENABLED", "true").lower() == "true"

    # Configurações do Prometheus Pushgateway (opcional)
    PROMETHEUS_GATEWAY_URL: str = os.getenv("PROMETHEUS_GATEWAY_URL", "")
    PROMETHEUS_JOB_NAME: str = os.getenv("PROMETHEUS_JOB_NAME", "sicooperative-etl")

    # Métricas detalhadas
    METRICS_DETAILED_LOGGING: bool = os.getenv("METRICS_DETAILED_LOGGING", "false").lower() == "true"

    # ========================================================================
    # CONFIGURAÇÕES DE PARTICIONAMENTO JDBC
    # ========================================================================

    # Controle geral de particionamento JDBC
    JDBC_PARTITIONING_ENABLED: bool = os.getenv("JDBC_PARTITIONING_ENABLED", "true").lower() == "true"

    # Tamanho alvo por partição (em registros)
    JDBC_TARGET_RECORDS_PER_PARTITION: int = int(os.getenv("JDBC_TARGET_RECORDS_PER_PARTITION", "25000"))

    # Fator de ajuste para tabelas pequenas (reduz paralelismo)
    JDBC_SMALL_TABLE_FACTOR: float = float(os.getenv("JDBC_SMALL_TABLE_FACTOR", "0.1"))

    # Número máximo de conexões JDBC por tabela
    JDBC_MAX_CONNECTIONS_PER_TABLE: int = int(os.getenv("JDBC_MAX_CONNECTIONS_PER_TABLE", "10"))

    # Timeout para queries de estatísticas (segundos)
    JDBC_STATS_QUERY_TIMEOUT: int = int(os.getenv("JDBC_STATS_QUERY_TIMEOUT", "30"))

    @classmethod
    def get_jdbc_partition_options(cls, table_name: str, spark_session=None) -> dict:
        """
        Obtém opções de particionamento JDBC otimizadas para tabela específica

        Implementa:
        - Tuning dinâmico: lowerBound/upperBound obtidos dinamicamente via SELECT MIN(id), MAX(id)
        - numPartitions ajustável baseado no tamanho da tabela
        - Fallback para small tables: evita paralelismo exagerado

        Args:
            table_name: Nome da tabela para particionamento
            spark_session: Sessão Spark (opcional, para queries dinâmicas)

        Returns:
            dict: Opções de particionamento JDBC ou None se não aplicável
        """
        if not cls.JDBC_PARTITIONING_ENABLED:
            return None

        # Configurações de particionamento por tabela
        partition_configs = {
            "associado": {
                "column": "id",
                "min_partitions": 2,
                "max_partitions": 8,
                "small_table_threshold": 10000,
                "use_dynamic_bounds": True
            },
            "conta": {
                "column": "id",
                "min_partitions": 2,
                "max_partitions": 8,
                "small_table_threshold": 5000,
                "use_dynamic_bounds": True
            },
            "cartao": {
                "column": "id",
                "min_partitions": 4,
                "max_partitions": 16,
                "small_table_threshold": 20000,
                "use_dynamic_bounds": True
            },
            "movimento": {
                "column": "id",
                "min_partitions": 8,
                "max_partitions": 32,
                "small_table_threshold": 100000,
                "use_dynamic_bounds": True
            }
        }

        if table_name not in partition_configs:
            return None

        config = partition_configs[table_name]
        partition_column = config["column"]

        # Se não temos sessão Spark, usar valores padrão
        if not spark_session:
            return {
                "partitionColumn": partition_column,
                "lowerBound": 1,
                "upperBound": 1000000,
                "numPartitions": min(config["max_partitions"], 8)
            }

        try:
            # Obter estatísticas da tabela dinamicamente
            jdbc_url = cls.get_mysql_jdbc_url()

            # Query para obter MIN e MAX do ID com timeout
            bounds_query = f"(SELECT MIN({partition_column}) as min_id, MAX({partition_column}) as max_id FROM {table_name}) as bounds_query"

            bounds_df = spark_session.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", bounds_query) \
                .option("user", cls.MYSQL_USER) \
                .option("password", cls.MYSQL_PASSWORD) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("fetchsize", "1000") \
                .load()

            if bounds_df.count() == 0:
                # Tabela vazia, não particionar
                return None

            bounds_row = bounds_df.first()
            min_id = bounds_row["min_id"]
            max_id = bounds_row["max_id"]

            if min_id is None or max_id is None:
                # Não há dados para particionar
                return None

            # Calcular número de partições baseado no tamanho
            total_records = max_id - min_id + 1

            # Fallback para small tables: evitar paralelismo exagerado
            if total_records < config["small_table_threshold"]:
                # Para tabelas pequenas, usar menos partições
                num_partitions = max(1, min(config["min_partitions"], total_records // 1000 + 1))
            else:
                # Para tabelas grandes, calcular partições otimizadas
                target_records_per_partition = cls.JDBC_TARGET_RECORDS_PER_PARTITION
                calculated_partitions = max(
                    config["min_partitions"],
                    min(config["max_partitions"], total_records // target_records_per_partition + 1)
                )
                num_partitions = calculated_partitions

            # Ajuste final: garantir que não exceda o range disponível
            if max_id - min_id < num_partitions:
                num_partitions = max(1, max_id - min_id + 1)

            # Limitar pelo máximo de conexões JDBC
            num_partitions = min(num_partitions, cls.JDBC_MAX_CONNECTIONS_PER_TABLE)

            partition_options = {
                "partitionColumn": partition_column,
                "lowerBound": int(min_id),
                "upperBound": int(max_id),
                "numPartitions": num_partitions
            }

            return partition_options

        except Exception as e:
            # Em caso de erro, usar configuração padrão conservadora
            print(f"⚠️ Erro ao obter estatísticas dinâmicas para {table_name}: {e}")
            print(f"📊 Usando configuração padrão de particionamento")

            return {
                "partitionColumn": partition_column,
                "lowerBound": 1,
                "upperBound": 100000,
                "numPartitions": config["min_partitions"]
            }

    @classmethod
    def demonstrate_jdbc_partitioning(cls, spark_session=None):
        """
        Demonstra como funciona o sistema de particionamento JDBC

        Args:
            spark_session: Sessão Spark para testes dinâmicos
        """
        print("\nDemonstracao do Sistema de Particionamento JDBC Otimizado")
        print("=" * 60)

        # Cenários de exemplo
        scenarios = [
            ("movimento", 50000, "Tabela média"),
            ("movimento", 500000, "Tabela grande"),
            ("movimento", 5000, "Tabela pequena"),
            ("cartao", 15000, "Cartões médio"),
            ("associado", 2000, "Associados pequeno")
        ]

        for table_name, record_count, description in scenarios:
            print(f"\n{description} ({record_count} registros):")

            # Simular estatísticas da tabela
            min_id = 1
            max_id = record_count

            # Calcular particionamento
            total_records = max_id - min_id + 1

            if total_records < 10000:  # Small table
                num_partitions = max(1, min(2, total_records // 1000 + 1))
                print(f"  -> Particoes: {num_partitions} (otimizado para tabela pequena)")
            else:  # Large table
                target_records_per_partition = cls.JDBC_TARGET_RECORDS_PER_PARTITION
                calculated_partitions = max(8, min(32, total_records // target_records_per_partition + 1))
                print(f"  -> Particoes: {calculated_partitions} (otimizado para tabela grande)")

            # Mostrar configuração
            options = cls.get_jdbc_partition_options(table_name, spark_session)
            if options:
                print(f"  -> Range: {options['lowerBound']}-{options['upperBound']}")
                print(f"  -> Particoes JDBC: {options['numPartitions']}")
            else:
                print("  -> Sem particionamento (tabela vazia)")

        print("\n" + "=" * 60)
        print("Beneficios implementados:")
        print("- Tuning dinamico baseado em estatisticas reais")
        print("- Fallback inteligente para tabelas pequenas")
        print("- Otimizacao automatica de numPartitions")
        print("- Controle de conexoes JDBC por tabela")
        print("- Configuracao centralizada e ajustavel")

    @classmethod
    def print_jdbc_partitioning_info(cls):
        """
        Imprime informações sobre configurações de particionamento JDBC
        """
        print("\nConfiguracoes de Particionamento JDBC:")
        print(f"  - Particionamento habilitado: {cls.JDBC_PARTITIONING_ENABLED}")
        print(f"  - Registros alvo por particao: {cls.JDBC_TARGET_RECORDS_PER_PARTITION}")
        print(f"  - Maximo de conexoes por tabela: {cls.JDBC_MAX_CONNECTIONS_PER_TABLE}")
        print(f"  - Timeout de estatisticas: {cls.JDBC_STATS_QUERY_TIMEOUT}s")

        print("\nConfiguracoes por tabela:")
        table_configs = {
            "associado": (10000, "2-8"),
            "conta": (5000, "2-8"),
            "cartao": (20000, "4-16"),
            "movimento": (100000, "8-32")
        }

        for table, (threshold, partitions) in table_configs.items():
            print(f"  - {table}: threshold={threshold}, particoes={partitions}")

    @classmethod
    def print_config(cls):
        """
        Imprime as configurações do pipeline ETL
        """
        print("Configurações do Pipeline ETL")
        print("============================")
        for key, value in cls.__dict__.items():
            if not key.startswith("_"):
                if 'PASS' in key:
                    print(f"{key}: ********")
                else:
                    print(f"{key}: {value}")

        # Imprimir informações específicas de particionamento JDBC
        cls.print_jdbc_partitioning_info()

    @classmethod
    def get_spark_config(cls) -> dict:
        """
        Retorna configurações básicas do Spark

        Returns:
            dict: Configurações do Spark
        """
        return {
            # Nome da aplicação Spark
            "spark.app.name": cls.SPARK_APP_NAME,
            # Endereço do master Spark
            "spark.master": cls.SPARK_MASTER,
            # Memória do driver Spark
            "spark.driver.memory": cls.SPARK_DRIVER_MEMORY,
            # Memória do executor Spark
            "spark.executor.memory": cls.SPARK_EXECUTOR_MEMORY,
            # Ativação do Adaptive Query Execution
            "spark.sql.adaptive.enabled": cls.SPARK_SQL_ADAPTIVE_ENABLED,
            # Ativação do coalescePartitions
            "spark.sql.adaptive.coalescePartitions.enabled": cls.SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS
        }


# Instância global de configuração
config = Config()
