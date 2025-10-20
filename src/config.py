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
    
    # Configurações de Logging
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

    # Arquivo de exportação de métricas (formato JSON)
    METRICS_EXPORT_FILE: str = os.getenv("METRICS_EXPORT_FILE", "pipeline_metrics.json")

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

    @classmethod
    def get_spark_config(cls):
        """
        Retorna as configurações do Spark
        """
        return {
            "spark.app.name": cls.SPARK_APP_NAME,
            "spark.master": cls.SPARK_MASTER,
            "spark.driver.memory": cls.SPARK_DRIVER_MEMORY,
            "spark.executor.memory": cls.SPARK_EXECUTOR_MEMORY,
            "spark.sql.adaptive.enabled": cls.SPARK_SQL_ADAPTIVE_ENABLED,
            "spark.sql.adaptive.coalescePartitions.enabled": cls.SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS
        }


# Instância global de configuração
config = Config()
