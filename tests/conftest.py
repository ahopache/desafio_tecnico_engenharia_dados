"""
Configuração de Fixtures para Testes
Fixtures compartilhadas entre todos os testes
"""

import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType
)
from datetime import datetime

# Adicionar src ao path para imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def spark():
    """
    Cria uma sessão Spark para testes
    Scope: session (uma única sessão para todos os testes)
    """
    spark = SparkSession.builder \
        .appName("SiCooperative-Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()
    
    # Configurar nível de log
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    # Cleanup
    spark.stop()


@pytest.fixture
def sample_associado_data():
    """Dados de exemplo para tabela associado"""
    return [
        (1, "João", "Silva", 35, "joao.silva@email.com"),
        (2, "Maria", "Santos", 28, "maria.santos@email.com"),
        (3, "Pedro", "Oliveira", 42, "pedro.oliveira@email.com")
    ]


@pytest.fixture
def sample_conta_data():
    """Dados de exemplo para tabela conta"""
    return [
        (1, "corrente", datetime(2020, 1, 15, 10, 30), 1),
        (2, "poupanca", datetime(2020, 6, 20, 14, 15), 1),
        (3, "corrente", datetime(2019, 3, 10, 9, 0), 2)
    ]


@pytest.fixture
def sample_cartao_data():
    """Dados de exemplo para tabela cartao"""
    return [
        (1, "4532123456789012", "JOAO SILVA", datetime(2020, 1, 20, 10, 0), 1, 1),
        (2, "5412345678901234", "JOAO SILVA", datetime(2020, 2, 15, 11, 30), 1, 1),
        (3, "4024007156789012", "MARIA SANTOS", datetime(2019, 3, 15, 10, 30), 3, 2)
    ]


@pytest.fixture
def sample_movimento_data():
    """Dados de exemplo para tabela movimento"""
    return [
        (1, 150.50, "Compra em Zaffari", datetime(2024, 10, 13, 15, 30), 1),
        (2, 75.20, "Posto Ipiranga", datetime(2024, 10, 12, 8, 15), 1),
        (3, 200.00, "Farmacia Panvel", datetime(2024, 10, 11, 14, 45), 2),
        (4, 89.90, "Restaurante Don Aurélio", datetime(2024, 10, 10, 12, 30), 3)
    ]


@pytest.fixture
def df_associado(spark, sample_associado_data):
    """DataFrame de associados para testes"""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("nome", StringType(), False),
        StructField("sobrenome", StringType(), False),
        StructField("idade", IntegerType(), False),
        StructField("email", StringType(), False)
    ])
    
    return spark.createDataFrame(sample_associado_data, schema)


@pytest.fixture
def df_conta(spark, sample_conta_data):
    """DataFrame de contas para testes"""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("tipo", StringType(), False),
        StructField("data_criacao", TimestampType(), False),
        StructField("id_associado", IntegerType(), False)
    ])
    
    return spark.createDataFrame(sample_conta_data, schema)


@pytest.fixture
def df_cartao(spark, sample_cartao_data):
    """DataFrame de cartões para testes"""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("num_cartao", StringType(), False),
        StructField("nom_impresso", StringType(), False),
        StructField("data_criacao", TimestampType(), False),
        StructField("id_conta", IntegerType(), False),
        StructField("id_associado", IntegerType(), False)
    ])
    
    return spark.createDataFrame(sample_cartao_data, schema)


@pytest.fixture
def df_movimento(spark, sample_movimento_data):
    """DataFrame de movimentos para testes"""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("vlr_transacao", DecimalType(10, 2), False),
        StructField("des_transacao", StringType(), False),
        StructField("data_movimento", TimestampType(), False),
        StructField("id_cartao", IntegerType(), False)
    ])
    
    return spark.createDataFrame(sample_movimento_data, schema)


@pytest.fixture
def expected_output_columns():
    """Lista de colunas esperadas no output final"""
    return [
        "nome_associado",
        "sobrenome_associado",
        "idade_associado",
        "vlr_transacao_movimento",
        "des_transacao_movimento",
        "data_movimento",
        "numero_cartao",
        "nome_impresso_cartao",
        "data_criacao_cartao",
        "tipo_conta",
        "data_criacao_conta"
    ]
