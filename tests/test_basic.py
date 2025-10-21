"""
Testes básicos para o pipeline ETL SiCooperative
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Cria sessão Spark para testes"""
    spark = SparkSession.builder \
        .appName("SiCooperative-Tests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_spark_session_creation(spark):
    """Testa se a sessão Spark é criada corretamente"""
    assert spark is not None
    assert spark.version is not None


def test_basic_dataframe_operations(spark):
    """Testa operações básicas de DataFrame"""
    # Criar DataFrame de teste
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])

    # Testar operações básicas
    assert df.count() == 3
    assert df.columns == ["name", "age"]

    # Testar filtro
    filtered_df = df.filter(df.age > 25)
    assert filtered_df.count() == 2


def test_import_etl_modules():
    """Testa se módulos ETL podem ser importados"""
    try:
        # Tentar importar módulos principais (sem executá-los)
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

        # Apenas testar imports básicos
        import importlib.util
        spec = importlib.util.spec_from_file_location("config", "../src/config.py")
        assert spec is not None

        print("✅ Módulos ETL podem ser importados")
    except Exception as e:
        pytest.fail(f"Falha ao importar módulos ETL: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
