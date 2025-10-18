"""
Testes para o módulo etl_pipeline.py
Valida o pipeline ETL completo e suas transformações
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from chispa.dataframe_comparer import assert_df_equality
from etl_pipeline import SiCooperativeETL


class TestETLTransformations:
    """Testes para transformações do pipeline ETL"""
    
    def test_join_movimento_cartao(self, spark, df_movimento, df_cartao):
        """Testa JOIN entre movimento e cartao"""
        df_joined = df_movimento.join(
            df_cartao,
            df_movimento.id_cartao == df_cartao.id,
            "inner"
        )
        
        # Deve ter 4 movimentos (todos têm cartão válido)
        assert df_joined.count() == 4
        
        # Deve ter colunas de ambas as tabelas
        assert "vlr_transacao" in df_joined.columns
        assert "num_cartao" in df_joined.columns
    
    def test_join_cartao_conta(self, spark, df_cartao, df_conta):
        """Testa JOIN entre cartao e conta"""
        df_joined = df_cartao.join(
            df_conta,
            df_cartao.id_conta == df_conta.id,
            "inner"
        )
        
        # Deve ter 3 cartões (todos têm conta válida)
        assert df_joined.count() == 3
        
        # Deve ter colunas de ambas as tabelas
        assert "num_cartao" in df_joined.columns
        assert "tipo" in df_joined.columns
    
    def test_join_conta_associado(self, spark, df_conta, df_associado):
        """Testa JOIN entre conta e associado"""
        df_joined = df_conta.join(
            df_associado,
            df_conta.id_associado == df_associado.id,
            "inner"
        )
        
        # Deve ter 3 contas (todas têm associado válido)
        assert df_joined.count() == 3
        
        # Deve ter colunas de ambas as tabelas
        assert "tipo" in df_joined.columns
        assert "nome" in df_joined.columns
    
    def test_full_join_chain(
        self, 
        spark, 
        df_movimento, 
        df_cartao, 
        df_conta, 
        df_associado
    ):
        """Testa cadeia completa de JOINs"""
        # JOIN 1: movimento + cartao
        df_joined = df_movimento.join(
            df_cartao,
            df_movimento.id_cartao == df_cartao.id,
            "inner"
        )
        
        # JOIN 2: + conta
        df_joined = df_joined.join(
            df_conta,
            df_cartao.id_conta == df_conta.id,
            "inner"
        )
        
        # JOIN 3: + associado
        df_joined = df_joined.join(
            df_associado,
            df_cartao.id_associado == df_associado.id,
            "inner"
        )
        
        # Deve ter 4 registros finais
        assert df_joined.count() == 4
        
        # Deve ter colunas de todas as tabelas
        assert "vlr_transacao" in df_joined.columns  # movimento
        assert "num_cartao" in df_joined.columns     # cartao
        assert "tipo" in df_joined.columns           # conta
        assert "nome" in df_joined.columns           # associado
    
    def test_column_renaming(
        self, 
        spark, 
        df_movimento, 
        df_cartao, 
        df_conta, 
        df_associado,
        expected_output_columns
    ):
        """Testa renomeação de colunas conforme especificação"""
        # Realizar JOINs
        df_joined = df_movimento.join(
            df_cartao,
            df_movimento.id_cartao == df_cartao.id,
            "inner"
        ).join(
            df_conta,
            df_cartao.id_conta == df_conta.id,
            "inner"
        ).join(
            df_associado,
            df_cartao.id_associado == df_associado.id,
            "inner"
        )
        
        # Aplicar transformações (renomear colunas)
        df_transformed = df_joined.select(
            df_associado.nome.alias("nome_associado"),
            df_associado.sobrenome.alias("sobrenome_associado"),
            df_associado.idade.cast("string").alias("idade_associado"),
            df_movimento.vlr_transacao.cast("string").alias("vlr_transacao_movimento"),
            df_movimento.des_transacao.alias("des_transacao_movimento"),
            F.date_format(df_movimento.data_movimento, "yyyy-MM-dd HH:mm:ss")
                .alias("data_movimento"),
            df_cartao.num_cartao.alias("numero_cartao"),
            df_cartao.nom_impresso.alias("nome_impresso_cartao"),
            F.date_format(df_cartao.data_criacao, "yyyy-MM-dd HH:mm:ss")
                .alias("data_criacao_cartao"),
            df_conta.tipo.alias("tipo_conta"),
            F.date_format(df_conta.data_criacao, "yyyy-MM-dd HH:mm:ss")
                .alias("data_criacao_conta")
        )
        
        # Verificar se todas as colunas esperadas existem
        assert set(df_transformed.columns) == set(expected_output_columns)
        
        # Verificar ordem das colunas
        assert df_transformed.columns == expected_output_columns
    
    def test_data_type_conversions(
        self, 
        spark, 
        df_movimento, 
        df_cartao, 
        df_conta, 
        df_associado
    ):
        """Testa conversões de tipos de dados"""
        # Realizar JOINs e transformações
        df_joined = df_movimento.join(
            df_cartao,
            df_movimento.id_cartao == df_cartao.id,
            "inner"
        ).join(
            df_conta,
            df_cartao.id_conta == df_conta.id,
            "inner"
        ).join(
            df_associado,
            df_cartao.id_associado == df_associado.id,
            "inner"
        )
        
        df_transformed = df_joined.select(
            df_associado.idade.cast("string").alias("idade_associado"),
            df_movimento.vlr_transacao.cast("string").alias("vlr_transacao_movimento"),
            F.date_format(df_movimento.data_movimento, "yyyy-MM-dd HH:mm:ss")
                .alias("data_movimento")
        )
        
        # Verificar tipos
        schema = df_transformed.schema
        
        assert schema["idade_associado"].dataType == StringType()
        assert schema["vlr_transacao_movimento"].dataType == StringType()
        assert schema["data_movimento"].dataType == StringType()
    
    def test_no_data_loss_in_joins(
        self, 
        spark, 
        df_movimento, 
        df_cartao, 
        df_conta, 
        df_associado
    ):
        """Testa se não há perda de dados nos JOINs"""
        # Contar movimentos originais
        original_count = df_movimento.count()
        
        # Realizar JOINs
        df_joined = df_movimento.join(
            df_cartao,
            df_movimento.id_cartao == df_cartao.id,
            "inner"
        ).join(
            df_conta,
            df_cartao.id_conta == df_conta.id,
            "inner"
        ).join(
            df_associado,
            df_cartao.id_associado == df_associado.id,
            "inner"
        )
        
        # Contar após JOINs
        joined_count = df_joined.count()
        
        # Não deve haver perda de dados (todos os movimentos têm FK válidas)
        assert joined_count == original_count
    
    def test_date_format_consistency(
        self, 
        spark, 
        df_movimento, 
        df_cartao, 
        df_conta, 
        df_associado
    ):
        """Testa se o formato de data é consistente"""
        df_joined = df_movimento.join(
            df_cartao,
            df_movimento.id_cartao == df_cartao.id,
            "inner"
        ).join(
            df_conta,
            df_cartao.id_conta == df_conta.id,
            "inner"
        ).join(
            df_associado,
            df_cartao.id_associado == df_associado.id,
            "inner"
        )
        
        df_transformed = df_joined.select(
            F.date_format(df_movimento.data_movimento, "yyyy-MM-dd HH:mm:ss")
                .alias("data_movimento"),
            F.date_format(df_cartao.data_criacao, "yyyy-MM-dd HH:mm:ss")
                .alias("data_criacao_cartao"),
            F.date_format(df_conta.data_criacao, "yyyy-MM-dd HH:mm:ss")
                .alias("data_criacao_conta")
        )
        
        # Coletar primeira linha
        row = df_transformed.first()
        
        # Verificar formato (deve ser YYYY-MM-DD HH:MM:SS)
        assert len(row.data_movimento) == 19
        assert row.data_movimento[4] == "-"
        assert row.data_movimento[7] == "-"
        assert row.data_movimento[10] == " "
        assert row.data_movimento[13] == ":"
        assert row.data_movimento[16] == ":"


class TestETLPipeline:
    """Testes para a classe SiCooperativeETL"""
    
    def test_etl_instance_creation(self):
        """Testa criação de instância do ETL"""
        etl = SiCooperativeETL(output_dir="./test_output")
        
        assert etl.output_dir == "./test_output"
        assert etl.spark is None  # Não inicializado ainda
        assert isinstance(etl.stats, dict)
    
    def test_etl_create_spark_session(self):
        """Testa criação da sessão Spark"""
        etl = SiCooperativeETL()
        spark = etl.create_spark_session()
        
        assert spark is not None
        assert etl.spark is not None
        
        # Cleanup
        spark.stop()
    
    def test_transform_and_join_method(
        self,
        df_associado,
        df_conta,
        df_cartao,
        df_movimento,
        expected_output_columns
    ):
        """Testa método transform_and_join do ETL"""
        etl = SiCooperativeETL()
        etl.spark = df_movimento.sparkSession
        
        df_result = etl.transform_and_join(
            df_associado,
            df_conta,
            df_cartao,
            df_movimento
        )
        
        # Verificar colunas
        assert set(df_result.columns) == set(expected_output_columns)
        
        # Verificar contagem
        assert df_result.count() == 4
    
    def test_stats_initialization(self):
        """Testa inicialização das estatísticas"""
        etl = SiCooperativeETL()
        
        assert "inicio" in etl.stats
        assert "fim" in etl.stats
        assert "duracao" in etl.stats
        assert "registros_associado" in etl.stats
        assert "registros_conta" in etl.stats
        assert "registros_cartao" in etl.stats
        assert "registros_movimento" in etl.stats
        assert "registros_final" in etl.stats


class TestDataQuality:
    """Testes de qualidade de dados"""
    
    def test_no_null_values_in_required_fields(
        self,
        spark,
        df_movimento,
        df_cartao,
        df_conta,
        df_associado
    ):
        """Testa se não há valores nulos em campos obrigatórios"""
        # Realizar transformação completa
        df_joined = df_movimento.join(
            df_cartao,
            df_movimento.id_cartao == df_cartao.id,
            "inner"
        ).join(
            df_conta,
            df_cartao.id_conta == df_conta.id,
            "inner"
        ).join(
            df_associado,
            df_cartao.id_associado == df_associado.id,
            "inner"
        )
        
        df_transformed = df_joined.select(
            df_associado.nome.alias("nome_associado"),
            df_movimento.vlr_transacao.cast("string").alias("vlr_transacao_movimento"),
            df_cartao.num_cartao.alias("numero_cartao")
        )
        
        # Verificar nulos
        null_counts = df_transformed.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in df_transformed.columns
        ]).collect()[0]
        
        # Não deve haver nulos
        for col_name in df_transformed.columns:
            assert null_counts[col_name] == 0
    
    def test_unique_movimento_ids(self, df_movimento):
        """Testa se IDs de movimento são únicos"""
        total_count = df_movimento.count()
        distinct_count = df_movimento.select("id").distinct().count()
        
        assert total_count == distinct_count
    
    def test_positive_transaction_values(self, df_movimento):
        """Testa se valores de transação são positivos"""
        negative_count = df_movimento.filter(
            F.col("vlr_transacao") <= 0
        ).count()
        
        assert negative_count == 0
