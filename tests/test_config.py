"""
Testes para o módulo config.py
Valida configurações e métodos de configuração
"""

import pytest
import os
from config import Config


class TestConfig:
    """Testes para a classe Config"""
    
    def test_mysql_jdbc_url_format(self):
        """Testa se a URL JDBC está no formato correto"""
        url = Config.get_mysql_jdbc_url()
        
        assert url.startswith("jdbc:mysql://")
        assert Config.MYSQL_HOST in url
        assert str(Config.MYSQL_PORT) in url
        assert Config.MYSQL_DATABASE in url
        assert "useSSL=false" in url
    
    def test_mysql_properties_has_required_keys(self):
        """Testa se as propriedades JDBC contêm as chaves necessárias"""
        props = Config.get_mysql_properties()
        
        assert "user" in props
        assert "password" in props
        assert "driver" in props
        assert props["driver"] == "com.mysql.cj.jdbc.Driver"
    
    def test_spark_config_has_required_keys(self):
        """Testa se as configurações do Spark contêm as chaves necessárias"""
        config = Config.get_spark_config()
        
        assert "spark.app.name" in config
        assert "spark.master" in config
        assert "spark.sql.adaptive.enabled" in config
    
    def test_output_path_construction(self):
        """Testa se o caminho de output é construído corretamente"""
        path = Config.get_output_path()
        
        assert Config.OUTPUT_DIR in path
        assert Config.OUTPUT_FILENAME in path
    
    def test_output_columns_count(self):
        """Testa se a lista de colunas de output tem 11 elementos"""
        assert len(Config.OUTPUT_COLUMNS) == 11
    
    def test_output_columns_names(self):
        """Testa se as colunas de output estão corretas"""
        expected_columns = [
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
        
        assert Config.OUTPUT_COLUMNS == expected_columns
    
    def test_table_names_are_strings(self):
        """Testa se os nomes das tabelas são strings"""
        assert isinstance(Config.TABLE_ASSOCIADO, str)
        assert isinstance(Config.TABLE_CONTA, str)
        assert isinstance(Config.TABLE_CARTAO, str)
        assert isinstance(Config.TABLE_MOVIMENTO, str)
    
    def test_validate_with_valid_config(self):
        """Testa validação com configurações válidas"""
        # Deve passar sem exceções
        try:
            Config.validate()
        except ValueError:
            pytest.fail("validate() levantou ValueError com configurações válidas")
    
    def test_validate_with_missing_config(self):
        """Testa validação com configurações faltando"""
        # Salvar valor original
        original_host = Config.MYSQL_HOST
        
        try:
            # Simular configuração faltando
            Config.MYSQL_HOST = ""
            
            # Deve levantar ValueError
            with pytest.raises(ValueError, match="Configurações obrigatórias faltando"):
                Config.validate()
        finally:
            # Restaurar valor original
            Config.MYSQL_HOST = original_host
    
    def test_log_level_is_valid(self):
        """Testa se o nível de log é válido"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        assert Config.LOG_LEVEL in valid_levels
    
    def test_output_mode_is_valid(self):
        """Testa se o modo de output é válido"""
        valid_modes = ["overwrite", "append"]
        assert Config.OUTPUT_MODE in valid_modes
    
    def test_output_header_is_boolean(self):
        """Testa se OUTPUT_HEADER é booleano"""
        assert isinstance(Config.OUTPUT_HEADER, bool)
    
    def test_mysql_port_is_integer(self):
        """Testa se a porta do MySQL é um inteiro"""
        assert isinstance(Config.MYSQL_PORT, int)
        assert Config.MYSQL_PORT > 0
        assert Config.MYSQL_PORT <= 65535
