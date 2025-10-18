"""
Testes para o módulo utils.py
Valida funções auxiliares e utilitários
"""

import pytest
import logging
from pyspark.sql import DataFrame
from utils import (
    setup_logger,
    validate_dataframe,
    format_duration,
    ValidationException
)


class TestLogger:
    """Testes para setup_logger"""
    
    def test_setup_logger_returns_logger(self):
        """Testa se setup_logger retorna um Logger"""
        logger = setup_logger("test_logger")
        assert isinstance(logger, logging.Logger)
    
    def test_setup_logger_with_custom_level(self):
        """Testa se o nível de log customizado é aplicado"""
        logger = setup_logger("test_logger_debug", level="DEBUG")
        assert logger.level == logging.DEBUG
        
        logger = setup_logger("test_logger_error", level="ERROR")
        assert logger.level == logging.ERROR
    
    def test_setup_logger_has_handlers(self):
        """Testa se o logger tem handlers configurados"""
        logger = setup_logger("test_logger_handlers")
        assert len(logger.handlers) > 0


class TestValidateDataFrame:
    """Testes para validate_dataframe"""
    
    def test_validate_dataframe_with_valid_df(self, spark):
        """Testa validação com DataFrame válido"""
        df = spark.createDataFrame([(1, "test"), (2, "test2")], ["id", "name"])
        
        # Não deve levantar exceção
        validate_dataframe(df, "test_df", min_rows=1)
    
    def test_validate_dataframe_with_insufficient_rows(self, spark):
        """Testa validação com DataFrame com poucas linhas"""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])
        
        with pytest.raises(ValueError, match="tem apenas 1 linhas"):
            validate_dataframe(df, "test_df", min_rows=5)
    
    def test_validate_dataframe_with_missing_columns(self, spark):
        """Testa validação com colunas faltando"""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])
        
        with pytest.raises(ValueError, match="está faltando colunas"):
            validate_dataframe(df, "test_df", required_columns=["id", "name", "age"])
    
    def test_validate_dataframe_with_all_required_columns(self, spark):
        """Testa validação com todas as colunas necessárias"""
        df = spark.createDataFrame(
            [(1, "test", 25)], 
            ["id", "name", "age"]
        )
        
        # Não deve levantar exceção
        validate_dataframe(
            df, 
            "test_df", 
            required_columns=["id", "name", "age"]
        )


class TestFormatDuration:
    """Testes para format_duration"""
    
    def test_format_duration_seconds(self):
        """Testa formatação de duração em segundos"""
        assert format_duration(45.5) == "45.50s"
        assert format_duration(5.123) == "5.12s"
    
    def test_format_duration_minutes(self):
        """Testa formatação de duração em minutos"""
        result = format_duration(125.5)  # 2 minutos e 5.5 segundos
        assert "2m" in result
        assert "5.50s" in result
    
    def test_format_duration_hours(self):
        """Testa formatação de duração em horas"""
        result = format_duration(7325)  # 2 horas, 2 minutos e 5 segundos
        assert "2h" in result
        assert "2m" in result
        assert "5.00s" in result
    
    def test_format_duration_zero(self):
        """Testa formatação de duração zero"""
        assert format_duration(0) == "0.00s"
    
    def test_format_duration_negative(self):
        """Testa formatação de duração negativa"""
        # Deve funcionar mesmo com valores negativos
        result = format_duration(-10)
        assert "s" in result


class TestExceptions:
    """Testes para exceções customizadas"""
    
    def test_validation_exception_can_be_raised(self):
        """Testa se ValidationException pode ser levantada"""
        with pytest.raises(ValidationException):
            raise ValidationException("Test error")
    
    def test_validation_exception_message(self):
        """Testa se a mensagem da exceção é preservada"""
        error_msg = "Custom validation error"
        
        with pytest.raises(ValidationException, match=error_msg):
            raise ValidationException(error_msg)
