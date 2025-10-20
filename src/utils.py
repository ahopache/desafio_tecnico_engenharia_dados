"""
Módulo de Utilidades
Funções auxiliares para logging, validação e operações comuns
"""

import logging
import sys
import re
from typing import Optional
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, functions as F

from config import Config


def setup_logger(name: str = __name__, level: Optional[str] = None) -> logging.Logger:
    """
    Configura e retorna um logger
    
    Args:
        name: Nome do logger
        level: Nível de log (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        logging.Logger: Logger configurado
    """
    log_level = level or Config.LOG_LEVEL
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Evitar duplicação de handlers
    if not logger.handlers:
        # Handler para console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Formatter
        formatter = logging.Formatter(
            Config.LOG_FORMAT,
            datefmt=Config.LOG_DATE_FORMAT
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    
    return logger


def validate_mysql_connection(spark: SparkSession) -> bool:
    """
    Valida a conexão com o MySQL
    
    Args:
        spark: Sessão Spark ativa
    
    Returns:
        bool: True se a conexão foi bem-sucedida
    
    Raises:
        Exception: Se não conseguir conectar ao MySQL
    """
    logger = setup_logger(__name__)
    
    try:
        logger.info("Validando conexão com MySQL...")
        
        # Tenta ler uma tabela simples
        test_query = "(SELECT 1 AS test) AS test_table"
        
        df = spark.read \
            .format("jdbc") \
            .option("url", Config.get_mysql_jdbc_url()) \
            .option("dbtable", test_query) \
            .option("user", Config.MYSQL_USER) \
            .option("password", Config.MYSQL_PASSWORD) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        
        # Força a execução da query
        count = df.count()
        
        if count == 1:
            logger.info("✓ Conexão com MySQL validada com sucesso!")
            return True
        else:
            raise Exception("Resultado inesperado na validação")
            
    except Exception as e:
        logger.error(f"✗ Erro ao conectar ao MySQL: {str(e)}")
        raise


def validate_dataframe(
    df: DataFrame,
    name: str,
    min_rows: int = 1,
    required_columns: Optional[list] = None
) -> None:
    """
    Valida um DataFrame
    
    Args:
        df: DataFrame a ser validado
        name: Nome do DataFrame (para logging)
        min_rows: Número mínimo de linhas esperado
        required_columns: Lista de colunas obrigatórias
    
    Raises:
        ValueError: Se a validação falhar
    """
    logger = setup_logger(__name__)
    
    # Validar se DataFrame não está vazio
    count = df.count()
    if count < min_rows:
        raise ValueError(
            f"DataFrame '{name}' tem apenas {count} linhas "
            f"(mínimo esperado: {min_rows})"
        )
    
    # Validar colunas obrigatórias
    if required_columns:
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(
                f"DataFrame '{name}' está faltando colunas: {missing_cols}"
            )
    
    logger.info(f"✓ DataFrame '{name}' validado: {count} linhas, {len(df.columns)} colunas")


def log_dataframe_info(df: DataFrame, name: str, sample_rows: int = 5) -> None:
    """
    Loga informações sobre um DataFrame
    
    Args:
        df: DataFrame
        name: Nome do DataFrame
        sample_rows: Número de linhas para mostrar no sample
    """
    logger = setup_logger(__name__)
    
    logger.info(f"\n{'=' * 70}")
    logger.info(f"DataFrame: {name}")
    logger.info(f"{'=' * 70}")
    logger.info(f"Total de linhas: {df.count()}")
    logger.info(f"Total de colunas: {len(df.columns)}")
    logger.info(f"\nSchema:")
    df.printSchema()
    logger.info(f"\nAmostra ({sample_rows} linhas):")
    df.show(sample_rows, truncate=False)
    logger.info(f"{'=' * 70}\n")


def format_duration(seconds: float) -> str:
    """
    Formata duração em segundos para formato legível
    
    Args:
        seconds: Duração em segundos
    
    Returns:
        str: Duração formatada (ex: "2m 30s")
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.2f}s"


def create_output_directory(path: str) -> None:
    """
    Cria o diretório de saída se não existir
    
    Args:
        path: Caminho do diretório
    """
    import os
    
    logger = setup_logger(__name__)
    
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logger.info(f"✓ Diretório de saída criado: {path}")
    else:
        logger.info(f"✓ Diretório de saída já existe: {path}")


def get_timestamp() -> str:
    """
    Retorna timestamp atual formatado
    
    Returns:
        str: Timestamp no formato YYYY-MM-DD HH:MM:SS
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_banner(message: str, char: str = "=") -> None:
    """
    Imprime um banner formatado
    
    Args:
        message: Mensagem a ser exibida
        char: Caractere para a borda
    """
    logger = setup_logger(__name__)
    width = 70
    
    logger.info(f"\n{char * width}")
    logger.info(f"{message.center(width)}")
    logger.info(f"{char * width}\n")


def print_statistics(stats: dict) -> None:
    """
    Imprime estatísticas finais do pipeline

    Args:
        stats: Dicionário com estatísticas do pipeline
    """
    print("=" * 70)
    print("📊 ESTATÍSTICAS DO PIPELINE")
    print("=" * 70)

    print(f"⏱️  Duração total: {stats.get('duracao', 'N/A')}")
    print(f"📥 Registros extraídos:")
    print(f"   • Associados: {stats.get('registros_associado', 0):,}","g")
    print(f"   • Contas: {stats.get('registros_conta', 0):,}","g")
    print(f"   • Cartões: {stats.get('registros_cartao', 0):,}","g")
    print(f"   • Movimentos: {stats.get('registros_movimento', 0):,}","g")
    print(f"📤 Registros finais: {stats.get('registros_final', 0):,}","g")

    # Estatísticas de qualidade de dados (se disponíveis)
    if 'quality_checks' in stats:
        quality_stats = stats['quality_checks']
        print(f"🔍 Verificações de qualidade:")
        print(f"   • Total: {quality_stats.get('total', 0)}")
        print(f"   • Aprovadas: {quality_stats.get('passed', 0)}")
        print(f"   • Avisos: {quality_stats.get('warnings', 0)}")
        print(f"   • Rejeições: {quality_stats.get('failed', 0)}")

    print("=" * 70)


class ETLException(Exception):
    """Exceção customizada para erros do pipeline ETL"""
    pass


class ValidationException(Exception):
    """Exceção customizada para erros de validação"""
    pass


def mask_credit_card(card_number_col):
    """
    Mascara um número de cartão, mantendo apenas os 6 primeiros e 4 últimos dígitos
    
    Args:
        card_number_col: Coluna com o número do cartão
        
    Returns:
        Coluna Spark com o número do cartão mascarado
    """
    return F.when(
        F.length(card_number_col) >= 10,
        F.concat(
            F.substring(card_number_col, 1, 6),
            F.lit('******'),
            F.substring(card_number_col, -4, 4)
        )
    ).otherwise('******' + F.substring(card_number_col, -4, 4))


def hash_sensitive_data(column, salt=Config.HASH_SALT):
    """
    Gera um hash SHA-256 de uma coluna com salt para anonimização IRREVERSÍVEL
    
    IMPORTANTE - IRREVERSÍVEL:
    - Usa algoritmo criptográfico SHA-256 que NÃO permite recuperação do valor original
    - Útil para auditoria, análise e conformidade com leis de privacidade (LGPD/GDPR)
    - NÃO é possível "descriptografar" ou reverter o hash para obter dados originais
    - Cada execução gera o mesmo hash para o mesmo input (determinístico)
    
    Args:
        column: Coluna a ser hasheada
        salt: String de salt para aumentar a segurança do hash
        
    Returns:
        Coluna Spark com o valor hasheado (string hexadecimal de 64 caracteres)
        
    Exemplo:
        Entrada: "1234567890123456"
        Salt: "s1c00p3r4t1v3_s3cur3_s4lt"
        Saída: "a1b2c3d4e5f6..." (64 caracteres hexadecimais)
    """
    # Converte para string e concatena com o salt
    salted_value = F.concat(F.coalesce(column.cast("string"), F.lit("")), F.lit(salt))

    # Gera o hash SHA-256
    return F.sha2(salted_value, 256)
    
def validate_pii_masking(df: DataFrame, logger=None) -> None:
    """
    Valida que dados sensíveis (PII) estão adequadamente mascarados
    
    Args:
        df: DataFrame a ser validado
        logger: Logger opcional para mensagens
        
    Raises:
        ValidationException: Se dados sensíveis não estiverem mascarados
    """
    if logger is None:
        logger = setup_logger(__name__)
    
    try:
        logger.info("🔒 Validando mascaramento de dados sensíveis (PII)...")
        
        # Verificar se coluna numero_cartao_masked existe
        if "numero_cartao_masked" not in df.columns:
            raise ValidationException("Coluna numero_cartao_masked não encontrada no DataFrame")
        
        # Amostrar alguns valores para validação
        sample_df = df.select("numero_cartao_masked").limit(10)
        
        for row in sample_df.collect():
            masked_card = row["numero_cartao_masked"]
            
            if masked_card:
                # Verificar se contém apenas dígitos e asteriscos
                if not all(c.isdigit() or c == '*' for c in str(masked_card)):
                    raise ValidationException(
                        f"Número de cartão mascarado contém caracteres inválidos: {masked_card}"
                    )
                
                # Verificar se tem exatamente 6 dígitos iniciais + 6 asteriscos + 4 dígitos finais
                expected_length = 16  # 6 + 6 + 4
                if len(str(masked_card)) != expected_length:
                    raise ValidationException(
                        f"Número de cartão mascarado tem comprimento incorreto: {masked_card} "
                        f"(esperado: {expected_length}, obtido: {len(str(masked_card))})"
                    )
                
                # Verificar se os primeiros 6 são dígitos
                first_six = str(masked_card)[:6]
                if not first_six.isdigit():
                    raise ValidationException(
                        f"Primeiros 6 dígitos do cartão mascarado não são válidos: {first_six}"
                    )
                
                # Verificar se há asteriscos no meio (posições 7-12)
                middle = str(masked_card)[6:12]
                if middle != "******":
                    raise ValidationException(
                        f"Parte mascarada do cartão não está correta: {middle} "
                        f"(esperado: ******)"
                    )
                
                # Verificar se os últimos 4 são dígitos
                last_four = str(masked_card)[-4:]
                if not last_four.isdigit():
                    raise ValidationException(
                        f"Últimos 4 dígitos do cartão mascarado não são válidos: {last_four}"
                    )
        
        logger.info("✓ Mascaramento de números de cartão validado com sucesso")
        
        # Verificar hashes de dados sensíveis
        if "numero_cartao_hash" in df.columns:
            hash_sample = df.select("numero_cartao_hash").limit(5)
            for row in hash_sample.collect():
                hash_value = row["numero_cartao_hash"]
                if hash_value:
                    # Verificar se é hash SHA-256 (64 caracteres hexadecimais)
                    if not (len(str(hash_value)) == 64 and all(c in '0123456789abcdefABCDEF' for c in str(hash_value))):
                        raise ValidationException(
                            f"Hash do número do cartão não está no formato correto: {hash_value}"
                        )
            
            logger.info("✓ Hash SHA-256 de números de cartão validado com sucesso")
        
        if "email_hash" in df.columns:
            email_hash_sample = df.select("email_hash").limit(5)
            for row in email_hash_sample.collect():
                hash_value = row["email_hash"]
                if hash_value:
                    # Verificar se é hash SHA-256 (64 caracteres hexadecimais)
                    if not (len(str(hash_value)) == 64 and all(c in '0123456789abcdefABCDEF' for c in str(hash_value))):
                        raise ValidationException(
                            f"Hash do email não está no formato correto: {hash_value}"
                        )
            
            logger.info("✓ Hash SHA-256 de emails validado com sucesso")
        
        logger.info("✅ Validação completa de PII: todos os dados sensíveis estão adequadamente mascarados")
        
    except Exception as e:
        logger.error(f"✗ Erro na validação de PII: {str(e)}")
        raise ValidationException(f"Falha na validação de mascaramento PII: {str(e)}")
        
def validate_no_full_pan_in_output(df: DataFrame, logger=None) -> None:
    """
    Verificação adicional: garante que NÃO há números de cartão completos (16 dígitos) no output

    Esta é uma camada extra de segurança para detectar vazamentos acidentais de PAN.

    Args:
        df: DataFrame a ser verificado
        logger: Logger opcional

    Raises:
        ValidationException: Se números de cartão completos forem encontrados
    """
    if logger is None:
        logger = setup_logger(__name__)

    try:
        logger.info("🔍 Verificação adicional: buscando números de cartão completos no output...")

        # Regex para detectar padrões de 16 dígitos seguidos
        # Isso pode aparecer em qualquer coluna de string
        string_columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']

        if not string_columns:
            logger.info("✓ Nenhuma coluna de string encontrada - verificação não aplicável")
            return

        # Amostrar dados para verificação (limitar para performance)
        sample_df = df.select(*string_columns).limit(100)  # Amostra de 100 linhas

        pan_pattern = r'\b\d{16}\b'  # 16 dígitos seguidos

        for col in string_columns:
            for row in sample_df.collect():
                value = str(row[col]) if row[col] is not None else ""

                # Verificar se há 16 dígitos seguidos (PAN completo)
                if re.search(pan_pattern, value):
                    raise ValidationException(
                        f"🚨 VAZAMENTO DETECTADO! Número de cartão completo encontrado "
                        f"na coluna '{col}': {value[:50]}..."
                    )

        logger.info("✅ Verificação adicional: nenhum número de cartão completo encontrado no output")

    except Exception as e:
        logger.error(f"✗ Erro na verificação de PAN completo: {str(e)}")
        raise ValidationException(f"Falha na verificação de segurança PAN: {str(e)}")
