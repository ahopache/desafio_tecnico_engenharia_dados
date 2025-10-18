"""
Script de inicialização de dados de teste
Popula o banco MySQL com dados de teste para execução dos testes de integração
"""

import mysql.connector
import os
import sys
from pathlib import Path
import random
from datetime import datetime, timedelta

# Configurações
PROJECT_ROOT = Path(__file__).parent

def create_test_data():
    """Cria dados de teste no banco MySQL"""

    # Configurações de conexão (sem banco específico inicialmente)
    config_no_db = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'user': os.getenv('MYSQL_USER', 'test_user'),
        'password': os.getenv('MYSQL_PASS', 'test_password')
    }

    config_with_db = config_no_db.copy()
    database_name = os.getenv('MYSQL_DATABASE', 'sicooperative_db')
    config_with_db['database'] = database_name

    conn = None
    cursor = None

    try:
        # 1. Conectar sem banco específico para criar o banco
        print("🔗 Conectando ao MySQL para criar banco de teste...")
        conn = mysql.connector.connect(**config_no_db)
        cursor = conn.cursor()

        # 2. Criar banco de dados se não existir
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"✅ Banco de dados '{database_name}' criado/verificado")

        # 3. Fechar conexão e reconectar com o banco específico
        conn.close()
        conn = None

        print("🔗 Reconectando com banco específico...")
        conn = mysql.connector.connect(**config_with_db)
        cursor = conn.cursor()

        print("🔗 Conectado ao banco de teste")

        # Verificar se já existem dados
        try:
            cursor.execute("SELECT COUNT(*) FROM associado")
            existing_count = cursor.fetchone()[0]
        except mysql.connector.Error as e:
            print(f"⚠️ Erro ao verificar dados existentes: {e}")
            existing_count = 0

        if existing_count > 0:
            print(f"⚠️ Banco já contém {existing_count} associados. Pulando inserção de dados.")
            show_statistics(cursor)
            return

        # Criar tabelas (se não existirem)
        create_tables(cursor)

        # Inserir dados de teste
        insert_test_data(cursor)

        # Commit das mudanças
        conn.commit()

        print("✅ Dados de teste inseridos com sucesso")
        # Estatísticas
        show_statistics(cursor)

    except mysql.connector.Error as e:
        print(f"❌ Erro no banco de dados: {e}")
        if "Unknown database" in str(e):
            print("💡 Dica: Certifique-se de que o banco de dados existe ou execute o script SQL de inicialização primeiro")
        raise

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("🔌 Conexão fechada")


def create_tables(cursor):
    """Cria tabelas necessárias para teste"""
    print("🏗️ Criando tabelas de teste com base no arquivo SQL...")

    with open(PROJECT_ROOT / "sql/01_create_schema.sql", "r", encoding="utf-8") as f:
        sql_script = f.read()

    # Itera sobre o resultado de execute() para garantir que todas as instruções sejam executadas
    for _ in cursor.execute(sql_script, multi=True):
        pass # Apenas itera, não precisa fazer nada com o resultado

    print("✅ Tabelas criadas")


def insert_test_data(cursor):
    """Insere dados de teste realistas"""
    print("📝 Inserindo dados de teste com base no arquivo SQL...")

    # Dados base
    with open(PROJECT_ROOT / "sql/02_insert_data.sql", "r", encoding='utf-8') as f:
        sql_script = f.read()

    # Itera sobre o resultado de execute() para garantir que todas as instruções sejam executadas
    for _ in cursor.execute(sql_script, multi=True):
        pass # Apenas itera, não precisa fazer nada com o resultado

    print("✅ Dados inseridos")

def show_statistics(cursor):
    """Mostra estatísticas dos dados inseridos"""
    print("📊 Estatísticas dos dados de teste:")

    tables = ["associado", "conta", "cartao", "movimento"]

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   • {table}: {count} registros")

    # Estatísticas adicionais
    cursor.execute("""
        SELECT
            COUNT(DISTINCT c.tipo) as tipos_conta,
            AVG(a.idade) as idade_media,
            MIN(m.vlr_transacao) as menor_transacao,
            MAX(m.vlr_transacao) as maior_transacao,
            SUM(m.vlr_transacao) as volume_total
        FROM movimento m
        JOIN cartao ca ON m.id_cartao = ca.id
        JOIN conta c ON ca.id_conta = c.id
        JOIN associado a ON ca.id_associado = a.id
    """)

    stats = cursor.fetchone()
    print(f"   • Tipos de conta únicos: {stats[0]}")
    print(f"   • Idade média dos associados: {stats[1]:.1f} anos")
    print(f"   • Menor transação: R$ {stats[2]:.2f}")
    print(f"   • Maior transação: R$ {stats[3]:.2f}")
    print(f"   • Volume total: R$ {stats[4]:.2f}")


def drop_test_data():
    """Remove todos os dados do banco de teste"""
    print("🗑️ Removendo dados de teste...")

    # Configurações de conexão (sem banco específico inicialmente)
    config_no_db = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'user': os.getenv('MYSQL_USER', 'test_user'),
        'password': os.getenv('MYSQL_PASS', 'test_password')
    }

    config_with_db = config_no_db.copy()
    database_name = os.getenv('MYSQL_DATABASE', 'sicooperative_db')
    config_with_db['database'] = database_name

    conn = None
    cursor = None

    try:
        # 1. Conectar sem banco específico para criar o banco
        print("🔗 Conectando ao MySQL para criar banco de teste...")
        conn = mysql.connector.connect(**config_no_db)
        cursor = conn.cursor()

        # 2. Remover banco de dados se existir
        cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
        print(f"✅ Banco de dados '{database_name}' removido/verificado")

    except mysql.connector.Error as e:
        print(f"❌ Erro no banco de dados: {e}")
        if "Unknown database" in str(e):
            print("💡 Dica: Certifique-se de que o banco de dados existe ou execute o script SQL de inicialização primeiro")
        raise

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("🔌 Conexão fechada")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        for param in sys.argv[1:]:
            if param == "-h" or param == "--help":
                print("Usage: python setup_test_data.py --recreate")
                sys.exit(0)
            elif param == "--recreate":
                drop_test_data()

    create_test_data()
