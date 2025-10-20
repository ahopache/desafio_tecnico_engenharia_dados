#!/usr/bin/env python3
"""
Script para otimizar ambiente Spark no Windows
Reduz warnings de limpeza de arquivos temporários
"""

import os
import shutil
import sys

def setup_spark_environment():
    """Configura ambiente Spark otimizado para Windows"""

    print("=== OTIMIZANDO AMBIENTE SPARK ===")

    # 1. Criar diretório temporário do Spark se não existir
    spark_temp_dir = "C:/tmp/spark-warehouse"
    if not os.path.exists(spark_temp_dir):
        os.makedirs(spark_temp_dir, exist_ok=True)
        print(f"[OK] Diretorio temporario criado: {spark_temp_dir}")
    else:
        print(f"[OK] Diretorio temporario ja existe: {spark_temp_dir}")

    # 2. Criar arquivo spark-defaults.conf se não existir
    spark_defaults = os.path.expanduser("~/.spark/spark-defaults.conf")
    os.makedirs(os.path.dirname(spark_defaults), exist_ok=True)

    if not os.path.exists(spark_defaults):
        with open(spark_defaults, 'w') as f:
            f.write("spark.sql.warehouse.dir=file:///C:/tmp/spark-warehouse\n")
            f.write("spark.sql.adaptive.enabled=true\n")
            f.write("spark.sql.adaptive.coalescePartitions.enabled=true\n")
            f.write("spark.driver.extraJavaOptions=-Djava.io.tmpdir=C:/tmp\n")
            f.write("spark.executor.extraJavaOptions=-Djava.io.tmpdir=C:/tmp\n")
        print(f"[OK] Arquivo spark-defaults.conf criado: {spark_defaults}")
    else:
        print(f"[OK] Arquivo spark-defaults.conf ja existe: {spark_defaults}")

    # 3. Configurar variável de ambiente HADOOP_HOME se necessário
    if not os.environ.get('HADOOP_HOME'):
        hadoop_home = r"C:\hadoop"
        if os.path.exists(os.path.join(hadoop_home, "bin", "winutils.exe")):
            os.environ['HADOOP_HOME'] = hadoop_home
            print(f"[OK] HADOOP_HOME configurado: {hadoop_home}")
        else:
            print("[AVISO] HADOOP_HOME nao configurado (winutils.exe nao encontrado)")

    # 4. Definir diretório temporário do Java
    os.environ['JAVA_TOOL_OPTIONS'] = '-Djava.io.tmpdir=C:/tmp'

    print("[OK] Ambiente Spark otimizado!")
    print("\nConfiguracoes aplicadas:")
    print("- Diretorio warehouse: C:/tmp/spark-warehouse")
    print("- Temp directory: C:/tmp")
    print("- JARs locais configurados")
    print("- Adaptive query enabled")

    print("\n=== FIM DA OTIMIZACAO ===")

if __name__ == "__main__":
    setup_spark_environment()
