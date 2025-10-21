#!/usr/bin/env python3
"""
Script de configuração automática do HADOOP_HOME para Windows
Resolve o problema do winutils.exe necessário para Spark no Windows
"""

import os
import sys
import urllib.request
import zipfile
from pathlib import Path


def setup_hadoop_windows():
    """Configura HADOOP_HOME automaticamente no Windows"""

    if not sys.platform.startswith('win'):
        print("ℹ️ Sistema não é Windows. HADOOP_HOME não é necessário.")
        return True

    hadoop_home = r"C:\hadoop"
    bin_dir = os.path.join(hadoop_home, "bin")
    winutils_path = os.path.join(bin_dir, "winutils.exe")

    # Verificar se já existe
    if os.path.exists(winutils_path):
        print(f"✅ winutils.exe já existe em {winutils_path}")
        os.environ['HADOOP_HOME'] = hadoop_home
        return True

    print(f"🔧 Configurando HADOOP_HOME em {hadoop_home}...")

    try:
        # Criar diretórios
        os.makedirs(bin_dir, exist_ok=True)

        # Baixar winutils.exe do Hadoop 3.2.1
        winutils_url = "https://github.com/steveloughran/winutils/raw/master/hadoop-3.2.1/bin/winutils.exe"
        print(f"⬇️ Baixando winutils.exe de {winutils_url}...")

        urllib.request.urlretrieve(winutils_url, winutils_path)

        if os.path.exists(winutils_path):
            print(f"✅ winutils.exe baixado com sucesso em {winutils_path}")
            os.environ['HADOOP_HOME'] = hadoop_home

            # Configurar variável de ambiente permanentemente (se possível)
            try:
                # Tentar usar setx para configuração permanente
                os.system(f'setx HADOOP_HOME "{hadoop_home}" /M >nul 2>&1')
                print(f"✅ HADOOP_HOME configurado permanentemente: {hadoop_home}")
            except:
                print(f"⚠️ Configure HADOOP_HOME manualmente: {hadoop_home}")

            return True
        else:
            print("❌ Falha ao baixar winutils.exe")
            return False

    except Exception as e:
        print(f"❌ Erro ao configurar HADOOP_HOME: {e}")
        return False


def check_hadoop_setup():
    """Verifica se HADOOP_HOME está configurado corretamente"""

    hadoop_home = os.environ.get('HADOOP_HOME')

    if not hadoop_home:
        print("❌ HADOOP_HOME não está configurado")
        return False

    winutils_path = os.path.join(hadoop_home, "bin", "winutils.exe")

    if not os.path.exists(winutils_path):
        print(f"❌ winutils.exe não encontrado em {winutils_path}")
        return False

    print(f"✅ HADOOP_HOME configurado corretamente: {hadoop_home}")
    return True


if __name__ == "__main__":
    print("🔧 Configurando ambiente Hadoop para Windows...")

    if setup_hadoop_windows():
        print("✅ Configuração concluída!")
        print("")
        print("📋 Resumo:")
        print(f"   HADOOP_HOME: {os.environ.get('HADOOP_HOME', 'Não configurado')}")
        # print("   Sistema: Windows"
        print("   Status: Pronto para Spark")
        print("")
        print("💡 Para usar em outros terminais, execute:")
        print("   set HADOOP_HOME=C:\\hadoop")
    else:
        print("❌ Falha na configuração automática")
        print("")
        print("🔧 Solução manual:")
        print("1. Crie a pasta C:\\hadoop\\bin")
        print("2. Baixe winutils.exe de: https://github.com/steveloughran/winutils")
        print("3. Coloque o arquivo em C:\\hadoop\\bin\\winutils.exe")
        print("4. Configure a variável: set HADOOP_HOME=C:\\hadoop")
