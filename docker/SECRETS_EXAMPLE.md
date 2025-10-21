# ============================================================================
# Arquivo de Exemplo para Secrets - SiCooperative Data Lake POC
# ============================================================================
# IMPORTANTE: Este arquivo demonstra como configurar os secrets para desenvolvimento
# NUNCA commite arquivos reais de secrets no repositório!
#
# Para desenvolvimento local:
# 1. Crie o diretório docker/secrets/
# 2. Configure senhas seguras conforme abaixo
# 3. Execute: chmod 600 docker/secrets/mysql_*_password
# ============================================================================

# ----------------------------------------------------------------------------
# PASSOS PARA CONFIGURAR SECRETS
# ----------------------------------------------------------------------------

# 1. Criar diretório:
# mkdir -p docker/secrets

# 2. Criar arquivos de senha:
# echo "sua_senha_segura_root" > docker/secrets/mysql_root_password
# echo "sua_senha_segura_usuario" > docker/secrets/mysql_password

# 3. Definir permissões:
# chmod 600 docker/secrets/mysql_root_password
# chmod 600 docker/secrets/mysql_password

# 4. Para desenvolvimento rápido:
# echo "root_password" > docker/secrets/mysql_root_password
# echo "etl_password" > docker/secrets/mysql_password

# ============================================================================
# EXEMPLOS DE SENHAS SEGURAS
# ============================================================================

# Geração de senha segura:
# openssl rand -hex 32
# pwgen -s 32 1

# Exemplo: 8f4a2c9e7d5b1f8c3e6a9d4b7c2e5f1a8b3d6c9e2f5a8c1d4g7h2j5k8l1m4n7p2q5r8s1t4u7v2w5x8y1z4

# ============================================================================
# VALIDAÇÃO DE SEGURANÇA
# ============================================================================

# Verificar permissões:
# ls -la docker/secrets/
# stat docker/secrets/mysql_root_password

# ============================================================================
