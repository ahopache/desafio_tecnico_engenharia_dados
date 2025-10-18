"""
Gerador de Dados Fictícios com Faker
Gera dados realistas para popular o banco de dados MySQL
"""

from faker import Faker
from datetime import datetime, timedelta
import random
import sys

# Configurar Faker para português do Brasil
fake = Faker('pt_BR')
Faker.seed(42)  # Seed para reprodutibilidade
random.seed(42)


def generate_associados(quantidade=100):
    """
    Gera dados de associados (clientes)
    
    Args:
        quantidade: Número de associados a gerar
    
    Returns:
        list: Lista de tuplas (nome, sobrenome, idade, email)
    """
    associados = []
    emails_usados = set()
    
    for i in range(1, quantidade + 1):
        # Gerar nome e sobrenome
        nome = fake.first_name()
        sobrenome = fake.last_name()
        
        # Gerar idade (18 a 80 anos)
        idade = random.randint(18, 80)
        
        # Gerar email único
        base_email = f"{nome.lower()}.{sobrenome.lower()}".replace(" ", "")
        email = f"{base_email}@email.com"
        
        # Garantir unicidade do email
        contador = 1
        while email in emails_usados:
            email = f"{base_email}{contador}@email.com"
            contador += 1
        
        emails_usados.add(email)
        
        associados.append((nome, sobrenome, idade, email))
    
    return associados


def generate_contas(associados, media_contas_por_associado=2):
    """
    Gera dados de contas bancárias
    
    Args:
        associados: Lista de associados
        media_contas_por_associado: Média de contas por associado
    
    Returns:
        list: Lista de tuplas (tipo, data_criacao, id_associado, saldo_atual)
    """
    contas = []
    tipos_conta = ['corrente', 'poupanca', 'investimento']
    
    for idx, associado in enumerate(associados, 1):
        # Cada associado tem 1 ou 2 contas
        num_contas = random.choices(
            [media_contas_por_associado-1,
             media_contas_por_associado,
             media_contas_por_associado+1], weights=[20, 70, 10])[0]
        
        for _ in range(num_contas):
            tipo = random.choice(tipos_conta)
            
            # Data de criação nos últimos 5 anos
            dias_atras = random.randint(0, 365 * 5)
            data_criacao = datetime.now() - timedelta(days=dias_atras)
            data_criacao_str = data_criacao.strftime('%Y-%m-%d %H:%M:%S')
            
            # Saldo atual
            if tipo == 'corrente':
                saldo = round(random.uniform(500, 15000), 2)
            elif tipo == 'poupanca':
                saldo = round(random.uniform(5000, 50000), 2)
            else:  # investimento
                saldo = round(random.uniform(10000, 100000), 2)
            
            contas.append((tipo, data_criacao_str, idx, saldo))
    
    return contas


def generate_cartoes(contas, associados):
    """
    Gera dados de cartões
    
    Args:
        contas: Lista de contas
        associados: Lista de associados
    
    Returns:
        list: Lista de tuplas (num_cartao, nom_impresso, data_criacao, data_validade, 
                               id_conta, id_associado, tipo_cartao, limite_credito)
    """
    cartoes = []
    tipos_cartao = ['debito', 'credito', 'multiplo']
    numeros_usados = set()
    
    for idx_conta, conta in enumerate(contas, 1):
        id_associado = conta[2]  # id_associado da conta
        associado = associados[id_associado - 1]
        nome_completo = f"{associado[0]} {associado[1]}".upper()
        
        # Cada conta tem 1 ou 2 cartões
        num_cartoes = random.choices([1, 2], weights=[60, 40])[0]
        
        for _ in range(num_cartoes):
            # Gerar número de cartão único
            while True:
                # Prefixos comuns: 4 (Visa), 5 (Mastercard)
                prefixo = random.choice(['4', '5'])
                num_cartao = prefixo + ''.join([str(random.randint(0, 9)) for _ in range(15)])
                if num_cartao not in numeros_usados:
                    numeros_usados.add(num_cartao)
                    break
            
            # Data de criação (após a criação da conta)
            data_conta = datetime.strptime(conta[1], '%Y-%m-%d %H:%M:%S')
            dias_depois = random.randint(1, 30)
            data_criacao = data_conta + timedelta(days=dias_depois)
            data_criacao_str = data_criacao.strftime('%Y-%m-%d %H:%M:%S')
            
            # Data de validade (5 anos após criação)
            data_validade = data_criacao + timedelta(days=365 * 5)
            data_validade_str = data_validade.strftime('%Y-%m-%d')
            
            # Tipo de cartão
            tipo_cartao = random.choice(tipos_cartao)
            
            # Limite de crédito
            if tipo_cartao == 'debito':
                limite = 0.00
            else:
                limite = round(random.uniform(1000, 20000), 2)
            
            cartoes.append((
                num_cartao,
                nome_completo,
                data_criacao_str,
                data_validade_str,
                idx_conta,
                id_associado,
                tipo_cartao,
                limite
            ))
    
    return cartoes


def generate_movimentos(cartoes, num_movimentos_por_cartao=10):
    """
    Gera dados de movimentos (transações)
    
    Args:
        cartoes: Lista de cartões
        num_movimentos_por_cartao: Número médio de movimentos por cartão
    
    Returns:
        list: Lista de tuplas (vlr_transacao, des_transacao, data_movimento, 
                               id_cartao, tipo_movimento, categoria, estabelecimento)
    """
    movimentos = []
    
    # Categorias e estabelecimentos realistas
    categorias_estabelecimentos = {
        'alimentacao': [
            'Zaffari', 'Carrefour', 'Big', 'Nacional', 'Bourbon Supermercados',
            'Restaurante Chalé da Praça XV', 'Restaurante Gambrinus',
            'Padaria Dona Nena', 'Confeitaria Rocco', 'McDonald\'s'
        ],
        'transporte': [
            'Posto Ipiranga', 'Posto Shell', 'Uber', '99 Taxi',
            'Estacionamento Shopping', 'Pedágio Freeway', 'Auto Posto BR'
        ],
        'saude': [
            'Farmácia Panvel', 'Farmácia São João', 'Drogasil',
            'Hospital Moinhos de Vento', 'Clínica Mãe de Deus',
            'Laboratório Weinmann'
        ],
        'educacao': [
            'Livraria Cultura', 'Livraria Saraiva', 'Papelaria Kalunga',
            'Curso de Inglês CNA', 'Universidade PUCRS'
        ],
        'lazer': [
            'Cinemark Bourbon', 'UCI Cinemas', 'Parque da Redenção',
            'Bar Ocidente', 'Opinião Pub', 'Teatro São Pedro'
        ],
        'vestuario': [
            'Renner', 'C&A', 'Riachuelo', 'Zara', 'Marisa',
            'Centauro', 'Nike Store'
        ],
        'moradia': [
            'Leroy Merlin', 'Telhanorte', 'Tok&Stok', 'Lojas Colombo',
            'Magazine Luiza', 'Casas Bahia'
        ],
        'servicos': [
            'Salão de Beleza', 'Barbearia', 'Academia Musculação e CrossFit',
            'Lavanderia Lava Rápido', 'Oficina Mecânica'
        ]
    }
    
    for idx_cartao, cartao in enumerate(cartoes, 1):
        data_cartao = datetime.strptime(cartao[2], '%Y-%m-%d %H:%M:%S')
        
        # Número variável de movimentos
        num_movimentos = random.randint(5, num_movimentos_por_cartao * 2)
        
        for _ in range(num_movimentos):
            # Categoria aleatória
            categoria = random.choice(list(categorias_estabelecimentos.keys()))
            estabelecimento = random.choice(categorias_estabelecimentos[categoria])
            
            # Valor da transação (varia por categoria)
            if categoria == 'alimentacao':
                valor = round(random.uniform(10, 500), 2)
            elif categoria == 'transporte':
                valor = round(random.uniform(20, 300), 2)
            elif categoria == 'saude':
                valor = round(random.uniform(30, 800), 2)
            elif categoria == 'educacao':
                valor = round(random.uniform(50, 2000), 2)
            elif categoria == 'lazer':
                valor = round(random.uniform(20, 400), 2)
            elif categoria == 'vestuario':
                valor = round(random.uniform(50, 1500), 2)
            elif categoria == 'moradia':
                valor = round(random.uniform(100, 3000), 2)
            else:  # servicos
                valor = round(random.uniform(30, 500), 2)
            
            # Data do movimento (após criação do cartão, últimos 365 dias)
            dias_desde_cartao = (datetime.now() - data_cartao).days
            if dias_desde_cartao > 365:
                dias_atras = random.randint(0, 365)
            else:
                dias_atras = random.randint(0, max(1, dias_desde_cartao))
            
            data_movimento = datetime.now() - timedelta(days=dias_atras)
            # Adicionar hora aleatória
            data_movimento = data_movimento.replace(
                hour=random.randint(6, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
            data_movimento_str = data_movimento.strftime('%Y-%m-%d %H:%M:%S')
            
            # Descrição
            descricao = f"Compra em {estabelecimento}"
            
            # Tipo de movimento (maioria débito)
            tipo_movimento = random.choices(
                ['debito', 'credito', 'estorno'],
                weights=[85, 10, 5]
            )[0]
            
            if tipo_movimento == 'estorno':
                valor = -valor
                descricao = f"ESTORNO - {descricao}"
            
            movimentos.append((
                valor,
                descricao,
                data_movimento_str,
                idx_cartao,
                tipo_movimento,
                categoria,
                estabelecimento
            ))
    
    return movimentos


def generate_sql_file(output_file='sql/02_insert_data.sql', qtd_associados=100, media_contas_por_associado=2, media_movimentos_por_cartao=10):
    """
    Gera arquivo SQL completo com dados fictícios
    
    Args:
        output_file: Nome do arquivo de saída
    """
    print("=" * 70)
    print("GERADOR DE DADOS FICTÍCIOS - SiCooperative")
    print("=" * 70)
    print()
    
    # Gerar dados
    print("Gerando associados...")
    associados = generate_associados(qtd_associados)
    print(f"[OK] {len(associados)} associados gerados")
    
    print("Gerando contas...")
    contas = generate_contas(associados, media_contas_por_associado)
    print(f"[OK] {len(contas)} contas geradas")
    
    print("Gerando cartoes...")
    cartoes = generate_cartoes(contas, associados)
    print(f"[OK] {len(cartoes)} cartoes gerados")
    
    print("Gerando movimentos...")
    movimentos = generate_movimentos(cartoes, media_movimentos_por_cartao)
    print(f"[OK] {len(movimentos)} movimentos gerados")
    
    print()
    print(f"Escrevendo arquivo SQL: {output_file}")
    
    # Escrever arquivo SQL
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("-- " + "=" * 68 + "\n")
        f.write("-- Script: 02_insert_data.sql\n")
        f.write("-- Descrição: Dados fictícios gerados com Faker (pt_BR)\n")
        f.write("-- Gerado em: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n")
        f.write("-- " + "=" * 68 + "\n\n")
        
        f.write("USE sicooperative_db;\n\n")
        f.write("SET FOREIGN_KEY_CHECKS = 0;\n")
        f.write("SET UNIQUE_CHECKS = 0;\n")
        f.write("SET AUTOCOMMIT = 0;\n\n")
        
        # Limpar tabelas
        f.write("-- Limpar dados existentes\n")
        f.write("TRUNCATE TABLE movimento;\n")
        f.write("TRUNCATE TABLE cartao;\n")
        f.write("TRUNCATE TABLE conta;\n")
        f.write("TRUNCATE TABLE associado;\n\n")
        
        # Inserir associados
        f.write("-- " + "=" * 68 + "\n")
        f.write(f"-- ASSOCIADOS ({len(associados)} registros)\n")
        f.write("-- " + "=" * 68 + "\n\n")
        f.write("INSERT INTO associado (nome, sobrenome, idade, email) VALUES\n")
        
        for i, (nome, sobrenome, idade, email) in enumerate(associados):
            virgula = "," if i < len(associados) - 1 else ";"
            f.write(f"('{nome}', '{sobrenome}', {idade}, '{email}'){virgula}\n")
        
        f.write("\nCOMMIT;\n\n")
        
        # Inserir contas
        f.write("-- " + "=" * 68 + "\n")
        f.write(f"-- CONTAS ({len(contas)} registros)\n")
        f.write("-- " + "=" * 68 + "\n\n")
        f.write("INSERT INTO conta (tipo, data_criacao, id_associado, saldo_atual, ativa) VALUES\n")
        
        for i, (tipo, data_criacao, id_associado, saldo) in enumerate(contas):
            virgula = "," if i < len(contas) - 1 else ";"
            f.write(f"('{tipo}', '{data_criacao}', {id_associado}, {saldo}, TRUE){virgula}\n")
        
        f.write("\nCOMMIT;\n\n")
        
        # Inserir cartões
        f.write("-- " + "=" * 68 + "\n")
        f.write(f"-- CARTÕES ({len(cartoes)} registros)\n")
        f.write("-- " + "=" * 68 + "\n\n")
        f.write("INSERT INTO cartao (num_cartao, nom_impresso, data_criacao, data_validade, "
                "id_conta, id_associado, tipo_cartao, ativo, limite_credito) VALUES\n")
        
        for i, cartao in enumerate(cartoes):
            virgula = "," if i < len(cartoes) - 1 else ";"
            f.write(f"('{cartao[0]}', '{cartao[1]}', '{cartao[2]}', '{cartao[3]}', "
                   f"{cartao[4]}, {cartao[5]}, '{cartao[6]}', TRUE, {cartao[7]}){virgula}\n")
        
        f.write("\nCOMMIT;\n\n")
        
        # Inserir movimentos (em lotes de 1000)
        f.write("-- " + "=" * 68 + "\n")
        f.write(f"-- MOVIMENTOS ({len(movimentos)} registros)\n")
        f.write("-- " + "=" * 68 + "\n\n")
        
        batch_size = 1000
        for batch_start in range(0, len(movimentos), batch_size):
            batch_end = min(batch_start + batch_size, len(movimentos))
            batch = movimentos[batch_start:batch_end]
            
            f.write("INSERT INTO movimento (vlr_transacao, des_transacao, data_movimento, "
                   "id_cartao, tipo_movimento, categoria, estabelecimento) VALUES\n")
            
            for i, mov in enumerate(batch):
                virgula = "," if i < len(batch) - 1 else ";"
                # Escapar aspas simples na descrição
                descricao = mov[1].replace("'", "''")
                estabelecimento = mov[6].replace("'", "''")
                f.write(f"({mov[0]}, '{descricao}', '{mov[2]}', {mov[3]}, "
                       f"'{mov[4]}', '{mov[5]}', '{estabelecimento}'){virgula}\n")
            
            f.write("\nCOMMIT;\n\n")
        
        # Restaurar configurações
        # f.write("SET FOREIGN_KEY_CHECKS = 1;\n")
        # f.write("SET UNIQUE_CHECKS = 1;\n")
        # f.write("SET AUTOCOMMIT = 1;\n\n")
        
        # Estatísticas
        # f.write("-- Estatísticas\n")
        # f.write("SELECT 'Dados inseridos com sucesso!' AS status;\n\n")
        # f.write("SELECT \n")
        # f.write("    'Associados' AS tabela,\n")
        # f.write("    COUNT(*) AS total_registros\n")
        # f.write("FROM associado\n")
        # f.write("UNION ALL\n")
        # f.write("SELECT 'Contas', COUNT(*) FROM conta\n")
        # f.write("UNION ALL\n")
        # f.write("SELECT 'Cartões', COUNT(*) FROM cartao\n")
        # f.write("UNION ALL\n")
        # f.write("SELECT 'Movimentos', COUNT(*) FROM movimento;\n")
    
    print(f"[OK] Arquivo gerado com sucesso: {output_file}")
    print()
    print("=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"Associados:  {len(associados)}")
    print(f"Contas:      {len(contas)}")
    print(f"Cartões:     {len(cartoes)}")
    print(f"Movimentos:  {len(movimentos)}")
    print("=" * 70)


if __name__ == "__main__":
    # Verificar se Faker está instalado
    try:
        from faker import Faker
    except ImportError:
        print("ERRO: Faker não está instalado!")
        print("Instale com: pip install faker")
        sys.exit(1)
    
    # Parametros
    associados = 100
    media_contas_por_associado = 2
    media_movimentos_por_cartao = 10

    if len(sys.argv) > 1:
        for param in sys.argv[1:]:
            if param == "-h" or param == "--help":
                print("Usage: python generate_fake_data.py --associados <associados> --media_contas_por_associado <media_contas_por_associado> --media_movimentos_por_cartao <media_movimentos_por_cartao>")
                sys.exit(0)
            elif param == "--associados":
                associados = int(sys.argv[sys.argv.index(param) + 1])
            elif param == "--media_contas_por_associado":
                media_contas_por_associado = int(sys.argv[sys.argv.index(param) + 1])
            elif param == "--media_movimentos_por_cartao":
                media_movimentos_por_cartao = int(sys.argv[sys.argv.index(param) + 1])
    
    # Gerar arquivo
    generate_sql_file(
        qtd_associados=associados,
        media_contas_por_associado=media_contas_por_associado,
        media_movimentos_por_cartao=media_movimentos_por_cartao)
