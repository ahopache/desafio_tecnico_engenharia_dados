# Scripts SQL - SiCooperative Data Lake POC

Este diretório contém os scripts SQL para criação e população do banco de dados MySQL.

## 📄 Arquivos

### **01_create_schema.sql** (270 linhas)
**DDL - Criação do Schema**

Cria toda a estrutura do banco de dados:
- ✅ Database `sicooperative_db`
- ✅ 4 Tabelas normalizadas (3FN):
  - `associado` - Clientes da cooperativa
  - `conta` - Contas bancárias
  - `cartao` - Cartões vinculados às contas
  - `movimento` - Transações dos cartões

**Features:**
- ✅ Foreign keys com ON DELETE RESTRICT e ON UPDATE CASCADE
- ✅ Check constraints (idade, email, saldo, valores)
- ✅ Índices otimizados para queries de JOIN
- ✅ View `vw_movimento_completo` (visão consolidada)
- ✅ Stored Procedure `sp_estatisticas_associado`
- ✅ Function `fn_total_transacoes_cartao`
- ✅ Charset UTF-8 (utf8mb4_unicode_ci)

### **02_insert_data.sql** (~3800 linhas)
**DML - Inserção de Dados Fictícios**

Popula o banco com dados realistas gerados pelo Faker:
- ✅ **100 associados** com nomes brasileiros
- ✅ **193 contas** (média de 2 por associado)
- ✅ **264 cartões** com números válidos
- ✅ **3166 movimentos** com estabelecimentos reais

**Dados Realistas:**
- ✅ Nomes e sobrenomes brasileiros (Faker pt_BR)
- ✅ Estabelecimentos reais de Porto Alegre:
  - Zaffari, Carrefour, Big, Nacional
  - Posto Ipiranga, Uber, 99 Taxi
  - Farmácia Panvel, Hospital Moinhos de Vento
  - Renner, C&A, Zara
  - Cinemark, Bar Ocidente, Teatro São Pedro
- ✅ Categorias: alimentação, transporte, saúde, educação, lazer, vestuário, moradia, serviços
- ✅ Valores proporcionais às categorias
- ✅ Datas nos últimos 365 dias

### **generate_fake_data.py** (440 linhas)
**Gerador de Dados com Faker**

Script Python para regenerar o arquivo `02_insert_data.sql` com novos dados:

```bash
# Gerar novos dados (padrão)
python sql/generate_fake_data.py

# Gerar com parâmetros personalizados
python sql/generate_fake_data.py --associados 200 --media_contas_por_associado 3 --media_movimentos_por_cartao 15

# Ver ajuda
python sql/generate_fake_data.py --help

# Isso sobrescreve: sql/02_insert_data.sql
```

**Parâmetros disponíveis:**
- `--associados` (padrão: 100): Quantidade de associados
- `--media_contas_por_associado` (padrão: 2): Média de contas por associado
- `--media_movimentos_por_cartao` (padrão: 10): Movimentos por cartão
- `--help` ou `-h`: Mostra ajuda

**Configurável:**
- Quantidade de associados (padrão: 100)
- Média de contas por associado (padrão: 2)
- Movimentos por cartão (padrão: 10)
- Seed para reprodutibilidade (padrão: 42)

---

## 🚀 Como Usar

### **Opção 1: Execução Manual**

```bash
# 1. Criar schema
mysql -u root -p < sql/01_create_schema.sql

# 2. Popular com dados
mysql -u root -p < sql/02_insert_data.sql
```

### **Opção 2: Docker (Automático)**

Os scripts são executados automaticamente na inicialização do container MySQL:

```bash
cd docker
docker-compose up -d

# Os scripts em /docker-entrypoint-initdb.d são executados em ordem:
# 1. 01_create_schema.sql
# 2. 02_insert_data.sql
```

### **Opção 3: Regenerar Dados**

```bash
# Instalar Faker (se necessário)
pip install faker

# Gerar novos dados (padrão: 100 associados, 2 contas/associado, 10 movimentos/cartão)
python sql/generate_fake_data.py

# Gerar dados personalizados
python sql/generate_fake_data.py --associados 500 --media_contas_por_associado 3 --media_movimentos_por_cartao 20

# Aplicar no banco
mysql -u root -p sicooperative_db < sql/02_insert_data.sql
```

---

## 📊 Modelo de Dados

### **Relacionamentos**

```
┌─────────────┐
│  associado  │
│  (variável) │
└──────┬──────┘
       │ 1
       │
       │ N
┌──────┴──────┐
│    conta    │
│  (variável) │
└──────┬──────┘
       │ 1
       │
       │ N
┌──────┴──────┐
│   cartao    │
│  (variável) │
└──────┬──────┘
       │ 1
       │
       │ N
┌──────┴──────┐
│  movimento  │
│  (variável) │
└─────────────┘
```

> 📊 **Nota**: Os números de registros variam de acordo com os parâmetros usados no `generate_fake_data.py`

### **Tabelas**

| Tabela | Registros (exemplo) | Descrição |
|--------|-------------------|-----------|
| `associado` | ~100-500 | Clientes (nome, sobrenome, idade, email) |
| `conta` | ~200-1500 | Contas bancárias (tipo, saldo, data_criacao) |
| `cartao` | ~250-2000 | Cartões (número, nome_impresso, validade, limite) |
| `movimento` | ~2500-50000 | Transações (valor, descrição, data, categoria) |

> 📈 **Exemplo com parâmetros padrão**: 100 associados → ~193 contas → ~264 cartões → ~3166 movimentos

---

## 🔧 Personalização

### **Modificar Volumes de Dados (Recomendado)**

A maneira mais fácil de personalizar é usando os parâmetros via linha de comando:

```bash
# Volumes pequenos (para desenvolvimento)
python sql/generate_fake_data.py --associados 50 --media_contas_por_associado 2 --media_movimentos_por_cartao 5

# Volumes médios (padrão)
python sql/generate_fake_data.py

# Volumes grandes (para testes de performance)
python sql/generate_fake_data.py --associados 1000 --media_contas_por_associado 3 --media_movimentos_por_cartao 20
```

### **Personalizações Avançadas (Editar Código)**

Para modificações mais específicas, edite `generate_fake_data.py`:

```python
# Alterar estabelecimentos (linha ~350)
categorias_estabelecimentos = {
    'alimentacao': [
        'Zaffari', 'Carrefour', 'Big',
        'Seu Novo Estabelecimento'  # Adicionar aqui
    ],
    # ...
}

# Mudar seed para dados diferentes (linha ~12-13)
Faker.seed(123)  # Alterar de 42 para outro número
random.seed(123)
```

---

## 📝 Notas Importantes

### **Ordem de Execução**
Os scripts **devem** ser executados em ordem:
1. `01_create_schema.sql` (cria estrutura)
2. `02_insert_data.sql` (popula dados)

### **Truncate vs Drop**
O `02_insert_data.sql` usa `TRUNCATE` para limpar dados existentes, mantendo a estrutura das tabelas.

### **Foreign Keys**
As foreign keys garantem integridade referencial:
- Não é possível deletar um associado com contas
- Não é possível deletar uma conta com cartões
- Não é possível deletar um cartão com movimentos

### **Performance**
- Inserts em lotes de 1000 registros
- Índices criados após inserção
- `SET AUTOCOMMIT = 0` para transações em lote

---

## 🐛 Troubleshooting

### **Erro: "Table already exists"**
```bash
# Dropar database e recriar
mysql -u root -p -e "DROP DATABASE IF EXISTS sicooperative_db;"
mysql -u root -p < sql/01_create_schema.sql
```

### **Erro: "Duplicate entry"**
```bash
# Limpar dados e reinserir
mysql -u root -p sicooperative_db -e "TRUNCATE TABLE movimento; TRUNCATE TABLE cartao; TRUNCATE TABLE conta; TRUNCATE TABLE associado;"
mysql -u root -p < sql/02_insert_data.sql
```

### **Erro: "Cannot delete or update a parent row"**
```bash
# Desabilitar foreign key checks temporariamente
mysql -u root -p sicooperative_db -e "SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE movimento; TRUNCATE TABLE cartao; TRUNCATE TABLE conta; TRUNCATE TABLE associado; SET FOREIGN_KEY_CHECKS = 1;"
```

---

## 📚 Referências

- [MySQL Documentation](https://dev.mysql.com/doc/)
- [Faker Documentation](https://faker.readthedocs.io/)
- [Faker pt_BR Provider](https://faker.readthedocs.io/en/master/locales/pt_BR.html)
