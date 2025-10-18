-- ============================================================================
-- Script: 01_create_schema.sql
-- Descrição: Criação do schema do banco de dados para o Data Lake POC
-- Autor: Desafio Técnico - Engenharia de Dados
-- Database: MySQL 8.0+
-- ============================================================================

-- Criar database se não existir
CREATE DATABASE IF NOT EXISTS sicooperative_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE sicooperative_db;

-- ============================================================================
-- TABELA: associado
-- Descrição: Armazena informações dos clientes (associados) da cooperativa
-- ============================================================================

DROP TABLE IF EXISTS movimento;
DROP TABLE IF EXISTS cartao;
DROP TABLE IF EXISTS conta;
DROP TABLE IF EXISTS associado;

CREATE TABLE associado (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Identificador único do associado',
    nome VARCHAR(100) NOT NULL COMMENT 'Nome do associado',
    sobrenome VARCHAR(100) NOT NULL COMMENT 'Sobrenome do associado',
    idade INT NOT NULL COMMENT 'Idade do associado',
    email VARCHAR(255) NOT NULL UNIQUE COMMENT 'Email do associado (único)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de criação do registro',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Data de atualização do registro',
    
    -- Constraints
    CONSTRAINT chk_idade CHECK (idade >= 18 AND idade <= 120),
    CONSTRAINT chk_email CHECK (email LIKE '%@%')
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Tabela de associados (clientes) da cooperativa';

-- Índices para otimização de queries
CREATE INDEX idx_associado_nome ON associado(nome, sobrenome);
CREATE INDEX idx_associado_email ON associado(email);

-- ============================================================================
-- TABELA: conta
-- Descrição: Armazena informações das contas bancárias dos associados
-- ============================================================================

CREATE TABLE conta (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Identificador único da conta',
    tipo ENUM('corrente', 'poupanca', 'investimento') NOT NULL COMMENT 'Tipo da conta',
    data_criacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de abertura da conta',
    id_associado INT NOT NULL COMMENT 'FK: Associado dono da conta',
    saldo_atual DECIMAL(15,2) DEFAULT 0.00 COMMENT 'Saldo atual da conta',
    ativa BOOLEAN DEFAULT TRUE COMMENT 'Indica se a conta está ativa',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de criação do registro',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Data de atualização do registro',
    
    -- Foreign Keys
    CONSTRAINT fk_conta_associado 
        FOREIGN KEY (id_associado) 
        REFERENCES associado(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    
    -- Constraints
    CONSTRAINT chk_saldo CHECK (saldo_atual >= -10000.00)
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Tabela de contas bancárias dos associados';

-- Índices para otimização de queries
CREATE INDEX idx_conta_associado ON conta(id_associado);
CREATE INDEX idx_conta_tipo ON conta(tipo);
CREATE INDEX idx_conta_data_criacao ON conta(data_criacao);

-- ============================================================================
-- TABELA: cartao
-- Descrição: Armazena informações dos cartões vinculados às contas
-- ============================================================================

CREATE TABLE cartao (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Identificador único do cartão',
    num_cartao VARCHAR(16) NOT NULL UNIQUE COMMENT 'Número do cartão (16 dígitos)',
    nom_impresso VARCHAR(100) NOT NULL COMMENT 'Nome impresso no cartão',
    data_criacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de emissão do cartão',
    data_validade DATE NOT NULL COMMENT 'Data de validade do cartão',
    id_conta INT NOT NULL COMMENT 'FK: Conta vinculada ao cartão',
    id_associado INT NOT NULL COMMENT 'FK: Associado titular do cartão',
    tipo_cartao ENUM('debito', 'credito', 'multiplo') DEFAULT 'debito' COMMENT 'Tipo do cartão',
    ativo BOOLEAN DEFAULT TRUE COMMENT 'Indica se o cartão está ativo',
    limite_credito DECIMAL(10,2) DEFAULT 0.00 COMMENT 'Limite de crédito (se aplicável)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de criação do registro',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Data de atualização do registro',
    
    -- Foreign Keys
    CONSTRAINT fk_cartao_conta 
        FOREIGN KEY (id_conta) 
        REFERENCES conta(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    
    CONSTRAINT fk_cartao_associado 
        FOREIGN KEY (id_associado) 
        REFERENCES associado(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    
    -- Constraints
    CONSTRAINT chk_num_cartao CHECK (LENGTH(num_cartao) = 16),
    CONSTRAINT chk_data_validade CHECK (data_validade > data_criacao),
    CONSTRAINT chk_limite_credito CHECK (limite_credito >= 0)
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Tabela de cartões vinculados às contas';

-- Índices para otimização de queries
CREATE INDEX idx_cartao_num ON cartao(num_cartao);
CREATE INDEX idx_cartao_conta ON cartao(id_conta);
CREATE INDEX idx_cartao_associado ON cartao(id_associado);
CREATE INDEX idx_cartao_tipo ON cartao(tipo_cartao);

-- ============================================================================
-- TABELA: movimento
-- Descrição: Armazena todas as transações realizadas com os cartões
-- ============================================================================

CREATE TABLE movimento (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Identificador único da transação',
    vlr_transacao DECIMAL(10,2) NOT NULL COMMENT 'Valor da transação',
    des_transacao VARCHAR(255) NOT NULL COMMENT 'Descrição da transação',
    data_movimento TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Data e hora da transação',
    id_cartao INT NOT NULL COMMENT 'FK: Cartão utilizado na transação',
    tipo_movimento ENUM('debito', 'credito', 'estorno') DEFAULT 'debito' COMMENT 'Tipo de movimento',
    categoria VARCHAR(50) COMMENT 'Categoria da transação (ex: alimentação, transporte)',
    estabelecimento VARCHAR(255) COMMENT 'Nome do estabelecimento',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data de criação do registro',
    
    -- Foreign Keys
    CONSTRAINT fk_movimento_cartao 
        FOREIGN KEY (id_cartao) 
        REFERENCES cartao(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    
    -- Constraints
    CONSTRAINT chk_vlr_transacao CHECK (vlr_transacao != 0)
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Tabela de movimentações (transações) dos cartões';

-- Índices para otimização de queries
CREATE INDEX idx_movimento_cartao ON movimento(id_cartao);
CREATE INDEX idx_movimento_data ON movimento(data_movimento);
CREATE INDEX idx_movimento_valor ON movimento(vlr_transacao);
CREATE INDEX idx_movimento_tipo ON movimento(tipo_movimento);
CREATE INDEX idx_movimento_categoria ON movimento(categoria);

-- ============================================================================
-- VIEWS AUXILIARES
-- ============================================================================

-- View: Visão consolidada de movimentos com informações completas
CREATE OR REPLACE VIEW vw_movimento_completo AS
SELECT 
    m.id AS movimento_id,
    m.vlr_transacao,
    m.des_transacao,
    m.data_movimento,
    m.tipo_movimento,
    m.categoria,
    m.estabelecimento,
    c.num_cartao,
    c.nom_impresso AS nome_impresso_cartao,
    c.data_criacao AS data_criacao_cartao,
    c.tipo_cartao,
    ct.tipo AS tipo_conta,
    ct.data_criacao AS data_criacao_conta,
    a.nome AS nome_associado,
    a.sobrenome AS sobrenome_associado,
    a.idade AS idade_associado,
    a.email AS email_associado
FROM movimento m
INNER JOIN cartao c ON m.id_cartao = c.id
INNER JOIN conta ct ON c.id_conta = ct.id
INNER JOIN associado a ON c.id_associado = a.id
ORDER BY m.data_movimento DESC;

-- ============================================================================
-- PROCEDURES E FUNCTIONS
-- ============================================================================


-- Procedure: Obter estatísticas de um associado
CREATE PROCEDURE sp_estatisticas_associado(IN p_id_associado INT)
BEGIN
    SELECT 
        a.nome,
        a.sobrenome,
        COUNT(DISTINCT ct.id) AS total_contas,
        COUNT(DISTINCT c.id) AS total_cartoes,
        COUNT(m.id) AS total_transacoes,
        COALESCE(SUM(CASE WHEN m.tipo_movimento = 'debito' THEN m.vlr_transacao ELSE 0 END), 0) AS total_debitos,
        COALESCE(SUM(CASE WHEN m.tipo_movimento = 'credito' THEN m.vlr_transacao ELSE 0 END), 0) AS total_creditos
    FROM associado a
    LEFT JOIN conta ct ON a.id = ct.id_associado
    LEFT JOIN cartao c ON a.id = c.id_associado
    LEFT JOIN movimento m ON c.id = m.id_cartao
    WHERE a.id = p_id_associado
    GROUP BY a.id, a.nome, a.sobrenome;
END 
;

-- Function: Calcular total de transações de um cartão
CREATE FUNCTION fn_total_transacoes_cartao(p_id_cartao INT)
RETURNS DECIMAL(15,2)
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_total DECIMAL(15,2);
    
    SELECT COALESCE(SUM(
        CASE 
            WHEN tipo_movimento = 'debito' THEN -vlr_transacao
            WHEN tipo_movimento = 'credito' THEN vlr_transacao
            ELSE 0
        END
    ), 0) INTO v_total
    FROM movimento
    WHERE id_cartao = p_id_cartao;
    
    RETURN v_total;
END 


-- ============================================================================
-- GRANTS E PERMISSÕES (Opcional - ajustar conforme necessário)
-- ============================================================================

-- Criar usuário para a aplicação (se necessário)
-- CREATE USER IF NOT EXISTS 'etl_user'@'%' IDENTIFIED BY 'etl_password';
-- GRANT SELECT, INSERT, UPDATE ON sicooperative_db.* TO 'etl_user'@'%';
-- FLUSH PRIVILEGES;

-- ============================================================================
-- VERIFICAÇÃO DO SCHEMA
-- ============================================================================

-- Verificar tabelas criadas
/*
SELECT 
    TABLE_NAME,
    TABLE_ROWS,
    AUTO_INCREMENT,
    CREATE_TIME,
    TABLE_COMMENT
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'sicooperative_db'
ORDER BY TABLE_NAME;

-- Verificar constraints
SELECT 
    TABLE_NAME,
    CONSTRAINT_NAME,
    CONSTRAINT_TYPE
FROM information_schema.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'sicooperative_db'
ORDER BY TABLE_NAME, CONSTRAINT_TYPE;
*/

-- ============================================================================
-- FIM DO SCRIPT
-- ============================================================================

-- SELECT 'Schema criado com sucesso!' AS status;
