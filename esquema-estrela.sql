-- Script SQL para Criação do Esquema Estrela do Data Warehouse (locadora_dw)

-- Criação do banco de dados do Data Warehouse
CREATE DATABASE IF NOT EXISTS locadora_dw;
USE locadora_dw;

-- 1. Criação da Dimensão de Tempo (Dim_Tempo)
-- Esta dimensão é pré-populada e estática.
CREATE TABLE IF NOT EXISTS Dim_Tempo (
    sk_tempo INT PRIMARY KEY AUTO_INCREMENT,
    data_completa DATE NOT NULL,
    ano SMALLINT NOT NULL,
    mes SMALLINT NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    dia SMALLINT NOT NULL,
    dia_da_semana SMALLINT NOT NULL,
    nome_dia_da_semana VARCHAR(20) NOT NULL,
    trimestre SMALLINT NOT NULL,
    semestre SMALLINT NOT NULL,
    hora SMALLINT NOT NULL,
    minuto SMALLINT NOT NULL,
    segundo SMALLINT NOT NULL,
    turno_dia VARCHAR(20) NOT NULL,
    feriado BOOLEAN NOT NULL,
    UNIQUE (data_completa, hora, minuto, segundo) -- Garante unicidade para granularidade de segundo
);

-- 2. Criação da Dimensão de Cliente (Dim_Cliente) - SCD Tipo 2
CREATE TABLE IF NOT EXISTS Dim_Cliente (
    sk_cliente INT PRIMARY KEY AUTO_INCREMENT,
    id_cliente_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    tipo_cliente VARCHAR(10) NOT NULL, -- 'PF' ou 'PJ'
    nome_razao_social VARCHAR(255) NOT NULL,
    cpf VARCHAR(14), -- NULL se PJ
    cnpj VARCHAR(18), -- NULL se PF
    endereco VARCHAR(255),
    telefone VARCHAR(20),
    email VARCHAR(100),
    cidade_cliente VARCHAR(100),
    estado_cliente VARCHAR(100),
    pais_cliente VARCHAR(100),
    data_cadastro DATE,
    data_inicio_vigencia DATETIME NOT NULL,
    data_fim_vigencia DATETIME NOT NULL,
    flag_ativo BOOLEAN NOT NULL,
    INDEX idx_cliente_origem_sistema (id_cliente_origem, sistema_origem)
);

-- 3. Criação da Dimensão de Veículo (Dim_Veiculo) - SCD Tipo 2
CREATE TABLE IF NOT EXISTS Dim_Veiculo (
    sk_veiculo INT PRIMARY KEY AUTO_INCREMENT,
    id_veiculo_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    placa VARCHAR(10) NOT NULL,
    chassi VARCHAR(50) NOT NULL,
    marca VARCHAR(50),
    modelo VARCHAR(100),
    ano_fabricacao SMALLINT,
    cor VARCHAR(50),
    tipo_mecanizacao VARCHAR(50), -- Ex: 'Automático', 'Manual'
    nome_grupo_veiculo VARCHAR(100),
    descricao_grupo_veiculo VARCHAR(255),
    valor_diaria_base_grupo DECIMAL(10, 2),
    url_foto_principal VARCHAR(255),
    tem_ar_condicionado BOOLEAN,
    tem_cadeirinha BOOLEAN,
    data_inicio_vigencia DATETIME NOT NULL,
    data_fim_vigencia DATETIME NOT NULL,
    flag_ativo BOOLEAN NOT NULL,
    INDEX idx_veiculo_origem_sistema (id_veiculo_origem, sistema_origem)
);

-- 4. Criação da Dimensão de Pátio (Dim_Patio) - SCD Tipo 1
CREATE TABLE IF NOT EXISTS Dim_Patio (
    sk_patio INT PRIMARY KEY AUTO_INCREMENT,
    id_patio_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    nome_patio VARCHAR(100) NOT NULL,
    endereco_patio VARCHAR(255),
    cidade_patio VARCHAR(100),
    estado_patio VARCHAR(100),
    capacidade_vagas_patio INT,
    nome_empresa_proprietaria VARCHAR(255),
    UNIQUE KEY uk_patio_origem (id_patio_origem, sistema_origem)
);

-- 5. Criação da Dimensão de Empresa (Dim_Empresa) - SCD Tipo 1
CREATE TABLE IF NOT EXISTS Dim_Empresa (
    sk_empresa INT PRIMARY KEY AUTO_INCREMENT,
    id_empresa_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    nome_empresa VARCHAR(255) NOT NULL,
    cnpj_empresa VARCHAR(18),
    endereco_empresa VARCHAR(255),
    UNIQUE KEY uk_empresa_origem (id_empresa_origem, sistema_origem)
);

-- 6. Criação da Dimensão de Condutor (Dim_Condutor) - SCD Tipo 1
CREATE TABLE IF NOT EXISTS Dim_Condutor (
    sk_condutor INT PRIMARY KEY AUTO_INCREMENT,
    id_condutor_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    nome_completo_condutor VARCHAR(255) NOT NULL,
    numero_cnh_condutor VARCHAR(50),
    categoria_cnh_condutor VARCHAR(10),
    data_expiracao_cnh_condutor DATE,
    data_nascimento_condutor DATE,
    nacionalidade_condutor VARCHAR(100),
    tipo_documento_habilitacao VARCHAR(50),
    pais_emissao_cnh VARCHAR(100),
    data_entrada_brasil DATE,
    flag_traducao_juramentada BOOLEAN,
    UNIQUE KEY uk_condutor_origem (id_condutor_origem, sistema_origem)
);

-- 7. Criação da Dimensão de Status de Locação (Dim_Status_Locacao) - Estática
CREATE TABLE IF NOT EXISTS Dim_Status_Locacao (
    sk_status_locacao INT PRIMARY KEY AUTO_INCREMENT,
    nome_status_locacao VARCHAR(50) NOT NULL UNIQUE,
    descricao_status_locacao VARCHAR(255)
);

-- Inserção de dados padrão para Dim_Status_Locacao
INSERT IGNORE INTO Dim_Status_Locacao (nome_status_locacao, descricao_status_locacao) VALUES
('Ativa', 'Locação em andamento.'),
('Concluida', 'Locação finalizada com devolução.'),
('Cancelada', 'Locação que foi cancelada antes da retirada.');

-- 8. Criação da Dimensão de Status de Reserva (Dim_Status_Reserva) - Estática
CREATE TABLE IF NOT EXISTS Dim_Status_Reserva (
    sk_status_reserva INT PRIMARY KEY AUTO_INCREMENT,
    nome_status_reserva VARCHAR(50) NOT NULL UNIQUE,
    descricao_status_reserva VARCHAR(255)
);

-- Inserção de dados padrão para Dim_Status_Reserva
INSERT IGNORE INTO Dim_Status_Reserva (nome_status_reserva, descricao_status_reserva) VALUES
('Confirmada', 'Reserva confirmada pelo cliente.'),
('Cancelada', 'Reserva que foi cancelada.'),
('Em Espera', 'Reserva aguardando confirmação ou disponibilidade.');

-- 9. Criação da Dimensão de Tipo de Movimentação de Pátio (Dim_Tipo_Movimentacao_Patio) - Estática
CREATE TABLE IF NOT EXISTS Dim_Tipo_Movimentacao_Patio (
    sk_tipo_movimentacao INT PRIMARY KEY AUTO_INCREMENT,
    nome_tipo_movimentacao VARCHAR(50) NOT NULL UNIQUE,
    descricao_tipo_movimentacao VARCHAR(255)
);

-- Inserção de dados padrão para Dim_Tipo_Movimentacao_Patio
INSERT IGNORE INTO Dim_Tipo_Movimentacao_Patio (nome_tipo_movimentacao, descricao_tipo_movimentacao) VALUES
('Entrada', 'Veículo entrou no pátio.'),
('Saída', 'Veículo saiu do pátio.'),
('Transferência', 'Movimentação de veículo entre pátios.');

-- 10. Criação da Dimensão de Seguro (Dim_Seguro) - SCD Tipo 1
CREATE TABLE IF NOT EXISTS Dim_Seguro (
    sk_seguro INT PRIMARY KEY AUTO_INCREMENT,
    id_seguro_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    nome_seguro VARCHAR(100) NOT NULL,
    descricao_seguro VARCHAR(255),
    valor_diario_seguro DECIMAL(10, 2),
    UNIQUE KEY uk_seguro_origem (id_seguro_origem, sistema_origem)
);

-- 11. Criação da Tabela de Fatos: Fato_Locacao
CREATE TABLE IF NOT EXISTS Fato_Locacao (
    sk_locacao INT PRIMARY KEY AUTO_INCREMENT,
    sk_tempo_retirada INT NOT NULL,
    sk_tempo_devolucao_prevista INT NOT NULL,
    sk_tempo_devolucao_real INT, -- Pode ser NULL se a locação ainda estiver ativa
    sk_cliente INT NOT NULL,
    sk_veiculo INT NOT NULL,
    sk_condutor INT, -- Pode ser NULL se não houver condutor extra
    sk_patio_retirada INT NOT NULL,
    sk_patio_devolucao_prevista INT NOT NULL,
    sk_patio_devolucao_real INT, -- Pode ser NULL
    sk_status_locacao INT NOT NULL,
    sk_seguro INT, -- Pode ser NULL se não houver seguro contratado
    id_locacao_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    valor_total_locacao DECIMAL(10, 2),
    valor_base_locacao DECIMAL(10, 2),
    valor_multas_taxas DECIMAL(10, 2),
    valor_seguro DECIMAL(10, 2),
    valor_descontos DECIMAL(10, 2),
    quilometragem_percorrida DECIMAL(10, 2),
    duracao_locacao_horas_prevista DECIMAL(10, 2),
    duracao_locacao_horas_real DECIMAL(10, 2),
    quant_locacoes INT NOT NULL DEFAULT 1, -- Medida aditiva para contagem
    
    -- Chaves Estrangeiras
    FOREIGN KEY (sk_tempo_retirada) REFERENCES Dim_Tempo(sk_tempo),
    FOREIGN KEY (sk_tempo_devolucao_prevista) REFERENCES Dim_Tempo(sk_tempo),
    FOREIGN KEY (sk_tempo_devolucao_real) REFERENCES Dim_Tempo(sk_tempo),
    FOREIGN KEY (sk_cliente) REFERENCES Dim_Cliente(sk_cliente),
    FOREIGN KEY (sk_veiculo) REFERENCES Dim_Veiculo(sk_veiculo),
    FOREIGN KEY (sk_condutor) REFERENCES Dim_Condutor(sk_condutor),
    FOREIGN KEY (sk_patio_retirada) REFERENCES Dim_Patio(sk_patio),
    FOREIGN KEY (sk_patio_devolucao_prevista) REFERENCES Dim_Patio(sk_patio),
    FOREIGN KEY (sk_patio_devolucao_real) REFERENCES Dim_Patio(sk_patio),
    FOREIGN KEY (sk_status_locacao) REFERENCES Dim_Status_Locacao(sk_status_locacao),
    FOREIGN KEY (sk_seguro) REFERENCES Dim_Seguro(sk_seguro),
    
    UNIQUE KEY uk_locacao_origem (id_locacao_origem, sistema_origem)
);

-- 12. Criação da Tabela de Fatos: Fato_Reserva
CREATE TABLE IF NOT EXISTS Fato_Reserva (
    sk_reserva INT PRIMARY KEY AUTO_INCREMENT,
    sk_tempo_reserva INT NOT NULL,
    sk_tempo_retirada_prevista INT NOT NULL,
    sk_tempo_devolucao_prevista INT NOT NULL,
    sk_cliente INT NOT NULL,
    sk_grupo_veiculo INT NOT NULL, -- Referencia sk_veiculo em Dim_Veiculo para o grupo
    sk_patio_retirada INT NOT NULL,
    sk_status_reserva INT NOT NULL,
    id_reserva_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    quant_reservas INT NOT NULL DEFAULT 1, -- Medida aditiva para contagem
    dias_antecedencia_reserva INT,
    duracao_reserva_horas_prevista DECIMAL(10, 2),
    
    -- Chaves Estrangeiras
    FOREIGN KEY (sk_tempo_reserva) REFERENCES Dim_Tempo(sk_tempo),
    FOREIGN KEY (sk_tempo_retirada_prevista) REFERENCES Dim_Tempo(sk_tempo),
    FOREIGN KEY (sk_tempo_devolucao_prevista) REFERENCES Dim_Tempo(sk_tempo),
    FOREIGN KEY (sk_cliente) REFERENCES Dim_Cliente(sk_cliente),
    FOREIGN KEY (sk_grupo_veiculo) REFERENCES Dim_Veiculo(sk_veiculo),
    FOREIGN KEY (sk_patio_retirada) REFERENCES Dim_Patio(sk_patio),
    FOREIGN KEY (sk_status_reserva) REFERENCES Dim_Status_Reserva(sk_status_reserva),
    
    UNIQUE KEY uk_reserva_origem (id_reserva_origem, sistema_origem)
);

-- 13. Criação da Tabela de Fatos: Fato_Movimentacao_Patio
CREATE TABLE IF NOT EXISTS Fato_Movimentacao_Patio (
    sk_movimentacao INT PRIMARY KEY AUTO_INCREMENT,
    sk_tempo_movimentacao INT NOT NULL,
    sk_veiculo INT NOT NULL,
    sk_patio_origem INT, -- Pode ser NULL para entrada inicial
    sk_patio_destino INT, -- Pode ser NULL para saída final
    sk_tipo_movimentacao INT NOT NULL,
    sk_empresa_proprietaria_frota INT NOT NULL,
    sk_empresa_patio INT NOT NULL,
    id_estado_veiculo_locacao_origem VARCHAR(50) NOT NULL, -- Chave de negócio para rastrear o evento específico
    id_locacao_origem VARCHAR(50), -- Pode ser NULL, para movimentações não relacionadas a locações
    sistema_origem VARCHAR(50) NOT NULL,
    quant_movimentacoes INT NOT NULL DEFAULT 1, -- Medida aditiva para contagem
    ocupacao_veiculo_horas DECIMAL(10, 2), -- Medida para tempo de ocupação no pátio (se for saída)
    
    -- Chaves Estrangeiras
    FOREIGN KEY (sk_tempo_movimentacao) REFERENCES Dim_Tempo(sk_tempo),
    FOREIGN KEY (sk_veiculo) REFERENCES Dim_Veiculo(sk_veiculo),
    FOREIGN KEY (sk_patio_origem) REFERENCES Dim_Patio(sk_patio),
    FOREIGN KEY (sk_patio_destino) REFERENCES Dim_Patio(sk_patio),
    FOREIGN KEY (sk_tipo_movimentacao) REFERENCES Dim_Tipo_Movimentacao_Patio(sk_tipo_movimentacao),
    FOREIGN KEY (sk_empresa_proprietaria_frota) REFERENCES Dim_Empresa(sk_empresa),
    FOREIGN KEY (sk_empresa_patio) REFERENCES Dim_Empresa(sk_empresa),
    
    UNIQUE KEY uk_movimentacao_origem (id_estado_veiculo_locacao_origem, sistema_origem)
);

-- Criação do banco de dados para a área de staging
CREATE DATABASE IF NOT EXISTS locadora_dw_staging;
