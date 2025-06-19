-- Big Data - Modelagem SBD OLTP

-- Grupo
-- Alice Duarte Faria Ribeiro - DRE 122058907
-- Beatriz Farias do Nascimento – DRE 122053127
-- Gustavo do Amaral Roxo Pereira - DRE 122081146

CREATE DATABASE locadora_veiculos;

USE locadora_veiculos;

-- Criação da tabela EMPRESA
CREATE TABLE EMPRESA (
    id_empresa INT PRIMARY KEY AUTO_INCREMENT,
    nome_empresa VARCHAR(100) NOT NULL UNIQUE,
    cnpj VARCHAR(14) NOT NULL UNIQUE,
    endereco VARCHAR(255) NOT NULL,
    telefone VARCHAR(13)
);

-- Criação da tabela PATIO
CREATE TABLE PATIO (
    id_patio INT PRIMARY KEY AUTO_INCREMENT,
    id_empresa INT NOT NULL,
    nome_patio VARCHAR(100) NOT NULL UNIQUE,
    endereco VARCHAR(255) NOT NULL,
    capacidade_vagas INT NOT NULL CHECK (capacidade_vagas > 0),
    FOREIGN KEY (id_empresa) REFERENCES EMPRESA(id_empresa)
);

-- Criação da tabela VAGA
CREATE TABLE VAGA (
    id_vaga INT PRIMARY KEY AUTO_INCREMENT,
    id_patio INT NOT NULL,
    codigo_vaga VARCHAR(20) NOT NULL,
    status_vaga VARCHAR(20) NOT NULL DEFAULT 'Livre' CHECK (status_vaga IN ('Livre', 'Ocupada', 'Manutencao')),
    FOREIGN KEY (id_patio) REFERENCES PATIO(id_patio),
    UNIQUE (id_patio, codigo_vaga) -- Garante que o código da vaga seja único dentro de um pátio
);

-- Criação da tabela GRUPO_VEICULO
CREATE TABLE GRUPO_VEICULO (
    id_grupo_veiculo INT PRIMARY KEY AUTO_INCREMENT,
    nome_grupo VARCHAR(50) NOT NULL UNIQUE,
    descricao VARCHAR(255),
    valor_diaria_base DECIMAL(10, 2) NOT NULL CHECK (valor_diaria_base > 0)
);

-- Criação da tabela VEICULO
CREATE TABLE VEICULO (
    id_veiculo INT PRIMARY KEY AUTO_INCREMENT,
    id_grupo_veiculo INT NOT NULL,
    id_patio_atual INT, -- Pode ser NULL se o veículo estiver em trânsito ou fora de um pátio específico
    placa VARCHAR(7) NOT NULL UNIQUE,
    chassi VARCHAR(17) NOT NULL UNIQUE,
    marca VARCHAR(50) NOT NULL,
    modelo VARCHAR(50) NOT NULL,
    ano_fabricacao INT NOT NULL CHECK (ano_fabricacao > 1900),
    cor VARCHAR(30),
    tipo_mecanizacao VARCHAR(20) NOT NULL CHECK (tipo_mecanizacao IN ('Manual', 'Automatica')),
    quilometragem_atual DECIMAL(10, 2) NOT NULL CHECK (quilometragem_atual >= 0),
url_foto VARCHAR(255),
    status_veiculo VARCHAR(20) NOT NULL DEFAULT 'Disponivel' CHECK (status_veiculo IN ('Disponivel', 'Alugado', 'Em Manutencao', 'Indisponivel')),
    FOREIGN KEY (id_grupo_veiculo) REFERENCES GRUPO_VEICULO(id_grupo_veiculo),
    FOREIGN KEY (id_patio_atual) REFERENCES PATIO(id_patio)
);

-- Criação da tabela ACESSORIO
CREATE TABLE ACESSORIO (
    id_acessorio INT PRIMARY KEY AUTO_INCREMENT,
    nome_acessorio VARCHAR(100) NOT NULL UNIQUE,
    descricao VARCHAR(255)
);

-- Criação da tabela associativa VEICULO_ACESSORIO
CREATE TABLE VEICULO_ACESSORIO (
    id_veiculo INT NOT NULL,
    id_acessorio INT NOT NULL,
    PRIMARY KEY (id_veiculo, id_acessorio),
    FOREIGN KEY (id_veiculo) REFERENCES VEICULO(id_veiculo),
    FOREIGN KEY (id_acessorio) REFERENCES ACESSORIO(id_acessorio)
);

-- Criação da tabela PRONTUARIO
CREATE TABLE PRONTUARIO (
    id_prontuario INT PRIMARY KEY AUTO_INCREMENT,
    id_veiculo INT NOT NULL UNIQUE,
    data_ultima_revisao DATE,
    quilometragem_ultima_revisao DECIMAL(10, 2) CHECK (quilometragem_ultima_revisao >= 0),
    observacoes VARCHAR(255),
    FOREIGN KEY (id_veiculo) REFERENCES VEICULO(id_veiculo)
);

-- Criação da tabela CLIENTE
CREATE TABLE CLIENTE (
    id_cliente INT PRIMARY KEY AUTO_INCREMENT,
    tipo_cliente VARCHAR(2) NOT NULL CHECK (tipo_cliente IN ('PF', 'PJ')),
    nome_razao_social VARCHAR(100) NOT NULL,
    cpf VARCHAR(11) UNIQUE,
    cnpj VARCHAR(17) UNIQUE,
    endereco VARCHAR(255) NOT NULL,
    telefone VARCHAR(13),
    email VARCHAR(100) UNIQUE,
    CHECK (cpf IS NOT NULL OR cnpj IS NOT NULL)
);

-- Criação da tabela CONDUTOR
CREATE TABLE CONDUTOR (
    id_condutor INT PRIMARY KEY AUTO_INCREMENT,
    id_cliente INT NOT NULL,
    nome_completo VARCHAR(100) NOT NULL,
    numero_cnh VARCHAR(11) NOT NULL UNIQUE,
    categoria_cnh VARCHAR(2) NOT NULL,
    data_expiracao_cnh DATE NOT NULL,
    data_nascimento DATE NOT NULL,
    FOREIGN KEY (id_cliente) REFERENCES CLIENTE(id_cliente)
);

-- Criação da tabela RESERVA
CREATE TABLE RESERVA (
    id_reserva INT PRIMARY KEY AUTO_INCREMENT,
    id_cliente INT NOT NULL,
    id_grupo_veiculo INT NOT NULL,
    id_patio_retirada_previsto INT NOT NULL,
    data_hora_reserva DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_hora_retirada_prevista DATETIME NOT NULL,
    data_hora_devolucao_prevista DATETIME NOT NULL,
    status_reserva VARCHAR(20) NOT NULL DEFAULT 'Confirmada' CHECK (status_reserva IN ('Confirmada', 'Cancelada', 'Em Espera')),
    FOREIGN KEY (id_cliente) REFERENCES CLIENTE(id_cliente),
    FOREIGN KEY (id_grupo_veiculo) REFERENCES GRUPO_VEICULO(id_grupo_veiculo),
    FOREIGN KEY (id_patio_retirada_previsto) REFERENCES PATIO(id_patio)
);

-- Trigger para BEFORE INSERT na tabela RESERVA

DELIMITER //

CREATE TRIGGER trg_reserva_before_insert
BEFORE INSERT ON RESERVA
FOR EACH ROW
BEGIN
    -- Verifica se data_hora_retirada_prevista é maior que data_hora_reserva
    IF NEW.data_hora_retirada_prevista <= NEW.data_hora_reserva THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data e hora de retirada prevista deve ser posterior à data e hora da reserva.';
    END IF;

    -- Verifica se data_hora_devolucao_prevista é maior que data_hora_retirada_prevista
    IF NEW.data_hora_devolucao_prevista <= NEW.data_hora_retirada_prevista THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data e hora de devolução prevista deve ser posterior à data e hora de retirada prevista.';
    END IF;
END;
//

DELIMITER ;

-- Trigger para BEFORE UPDATE na tabela RESERVA

DELIMITER //

CREATE TRIGGER trg_reserva_before_update
BEFORE UPDATE ON RESERVA
FOR EACH ROW
BEGIN
    -- Verifica se data_hora_retirada_prevista é maior que data_hora_reserva
    IF NEW.data_hora_retirada_prevista <= NEW.data_hora_reserva THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data e hora de retirada prevista deve ser posterior à data e hora da reserva.';
    END IF;

    -- Verifica se data_hora_devolucao_prevista é maior que data_hora_retirada_prevista
    IF NEW.data_hora_devolucao_prevista <= NEW.data_hora_retirada_prevista THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data e hora de devolução prevista deve ser posterior à data e hora de retirada prevista.';
    END IF;
END;
//

DELIMITER ;

-- Criação da tabela LOCACAO
CREATE TABLE LOCACAO (
    id_locacao INT PRIMARY KEY AUTO_INCREMENT,
    id_reserva INT UNIQUE, -- Pode ser NULL para locações diretas (sem reserva prévia)
    id_cliente INT NOT NULL,
    id_veiculo INT NOT NULL,
    id_condutor INT NOT NULL,
    id_patio_retirada_real INT NOT NULL,
    id_patio_devolucao_prevista INT NOT NULL,
    id_patio_devolucao_real INT, -- Pode ser NULL até a devolução
    data_hora_retirada_real DATETIME NOT NULL,
    data_hora_devolucao_prevista DATETIME NOT NULL, -- CHECK constraint removed here
    data_hora_devolucao_real DATETIME, -- Pode ser NULL até a devolução
    quilometragem_retirada DECIMAL(10, 2) NOT NULL CHECK (quilometragem_retirada >= 0),
    quilometragem_devolucao DECIMAL(10, 2), -- CHECK constraint removed here
    valor_total_previsto DECIMAL(10, 2) NOT NULL CHECK (valor_total_previsto >= 0),
    valor_total_final DECIMAL(10, 2) CHECK (valor_total_final >= 0), -- NULL até a conclusão da locação
    status_locacao VARCHAR(20) NOT NULL DEFAULT 'Ativa' CHECK (status_locacao IN ('Ativa', 'Concluida', 'Cancelada')),
    FOREIGN KEY (id_reserva) REFERENCES RESERVA(id_reserva),
    FOREIGN KEY (id_cliente) REFERENCES CLIENTE(id_cliente),
    FOREIGN KEY (id_veiculo) REFERENCES VEICULO(id_veiculo),
    FOREIGN KEY (id_condutor) REFERENCES CONDUTOR(id_condutor),
    FOREIGN KEY (id_patio_retirada_real) REFERENCES PATIO(id_patio),
    FOREIGN KEY (id_patio_devolucao_prevista) REFERENCES PATIO(id_patio),
    FOREIGN KEY (id_patio_devolucao_real) REFERENCES PATIO(id_patio)
);

-- Trigger para BEFORE INSERT na tabela LOCACAO
DELIMITER //

CREATE TRIGGER trg_locacao_before_insert
BEFORE INSERT ON LOCACAO
FOR EACH ROW
BEGIN
    IF NEW.data_hora_devolucao_prevista <= NEW.data_hora_retirada_real THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data e hora de devolução prevista deve ser posterior à data e hora de retirada real.';
    END IF;

    IF NEW.data_hora_devolucao_real IS NOT NULL AND NEW.data_hora_devolucao_real < NEW.data_hora_retirada_real THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data e hora de devolução real não pode ser anterior à data e hora de retirada real.';
    END IF;

    IF NEW.quilometragem_devolucao IS NOT NULL AND NEW.quilometragem_devolucao < NEW.quilometragem_retirada THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A quilometragem de devolução não pode ser menor que a quilometragem de retirada.';
    END IF;
END;
//

-- Trigger para BEFORE UPDATE na tabela LOCACAO
CREATE TRIGGER trg_locacao_before_update
BEFORE UPDATE ON LOCACAO
FOR EACH ROW
BEGIN
    IF NEW.data_hora_devolucao_prevista <= NEW.data_hora_retirada_real THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data e hora de devolução prevista deve ser posterior à data e hora de retirada real.';
    END IF;

    IF NEW.data_hora_devolucao_real IS NOT NULL AND NEW.data_hora_devolucao_real < NEW.data_hora_retirada_real THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data e hora de devolução real não pode ser anterior à data e hora de retirada real.';
    END IF;

    IF NEW.quilometragem_devolucao IS NOT NULL AND NEW.quilometragem_devolucao < NEW.quilometragem_retirada THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A quilometragem de devolução não pode ser menor que a quilometragem de retirada.';
    END IF;
END;
//

DELIMITER ;

-- Criação da tabela SEGURO
CREATE TABLE SEGURO (
    id_seguro INT PRIMARY KEY AUTO_INCREMENT,
    nome_seguro VARCHAR(100) NOT NULL UNIQUE,
    descricao VARCHAR(255),
    valor_diario DECIMAL(10, 2) NOT NULL CHECK (valor_diario >= 0)
);


-- Criação da tabela COBRANCA 
CREATE TABLE COBRANCA (
    id_cobranca INT PRIMARY KEY AUTO_INCREMENT,
    id_locacao INT NOT NULL UNIQUE,
    data_cobranca DATE NOT NULL DEFAULT (CURRENT_DATE),
    valor_base DECIMAL(10, 2) NOT NULL CHECK (valor_base >= 0),
    valor_multas_taxas DECIMAL(10, 2) NOT NULL DEFAULT 0.00 CHECK (valor_multas_taxas >= 0),
    valor_seguro DECIMAL(10, 2) NOT NULL DEFAULT 0.00 CHECK (valor_seguro >= 0),
    valor_descontos DECIMAL(10, 2) NOT NULL DEFAULT 0.00 CHECK (valor_descontos >= 0),
    valor_final_cobranca DECIMAL(10, 2) NOT NULL CHECK (valor_final_cobranca >= 0),
    status_pagamento VARCHAR(20) NOT NULL DEFAULT 'Pendente' CHECK (status_pagamento IN ('Pendente', 'Pago', 'Cancelado')),
    data_vencimento DATE NOT NULL,
    data_pagamento DATE,
    FOREIGN KEY (id_locacao) REFERENCES LOCACAO(id_locacao)
);

-- Trigger para BEFORE INSERT na tabela COBRANCA

DELIMITER //

CREATE TRIGGER trg_cobranca_before_insert
BEFORE INSERT ON COBRANCA
FOR EACH ROW
BEGIN
    -- Rule: data_vencimento >= data_cobranca (comparing dates only)
    IF NEW.data_vencimento < DATE(NEW.data_cobranca) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data de vencimento não pode ser anterior à data da cobrança.';
    END IF;

    -- Rule: data_pagamento (if not null) >= data_cobranca
    IF NEW.data_pagamento IS NOT NULL AND NEW.data_pagamento < NEW.data_cobranca THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data de pagamento não pode ser anterior à data da cobrança.';
    END IF;
END;
//

DELIMITER ;

-- Trigger para BEFORE UPDATE na tabela COBRANCA

DELIMITER //

CREATE TRIGGER trg_cobranca_before_update
BEFORE UPDATE ON COBRANCA
FOR EACH ROW
BEGIN
    -- Rule: data_vencimento >= data_cobranca (comparing dates only)
    IF NEW.data_vencimento < DATE(NEW.data_cobranca) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data de vencimento não pode ser anterior à data da cobrança.';
    END IF;

    -- Rule: data_pagamento (if not null) >= data_cobranca
    IF NEW.data_pagamento IS NOT NULL AND NEW.data_pagamento < NEW.data_cobranca THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: A data de pagamento não pode ser anterior à data da cobrança.';
    END IF;
END;
//

DELIMITER ;
