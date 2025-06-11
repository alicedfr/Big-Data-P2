-- SQL Script para Criação da Área de Staging (Staging Area) do Data Warehouse
-- Este script cria tabelas temporárias que espelham as estruturas dos sistemas OLTP
-- para receber os dados brutos antes da transformação.

-- Usar um banco de dados específico para o staging
CREATE DATABASE IF NOT EXISTS locadora_dw_staging;
USE locadora_dw_staging;

-- Tabela de Staging para EMPRESA
CREATE TABLE IF NOT EXISTS stg_empresa (
    id_empresa_origem VARCHAR(50),
    nome_empresa VARCHAR(100),
    cnpj VARCHAR(18),
    endereco VARCHAR(255),
    telefone VARCHAR(20),
    sistema_origem VARCHAR(50), -- Identificador do sistema de origem (ex: 'Sistema_GrupoA', 'Sistema_GrupoB')
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para PATIO
CREATE TABLE IF NOT EXISTS stg_patio (
    id_patio_origem VARCHAR(50),
    id_empresa_origem VARCHAR(50), -- FK para a empresa proprietária do pátio
    nome_patio VARCHAR(100),
    endereco VARCHAR(255),
    capacidade_vagas INT,
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para GRUPO_VEICULO
CREATE TABLE IF NOT EXISTS stg_grupo_veiculo (
    id_grupo_veiculo_origem VARCHAR(50),
    nome_grupo VARCHAR(50),
    descricao VARCHAR(255),
    valor_diaria_base DECIMAL(10, 2),
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para VEICULO
CREATE TABLE IF NOT EXISTS stg_veiculo (
    id_veiculo_origem VARCHAR(50),
    id_grupo_veiculo_origem VARCHAR(50),
    id_patio_atual_origem VARCHAR(50), -- ID do pátio atual no sistema de origem
    placa VARCHAR(10),
    chassi VARCHAR(20),
    marca VARCHAR(50),
    modelo VARCHAR(50),
    ano_fabricacao INT,
    cor VARCHAR(30),
    tipo_mecanizacao VARCHAR(20),
    quilometragem_atual DECIMAL(10, 2),
    status_veiculo VARCHAR(20),
    url_foto_principal VARCHAR(255),
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para CLIENTE
CREATE TABLE IF NOT EXISTS stg_cliente (
    id_cliente_origem VARCHAR(50),
    tipo_cliente VARCHAR(5),
    nome_razao_social VARCHAR(100),
    cpf VARCHAR(11),
    cnpj VARCHAR(17),
    endereco VARCHAR(255),
    telefone VARCHAR(13),
    email VARCHAR(100),
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para CONDUTOR
CREATE TABLE IF NOT EXISTS stg_condutor (
    id_condutor_origem VARCHAR(50),
    id_cliente_origem VARCHAR(50), -- Ajustado para ser id_cliente apenas, conforme seu DDL mais recente
    id_funcionario_pj_origem VARCHAR(50), -- Manter se for mapeado de outros grupos
    nome_completo VARCHAR(100),
    numero_cnh VARCHAR(20),
    categoria_cnh VARCHAR(10),
    data_expiracao_cnh DATE,
    data_nascimento DATE,
    nacionalidade VARCHAR(50),
    tipo_documento_habilitacao VARCHAR(50),
    pais_emissao_cnh VARCHAR(100),
    data_entrada_brasil DATE,
    flag_traducao_juramentada BOOLEAN,
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para RESERVA
CREATE TABLE IF NOT EXISTS stg_reserva (
    id_reserva_origem VARCHAR(50),
    id_cliente_origem VARCHAR(50),
    id_grupo_veiculo_origem VARCHAR(50),
    id_patio_retirada_previsto_origem VARCHAR(50),
    id_patio_devolucao_previsto_origem VARCHAR(50), -- Adicionado para alinhar com modelo dimensional completo
    data_hora_reserva DATETIME,
    data_hora_retirada_prevista DATETIME,
    data_hora_devolucao_prevista DATETIME,
    status_reserva VARCHAR(20),
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para LOCACAO
CREATE TABLE IF NOT EXISTS stg_locacao (
    id_locacao_origem VARCHAR(50),
    id_reserva_origem VARCHAR(50), -- Pode ser NULL
    id_cliente_origem VARCHAR(50),
    id_veiculo_origem VARCHAR(50),
    id_condutor_origem VARCHAR(50),
    id_patio_retirada_real_origem VARCHAR(50),
    id_patio_devolucao_prevista_origem VARCHAR(50),
    id_patio_devolucao_real_origem VARCHAR(50), -- Pode ser NULL
    data_hora_retirada_real DATETIME,
    data_hora_devolucao_prevista DATETIME,
    data_hora_devolucao_real DATETIME, -- Pode ser NULL
    quilometragem_retirada DECIMAL(10, 2),
    quilometragem_devolucao DECIMAL(10, 2), -- Pode ser NULL
    valor_total_previsto DECIMAL(10, 2),
    valor_total_final DECIMAL(10, 2), -- Pode ser NULL
    status_locacao VARCHAR(20),
    id_seguro_contratado_origem INT, -- ID do seguro contratado (se for um plano único, ou NULL)
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para SEGURO (tipos de seguro)
CREATE TABLE IF NOT EXISTS stg_seguro (
    id_seguro_origem VARCHAR(50),
    nome_seguro VARCHAR(100),
    descricao TEXT,
    valor_diario DECIMAL(10, 2),
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para COBRANCA (mantida para detalhe, mas não é principal para movimentação de pátio)
CREATE TABLE IF NOT EXISTS stg_cobranca (
    id_cobranca_origem VARCHAR(50),
    id_locacao_origem VARCHAR(50),
    data_cobranca DATETIME, -- Ajustado para DATETIME para flexibilidade
    valor_base DECIMAL(10, 2),
    valor_multas_taxas DECIMAL(10, 2),
    valor_seguro DECIMAL(10, 2),
    valor_descontos DECIMAL(10, 2),
    valor_final_cobranca DECIMAL(10, 2),
    status_pagamento VARCHAR(20),
    data_vencimento DATE,
    data_pagamento DATETIME, -- Ajustado para DATETIME para flexibilidade
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Staging para ESTADO_VEICULO_LOCACAO (para eventos de movimentação de pátio/estado)
-- Esta tabela pode ser populada a partir de diferentes fontes (logs, fotos, prontuários)
-- para registrar eventos de entrada/saída de veículos nos pátios ou mudança de estado.
CREATE TABLE IF NOT EXISTS stg_estado_veiculo_locacao (
    id_estado_veiculo_locacao_origem VARCHAR(50), -- ID do evento de origem (ex: id de prontuário, id de foto, id de log de pátio)
    id_locacao_origem VARCHAR(50), -- Opcional: ID da locação associada ao evento
    id_veiculo_origem VARCHAR(50), -- ID do veículo envolvido
    id_patio_origem VARCHAR(50), -- Pátio onde o evento ocorreu
    tipo_registro VARCHAR(50), -- Ex: 'Entrega', 'Devolucao', 'Manutencao', 'EntradaPatio', 'SaidaPatio'
    data_hora_registro DATETIME,
    nivel_combustivel DECIMAL(3, 2),
    condicao_geral TEXT,
    observacoes TEXT,
    quilometragem_evento DECIMAL(10,2), -- Quilometragem no momento do evento
    sistema_origem VARCHAR(50),
    data_carga DATETIME DEFAULT CURRENT_TIMESTAMP
);
