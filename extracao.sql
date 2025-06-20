/********************************************************************************
 * UFRJ/IM/DMA - Big Data & Data Warehouse
 * Avaliação 02, Parte II - Modelagem de Data Warehouse
 *
 * Grupo:
 * - Alice Duarte Faria Ribeiro (DRE 122058907)
 * - Beatriz Farias do Nascimento (DRE 122053127)
 * - Gustavo do Amaral Roxo Pereira (DRE 122081146)
 *
 * Fase de EXTRAÇÃO
 * Simula a extração de dados das 4 fontes OLTP para a Staging Area.
 ********************************************************************************/

-- Limpa as tabelas de staging antes de uma nova carga para evitar duplicidade.
SET FOREIGN_KEY_CHECKS = 0; -- Desabilita a checagem de chaves estrangeiras para permitir o TRUNCATE
TRUNCATE TABLE staging.clientes;
TRUNCATE TABLE staging.veiculos;
TRUNCATE TABLE staging.patios;
TRUNCATE TABLE staging.locacoes;
TRUNCATE TABLE staging.reservas;
SET FOREIGN_KEY_CHECKS = 1; -- Reabilita a checagem de chaves estrangeiras

SET @last_etl_run_timestamp = '2025-06-19 00:00:00';

-- Inserindo dados de amostra para simular a extração das 4 fontes.
-- Fonte 1: modelofisico.sql (Nosso Grupo)

-- Extração de Clientes Novos/Modificados
-- Assumindo que a tabela `CLIENTE` tem um campo `data_cadastro` para controle incremental.
INSERT INTO staging.stg_clientes (id_cliente_origem, nome, tipo_cliente, cpf_cnpj, cidade, estado, email, telefone)
SELECT
    c.id_cliente,
    c.nome_razao_social,
    c.tipo_cliente,
    COALESCE(c.cpf, c.cnpj),
    SUBSTRING_INDEX(SUBSTRING_INDEX(c.endereco, ',', -2), ',', 1), -- Lógica para extrair cidade
    SUBSTRING_INDEX(c.endereco, ',', -1), -- Lógica para extrair estado
    c.email,
    c.telefone
FROM locadora_empresa1.CLIENTE c
WHERE c.id_cliente > (SELECT MAX(id_cliente_origem) FROM staging.stg_clientes WHERE fonte = 'Empresa 1'); -- Exemplo de controle por ID

-- Extração de Veículos
INSERT INTO staging.stg_veiculos (id_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, grupo, status_veiculo)
SELECT
    v.id_veiculo,
    v.placa,
    v.chassi,
    v.marca,
    v.modelo,
    v.cor,
    v.tipo_mecanizacao,
    gv.nome_grupo,
    v.status_veiculo
FROM locadora_empresa1.VEICULO v
JOIN locadora_empresa1.GRUPO_VEICULO gv ON v.id_grupo_veiculo = gv.id_grupo_veiculo;

-- Extração de Pátios
INSERT INTO staging.stg_patios (id_patio_origem, nome, endereco, capacidade_estimada)
SELECT
    id_patio,
    nome_patio,
    endereco,
    capacidade_vagas
FROM locadora_empresa1.PATIO;

-- Extração de Locações Novas
INSERT INTO staging.stg_locacoes (id_locacao, id_cliente, id_veiculo, data_retirada_real, data_devolucao_prevista, data_devolucao_real, valor_previsto, valor_final, status_locacao, patio_retirada, patio_devolucao, cpf_cnpj)
SELECT
    l.id_locacao,
    l.id_cliente,
    l.id_veiculo,
    l.data_hora_retirada_real,
    l.data_hora_devolucao_prevista,
    l.data_hora_devolucao_real,
    l.valor_total_previsto,
    c.valor_final_cobranca,
    l.status_locacao,
    p_ret.nome_patio,
    p_dev.nome_patio,
    cli.cpf_cnpj
FROM locadora_empresa1.LOCACAO l
JOIN locadora_empresa1.COBRANCA c ON l.id_locacao = c.id_locacao
JOIN locadora_empresa1.PATIO p_ret ON l.id_patio_retirada_real = p_ret.id_patio
LEFT JOIN locadora_empresa1.PATIO p_dev ON l.id_patio_devolucao_real = p_dev.id_patio
JOIN (SELECT id_cliente, COALESCE(cpf, cnpj) as cpf_cnpj FROM locadora_empresa1.CLIENTE) cli ON l.id_cliente = cli.id_cliente
WHERE l.data_hora_retirada_real > @last_etl_run_timestamp;

-- Fonte 2: script.sql (Grupo Kauer)
INSERT INTO staging.stg_clientes (id_cliente_origem, nome, tipo_cliente, cpf_cnpj, cidade, estado, email, telefone)
SELECT
    c.id_cliente,
    COALESCE(pf.nome_completo, pj.nome_empresa),
    c.tipo_cliente,
    COALESCE(pf.cpf, pj.cnpj),
    'N/A', 'N/A', c.email, c.telefone_principal
FROM locadora_empresa2.cliente c
LEFT JOIN locadora_empresa2.pessoa_fisica pf ON c.id_cliente = pf.id_cliente
LEFT JOIN locadora_empresa2.pessoa_juridica pj ON c.id_cliente = pj.id_cliente
WHERE c.data_cadastro > @last_etl_run_timestamp;

INSERT INTO staging.stg_locacoes (id_locacao, id_cliente, id_veiculo, data_retirada_real, data_devolucao_real, valor_final, status_locacao, patio_retirada, patio_devolucao, cpf_cnpj)
SELECT
    ct.id_contrato, ct.id_cliente, ct.id_veiculo, ct.data_hora_contrato,
    NULL, cb.valor, ct.status_locacao, p_ret.nome_patio, p_dev.nome_patio,
    cli.cpf_cnpj
FROM locadora_empresa2.contrato ct
JOIN locadora_empresa2.cobranca cb ON ct.id_contrato = cb.id_contrato
JOIN locadora_empresa2.patio p_ret ON ct.id_patio_retirada = p_ret.id_patio
LEFT JOIN locadora_empresa2.patio p_dev ON ct.id_patio_devolucao_efetiva = p_dev.id_patio
JOIN (
    SELECT c.id_cliente, COALESCE(pf.cpf, pj.cnpj) as cpf_cnpj
    FROM locadora_empresa2.cliente c
    LEFT JOIN locadora_empresa2.pessoa_fisica pf ON c.id_cliente = pf.id_cliente
    LEFT JOIN locadora_empresa2.pessoa_juridica pj ON c.id_cliente = pj.id_cliente
) cli ON ct.id_cliente = cli.id_cliente
WHERE ct.data_hora_contrato > @last_etl_run_timestamp;

-- Fonte 3: schema.sql (Grupo Medeiro)
INSERT INTO staging.stg_clientes (id_cliente_origem, nome, tipo_cliente, cpf_cnpj, cidade, estado, email, telefone)
SELECT
    cliente_id, nome_completo, tipo_pessoa, cpf_cnpj, endereco_cidade, endereco_estado, email, telefone
FROM locadora_empresa3.clientes
WHERE data_cadastro > @last_etl_run_timestamp;

INSERT INTO staging.stg_locacoes (id_locacao, id_cliente, id_veiculo, data_retirada_real, data_devolucao_prevista, data_devolucao_real, valor_previsto, valor_final, status_locacao, patio_retirada, patio_devolucao, cpf_cnpj)
SELECT
    l.locacao_id, l.cliente_id, l.veiculo_id, l.retirada_real, l.devolucao_prevista,
    l.devolucao_real, l.valor_previsto, l.valor_final, c.status_pago,
    p_ret.nome, p_dev.nome, cli.cpf_cnpj
FROM locadora_empresa3.locacoes l
JOIN locadora_empresa3.cobrancas c ON l.locacao_id = c.locacao_id
JOIN locadora_empresa3.patios p_ret ON l.patio_retirada_id = p_ret.patio_id
LEFT JOIN locadora_empresa3.patios p_dev ON l.patio_devolucao_id = p_dev.patio_id
JOIN locadora_empresa3.clientes cli ON l.cliente_id = cli.cliente_id
WHERE l.retirada_real > @last_etl_run_timestamp;

-- Fonte 4: script.sql (Grupo Manhães)
INSERT INTO staging.stg_clientes (id_cliente_origem, nome, tipo_cliente, cpf_cnpj, email, telefone)
SELECT
    cliente_id, nome_razao, tipo, cpf_cnpj, email, telefone
FROM locadora_empresa4.CLIENTE; -- Assumindo carga full por falta de data de controle

INSERT INTO staging.stg_locacoes (id_locacao, id_cliente, id_veiculo, data_retirada_real, data_devolucao_prevista, data_devolucao_real, valor_previsto, valor_final, status_locacao, patio_retirada, patio_devolucao, cpf_cnpj)
SELECT
    l.locacao_id, r.cliente_id, l.veiculo_id, l.data_retirada, l.data_devolucao_prevista,
    l.data_devolucao_real, c.valor_base, c.valor_final, c.status_pagamento,
    p_ret.nome, p_dev.nome, cli.cpf_cnpj
FROM locadora_empresa4.LOCACAO l
JOIN locadora_empresa4.RESERVA r ON l.reserva_id = r.reserva_id
JOIN locadora_empresa4.COBRANCA c ON l.locacao_id = c.locacao_id
JOIN locadora_empresa4.PATIO p_ret ON l.patio_saida_id = p_ret.patio_id
LEFT JOIN locadora_empresa4.PATIO p_dev ON l.patio_chegada_id = p_dev.patio_id
JOIN locadora_empresa4.CLIENTE cli ON r.cliente_id = cli.cliente_id
WHERE l.data_retirada > @last_etl_run_timestamp;
