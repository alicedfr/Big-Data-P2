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

-- EXTRAÇÃO DO SISTEMA DE ORIGEM 1 (https://github.com/alicedfr/Big-Data-P2)

INSERT INTO staging.stg_clientes (id_cliente_origem, nome, tipo_cliente, cpf_cnpj, cidade, estado, email, telefone)
SELECT id_cliente, nome_razao_social, tipo_cliente, COALESCE(cpf, cnpj), 'N/A', 'N/A', email, telefone
FROM locadora_empresa1.CLIENTE WHERE id_cliente > (SELECT MAX(id_cliente_origem) FROM staging.stg_clientes);

INSERT INTO staging.stg_veiculos (id_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, grupo, status_veiculo)
SELECT v.id_veiculo, v.placa, v.chassi, v.marca, v.modelo, v.cor, v.tipo_mecanizacao, gv.nome_grupo, v.status_veiculo
FROM locadora_empresa1.VEICULO v JOIN locadora_empresa1.GRUPO_VEICULO gv ON v.id_grupo_veiculo = gv.id_grupo_veiculo;

INSERT INTO staging.stg_patios (id_patio_origem, nome, endereco, capacidade_estimada)
SELECT id_patio, nome_patio, endereco, capacidade_vagas FROM locadora_empresa1.PATIO;

INSERT INTO staging.stg_locacoes (id_locacao, id_cliente, id_veiculo, data_retirada_real, data_devolucao_prevista, data_devolucao_real, valor_previsto, valor_final, status_locacao, patio_retirada, patio_devolucao, cpf_cnpj)
SELECT l.id_locacao, l.id_cliente, l.id_veiculo, l.data_hora_retirada_real, l.data_hora_devolucao_prevista, l.data_hora_devolucao_real, l.valor_total_previsto, c.valor_final_cobranca, l.status_locacao, p_ret.nome_patio, p_dev.nome_patio, cli.cpf_cnpj
FROM locadora_empresa1.LOCACAO l
JOIN locadora_empresa1.COBRANCA c ON l.id_locacao = c.id_locacao
JOIN locadora_empresa1.PATIO p_ret ON l.id_patio_retirada_real = p_ret.id_patio
LEFT JOIN locadora_empresa1.PATIO p_dev ON l.id_patio_devolucao_real = p_dev.id_patio
JOIN (SELECT id_cliente, COALESCE(cpf, cnpj) as cpf_cnpj FROM locadora_empresa1.CLIENTE) cli ON l.id_cliente = cli.id_cliente
WHERE l.data_hora_retirada_real > @last_etl_run_timestamp;

INSERT INTO staging.stg_reservas (id_reserva, id_cliente, id_grupo, data_reserva, data_retirada_prevista, data_devolucao_prevista, patio_retirada, status_reserva)
SELECT r.id_reserva, r.id_cliente, gv.nome_grupo, r.data_hora_reserva, r.data_hora_retirada_prevista, r.data_hora_devolucao_prevista, p.nome_patio, r.status_reserva
FROM locadora_empresa1.RESERVA r
JOIN locadora_empresa1.GRUPO_VEICULO gv ON r.id_grupo_veiculo = gv.id_grupo_veiculo
JOIN locadora_empresa1.PATIO p ON r.id_patio_retirada_previsto = p.id_patio
WHERE r.data_hora_reserva > @last_etl_run_timestamp;

-- EXTRAÇÃO DO SISTEMA DE ORIGEM 2 (https://github.com/brenopprufrj/data_warehouse_project/tree/main/Parte_1)

INSERT INTO staging.stg_clientes (id_cliente_origem, nome, tipo_cliente, cpf_cnpj, email, telefone)
SELECT cliente_id, nome_razao, tipo, cpf_cnpj, email, telefone FROM locadora_empresa2.CLIENTE;

INSERT INTO staging.stg_veiculos (id_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, grupo, status_veiculo)
SELECT v.veiculo_id, v.placa, v.chassis, v.marca, v.modelo, v.cor, v.mecanizacao, gv.nome, 'N/A'
FROM locadora_empresa2.VEICULO v JOIN locadora_empresa2.GRUPO_VEICULO gv ON v.grupo_id = gv.grupo_id;

INSERT INTO staging.stg_patios (id_patio_origem, nome, endereco)
SELECT patio_id, nome, localizacao FROM locadora_empresa2.PATIO;

INSERT INTO staging.stg_locacoes (id_locacao, id_cliente, id_veiculo, data_retirada_real, data_devolucao_prevista, data_devolucao_real, valor_previsto, valor_final, status_locacao, patio_retirada, patio_devolucao, cpf_cnpj)
SELECT l.locacao_id, r.cliente_id, l.veiculo_id, l.data_retirada, l.data_devolucao_prevista, l.data_devolucao_real, c.valor_base, c.valor_final, c.status_pagamento, p_ret.nome, p_dev.nome, cli.cpf_cnpj
FROM locadora_empresa2.LOCACAO l
JOIN locadora_empresa2.RESERVA r ON l.reserva_id = r.reserva_id
JOIN locadora_empresa2.COBRANCA c ON l.locacao_id = c.locacao_id
JOIN locadora_empresa2.PATIO p_ret ON l.patio_saida_id = p_ret.patio_id
LEFT JOIN locadora_empresa2.PATIO p_dev ON l.patio_chegada_id = p_dev.patio_id
JOIN locadora_empresa2.CLIENTE cli ON r.cliente_id = cli.cliente_id
WHERE l.data_retirada > @last_etl_run_timestamp;

INSERT INTO staging.stg_reservas (id_reserva, id_cliente, id_grupo, data_retirada_prevista, data_devolucao_prevista, patio_retirada, status_reserva)
SELECT r.reserva_id, r.cliente_id, gv.nome, r.data_inicio, r.data_fim_previsto, p.nome, r.status
FROM locadora_empresa2.RESERVA r
JOIN locadora_empresa2.GRUPO_VEICULO gv ON r.grupo_id = gv.grupo_id
JOIN locadora_empresa2.PATIO p ON r.patio_retirada_id = p.patio_id
WHERE r.data_inicio > DATE(@last_etl_run_timestamp);

-- EXTRAÇÃO DO SISTEMA DE ORIGEM 3 (https://github.com/BRJCM/Modelagem-SBD-OLTP/tree/main)

INSERT INTO staging.stg_clientes (id_cliente_origem, nome, tipo_cliente, cpf_cnpj, cidade, estado, email, telefone)
SELECT cliente_id, nome_completo, tipo_pessoa, cpf_cnpj, endereco_cidade, endereco_estado, email, telefone
FROM locadora_empresa3.clientes WHERE data_cadastro > @last_etl_run_timestamp;

INSERT INTO staging.stg_veiculos (id_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, grupo, status_veiculo)
SELECT v.veiculo_id, v.placa, v.chassi, v.marca, v.modelo, v.cor, v.cambio, gv.nome_grupo, v.situacao
FROM locadora_empresa3.veiculos v JOIN locadora_empresa3.grupos_veiculos gv ON v.grupo_id = gv.grupo_id;

INSERT INTO staging.stg_patios (id_patio_origem, nome, endereco)
SELECT patio_id, nome, endereco FROM locadora_empresa3.patios;

INSERT INTO staging.stg_locacoes (id_locacao, id_cliente, id_veiculo, data_retirada_real, data_devolucao_prevista, data_devolucao_real, valor_previsto, valor_final, status_locacao, patio_retirada, patio_devolucao, cpf_cnpj)
SELECT l.locacao_id, l.cliente_id, l.veiculo_id, l.retirada_real, l.devolucao_prevista, l.devolucao_real, l.valor_previsto, l.valor_final, c.status_pago, p_ret.nome, p_dev.nome, cli.cpf_cnpj
FROM locadora_empresa3.locacoes l
JOIN locadora_empresa3.cobrancas c ON l.locacao_id = c.locacao_id
JOIN locadora_empresa3.patios p_ret ON l.patio_retirada_id = p_ret.patio_id
LEFT JOIN locadora_empresa3.patios p_dev ON l.patio_devolucao_id = p_dev.patio_id
JOIN locadora_empresa3.clientes cli ON l.cliente_id = cli.cliente_id
WHERE l.retirada_real > @last_etl_run_timestamp;

INSERT INTO staging.stg_reservas (id_reserva, id_cliente, id_grupo, data_reserva, data_retirada_prevista, data_devolucao_prevista, patio_retirada, status_reserva)
SELECT r.reserva_id, r.cliente_id, gv.nome_grupo, r.criado_em, r.retirada_prevista, r.devolucao_prevista, p.nome, r.situacao_reserva
FROM locadora_empresa3.reservas r
JOIN locadora_empresa3.grupos_veiculos gv ON r.grupo_id = gv.grupo_id
JOIN locadora_empresa3.patios p ON r.patio_retirada_id = p.patio_id
WHERE r.criado_em > @last_etl_run_timestamp;

-- EXTRAÇÃO DO SISTEMA DE ORIGEM 4 (https://github.com/rickauer/datawarehouse)

INSERT INTO staging.stg_clientes (id_cliente_origem, nome, tipo_cliente, cpf_cnpj, cidade, estado, email, telefone)
SELECT c.id_cliente, COALESCE(pf.nome_completo, pj.nome_empresa), c.tipo_cliente, COALESCE(pf.cpf, pj.cnpj), 'N/A', 'N/A', c.email, c.telefone_principal
FROM locadora_empresa4.cliente c
LEFT JOIN locadora_empresa4.pessoa_fisica pf ON c.id_cliente = pf.id_cliente
LEFT JOIN locadora_empresa4.pessoa_juridica pj ON c.id_cliente = pj.id_cliente
WHERE c.data_cadastro > @last_etl_run_timestamp;

INSERT INTO staging.stg_veiculos (id_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, grupo, status_veiculo)
SELECT v.id_veiculo, v.placa, v.chassi, v.marca, v.modelo, v.cor, IF(v.mecanizacao, 'Automático', 'Manual'), gv.nome_grupo, v.status_veiculo
FROM locadora_empresa4.veiculo v JOIN locadora_empresa4.grupo_veiculo gv ON v.id_grupo_veiculo = gv.id_grupo_veiculo;

INSERT INTO staging.stg_patios (id_patio_origem, nome, endereco)
SELECT id_patio, nome_patio, endereco_patio FROM locadora_empresa4.patio;

INSERT INTO staging.stg_locacoes (id_locacao, id_cliente, id_veiculo, data_retirada_real, data_devolucao_real, valor_final, status_locacao, patio_retirada, patio_devolucao, cpf_cnpj)
SELECT ct.id_contrato, ct.id_cliente, ct.id_veiculo, ct.data_hora_contrato, NULL, cb.valor, ct.status_locacao, p_ret.nome_patio, p_dev.nome_patio, cli.cpf_cnpj
FROM locadora_empresa4.contrato ct
JOIN locadora_empresa4.cobranca cb ON ct.id_contrato = cb.id_contrato
JOIN locadora_empresa4.patio p_ret ON ct.id_patio_retirada = p_ret.id_patio
LEFT JOIN locadora_empresa4.patio p_dev ON ct.id_patio_devolucao_efetiva = p_dev.id_patio
JOIN (SELECT c.id_cliente, COALESCE(pf.cpf, pj.cnpj) as cpf_cnpj FROM locadora_empresa4.cliente c LEFT JOIN locadora_empresa4.pessoa_fisica pf ON c.id_cliente = pf.id_cliente LEFT JOIN locadora_empresa4.pessoa_juridica pj ON c.id_cliente = pj.id_cliente) cli ON ct.id_cliente = cli.id_cliente
WHERE ct.data_hora_contrato > @last_etl_run_timestamp;

INSERT INTO staging.stg_reservas (id_reserva, id_cliente, id_grupo, data_reserva, data_retirada_prevista, patio_retirada, status_reserva)
SELECT r.id_reserva, co.id_cliente, gv.nome_grupo, r.data_hora_reserva_inicio, r.data_hora_retirada_fim, 'N/A', 'N/A'
FROM locadora_empresa4.reserva r
JOIN locadora_empresa4.veiculo v ON r.id_veiculo = v.id_veiculo
JOIN locadora_empresa4.grupo_veiculo gv ON v.id_grupo_veiculo = gv.id_grupo_veiculo
LEFT JOIN locadora_empresa4.contrato co ON r.id_reserva = co.id_reserva -- LEFT JOIN para pegar reservas não efetivadas
WHERE r.data_hora_reserva_inicio > @last_etl_run_timestamp;