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

-- Inserindo dados de amostra para simular a extração das 4 fontes.
-- Fonte 1: modelofisico.sql (Nosso Grupo)
INSERT INTO staging.clientes (fonte_dados, id_cliente_origem, nome_razao_social, cpf, cidade, estado) VALUES
('Fonte 1', '101', 'João Silva', '111.111.111-11', 'Rio de Janeiro', 'RJ');
INSERT INTO staging.veiculos (fonte_dados, id_veiculo_origem, placa, marca, modelo, ano_fabricacao, grupo_veiculo, mecanizacao_texto, empresa_proprietaria) VALUES
('Fonte 1', 'V1', 'ABC1D23', 'Fiat', 'Mobi', 2022, 'Econômico', 'Manual', 'Empresa 1');
INSERT INTO staging.patios (fonte_dados, id_patio_origem, nome_patio, cidade_patio, empresa_gestora) VALUES
('Fonte 1', 'P1', 'Aeroporto do Galeão', 'Rio de Janeiro', 'Empresa 1'),
('Fonte 1', 'P2', 'Santos Dumont', 'Rio de Janeiro', 'Empresa 2');
INSERT INTO staging.locacoes (fonte_dados, id_locacao_origem, id_cliente_origem, id_veiculo_origem, id_patio_retirada_origem, id_patio_devolucao_origem, data_retirada, data_devolucao, valor_total_pago) VALUES
('Fonte 1', 'L1', '101', 'V1', 'P1', 'P2', '2025-05-10', '2025-05-15', 550.00);

-- Fonte 2: script.sql (Grupo Kauer)
INSERT INTO staging.clientes (fonte_dados, id_cliente_origem, nome_razao_social, cpf, cidade, estado) VALUES
('Fonte 2', '201', 'Maria Oliveira', '222.222.222-22', 'São Paulo', 'SP');
INSERT INTO staging.veiculos (fonte_dados, id_veiculo_origem, placa, marca, modelo, ano_fabricacao, grupo_veiculo, mecanizacao_bool, empresa_proprietaria) VALUES
('Fonte 2', 'V55', 'DEF4E56', 'Hyundai', 'Creta', 2023, 'SUV', true, 'Empresa 2');
INSERT INTO staging.patios (fonte_dados, id_patio_origem, nome_patio, cidade_patio, empresa_gestora) VALUES
('Fonte 2', 'P2', 'Santos Dumont', 'Rio de Janeiro', 'Empresa 2');
INSERT INTO staging.locacoes (fonte_dados, id_locacao_origem, id_cliente_origem, id_veiculo_origem, id_patio_retirada_origem, id_patio_devolucao_origem, data_retirada, data_devolucao, valor_total_pago) VALUES
('Fonte 2', 'L500', '201', 'V55', 'P2', 'P2', '2025-06-01', '2025-06-10', 1200.75);

-- Fonte 3: schema.sql (Grupo Medeiro)
INSERT INTO staging.clientes (fonte_dados, id_cliente_origem, nome_razao_social, cpf_cnpj_unificado, cidade, estado) VALUES
('Fonte 3', '33', 'Carlos Pereira', '33333333333', 'Belo Horizonte', 'MG');
INSERT INTO staging.veiculos (fonte_dados, id_veiculo_origem, placa, marca, modelo, ano_fabricacao, grupo_veiculo, mecanizacao_texto, empresa_proprietaria) VALUES
('Fonte 3', 'V99', 'GHI7F89', 'Jeep', 'Renegade', 2021, 'SUV', 'Automática', 'Empresa 3');
INSERT INTO staging.patios (fonte_dados, id_patio_origem, nome_patio, cidade_patio, empresa_gestora) VALUES
('Fonte 3', 'P3', 'Rodoviária', 'Rio de Janeiro', 'Empresa 3');
INSERT INTO staging.locacoes (fonte_dados, id_locacao_origem, id_cliente_origem, id_veiculo_origem, id_patio_retirada_origem, id_patio_devolucao_origem, data_retirada, data_devolucao, valor_total_pago) VALUES
('Fonte 3', 'L900', '33', 'V99', 'P3', 'P1', '2025-06-05', '2025-06-08', 980.50);

-- Fonte 4: script.sql (Grupo Manhães)
INSERT INTO staging.clientes (fonte_dados, id_cliente_origem, nome_razao_social, cpf_cnpj_unificado, cidade, estado) VALUES
('Fonte 4', '404', 'Ana Souza', '44444444414', 'Niterói', 'RJ');
INSERT INTO staging.veiculos (fonte_dados, id_veiculo_origem, placa, marca, modelo, ano_fabricacao, grupo_veiculo, mecanizacao_texto, empresa_proprietaria) VALUES
('Fonte 4', 'V4', 'JKL0M12', 'VW', 'Nivus', 2023, 'SUV Compacto', 'Auto', 'Empresa 4');
INSERT INTO staging.patios (fonte_dados, id_patio_origem, nome_patio, cidade_patio, empresa_gestora) VALUES
('Fonte 4', 'P4', 'Shopping Rio Sul', 'Rio de Janeiro', 'Empresa 4');
INSERT INTO staging.locacoes (fonte_dados, id_locacao_origem, id_cliente_origem, id_veiculo_origem, id_patio_retirada_origem, id_patio_devolucao_origem, data_retirada, data_devolucao, valor_total_pago) VALUES
('Fonte 4', 'L40', '404', 'V4', 'P4', 'P4', '2025-06-12', '2025-06-18', 1500.00);
