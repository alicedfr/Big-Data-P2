/********************************************************************************
 * UFRJ/IM/DMA - Big Data & Data Warehouse
 * Avaliação 02, Parte II - Modelagem de Data Warehouse
 *
 * Grupo:
 * - Alice Duarte Faria Ribeiro (DRE 122058907)
 * - Beatriz Farias do Nascimento (DRE 122053127)
 * - Gustavo do Amaral Roxo Pereira (DRE 122081146)
 *
 * Fase de TRANSFORMAÇÃO
 * Lê os dados da Staging Area, aplica as regras de conformação e
 * limpeza, e carrega as tabelas de DIMENSÃO do Data Warehouse.
 ********************************************************************************/

-- Seleciona o banco de dados do Data Warehouse
USE dw;

-- Limpa as tabelas de dimensão antes da carga.
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE dim_cliente;
TRUNCATE TABLE dim_veiculo;
TRUNCATE TABLE dim_patio;
TRUNCATE TABLE dim_tempo;
-- Limpa as tabelas de fato também, pois elas dependem das dimensões
TRUNCATE TABLE fato_locacoes;
TRUNCATE TABLE fato_reservas;
TRUNCATE TABLE fato_ocupacao_patio;
SET FOREIGN_KEY_CHECKS = 1;


-- Passo 1: Carga da dim_cliente
-- Insere clientes da staging no DW, evitando duplicatas.
INSERT INTO dim_cliente (nome, tipo_cliente, cpf_cnpj, cidade, estado, email, telefone)
SELECT DISTINCT
    s.nome,
    s.tipo_cliente,
    s.cpf_cnpj,
    s.cidade,
    s.estado,
    s.email,
    s.telefone
FROM staging.stg_clientes s
LEFT JOIN dw.dim_cliente d ON s.cpf_cnpj = d.cpf_cnpj
WHERE d.id_cliente IS NULL; -- Insere apenas se o cliente (pelo CPF/CNPJ) não existir no DW


-- Passo 2: Carga da dim_veiculo
-- Insere veículos da staging no DW, evitando duplicatas.
INSERT INTO dim_veiculo (placa, chassi, marca, modelo, cor, tipo_mecanizacao, grupo, status_veiculo)
SELECT DISTINCT
    s.placa,
    s.chassi,
    s.marca,
    s.modelo,
    s.cor,
    s.tipo_mecanizacao,
    s.grupo,
    s.status_veiculo
FROM staging.stg_veiculos s
LEFT JOIN dw.dim_veiculo d ON s.placa = d.placa
WHERE d.id_veiculo IS NULL; -- Insere apenas se o veículo (pela placa) não existir no DW


-- Passo 3: Carga da dim_patio
-- Insere pátios da staging no DW, evitando duplicatas.
INSERT INTO dim_patio (nome, endereco, capacidade_estimada)
SELECT DISTINCT
    s.nome,
    s.endereco,
    s.capacidade_estimada
FROM staging.stg_patios s
LEFT JOIN dw.dim_patio d ON s.nome = d.nome
WHERE d.id_patio IS NULL; -- Insere apenas se o pátio (pelo nome) não existir no DW


-- Passo 4: Carga da dim_tempo
-- Este é um passo crucial. A tabela de tempo deve ser populada com todos os dias
-- do período de interesse para que as tabelas de fatos possam se juntar a ela.
-- O script abaixo popula a tabela para o ano de 2025.
DELIMITER $$
CREATE PROCEDURE PopulateDimTempo()
BEGIN
    DECLARE v_full_date DATE;
    SET v_full_date = '2025-01-01';
    WHILE v_full_date <= '2025-12-31' DO
        INSERT INTO dim_tempo (data_completa, ano, mes, dia, dia_semana, trimestre)
        VALUES (
            v_full_date,
            YEAR(v_full_date),
            MONTH(v_full_date),
            DAY(v_full_date),
            DAYNAME(v_full_date),
            QUARTER(v_full_date)
        );
        SET v_full_date = DATE_ADD(v_full_date, INTERVAL 1 DAY);
    END WHILE;
END$$
DELIMITER ;

CALL PopulateDimTempo();
DROP PROCEDURE PopulateDimTempo;