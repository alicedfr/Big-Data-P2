/********************************************************************************
 * UFRJ/IM/DMA - Big Data & Data Warehouse
 * Avaliação 02, Parte II - Modelagem de Data Warehouse
 *
 * Grupo:
 * - Alice Duarte Faria Ribeiro (DRE 122058907)
 * - Beatriz Farias do Nascimento (DRE 122053127)
 * - Gustavo do Amaral Roxo Pereira (DRE 122081146)
 *
 * Cria as tabelas da área de Staging (Staging Area).
 ********************************************************************************/

-- Cria o schema (database) se ele não existir.
CREATE SCHEMA IF NOT EXISTS `staging`;
USE `staging`;

-- Tabela de Staging para Clientes
CREATE TABLE IF NOT EXISTS `staging`.`clientes` (
    `fonte_dados`          VARCHAR(50),
    `id_cliente_origem`    VARCHAR(100),
    `nome_razao_social`    VARCHAR(255),
    `cpf`                  VARCHAR(14),
    `cnpj`                 VARCHAR(20),
    `cpf_cnpj_unificado`   VARCHAR(20),
    `email`                VARCHAR(255),
    `cidade`               VARCHAR(100),
    `estado`               VARCHAR(50)
);

-- Tabela de Staging para Veículos
CREATE TABLE IF NOT EXISTS `staging`.`veiculos` (
    `fonte_dados`          VARCHAR(50),
    `id_veiculo_origem`    VARCHAR(100),
    `placa`                VARCHAR(12) NOT NULL,
    `chassi`               VARCHAR(50),
    `marca`                VARCHAR(100),
    `modelo`               VARCHAR(100),
    `ano_fabricacao`       INTEGER,
    `cor`                  VARCHAR(50),
    `grupo_veiculo`        VARCHAR(100),
    `mecanizacao_texto`    VARCHAR(20),
    `mecanizacao_bool`     BOOLEAN,
    `empresa_proprietaria` VARCHAR(100)
);

-- Tabela de Staging para Pátios
CREATE TABLE IF NOT EXISTS `staging`.`patios` (
    `fonte_dados`          VARCHAR(50),
    `id_patio_origem`      VARCHAR(100),
    `nome_patio`           VARCHAR(255) NOT NULL,
    `endereco_patio`       VARCHAR(500),
    `cidade_patio`         VARCHAR(100),
    `empresa_gestora`      VARCHAR(100)
);

-- Tabela de Staging para Locações
CREATE TABLE IF NOT EXISTS `staging`.`locacoes` (
    `fonte_dados`                  VARCHAR(50),
    `id_locacao_origem`            VARCHAR(100),
    `id_cliente_origem`            VARCHAR(100),
    `id_veiculo_origem`            VARCHAR(100),
    `id_patio_retirada_origem`     VARCHAR(100),
    `id_patio_devolucao_origem`    VARCHAR(100),
    `data_retirada`                DATETIME,
    `data_devolucao`               DATETIME,
    `valor_total_pago`             DECIMAL(12, 2)
);

-- Tabela de Staging para Reservas
CREATE TABLE IF NOT EXISTS `staging`.`reservas` (
    `fonte_dados`                  VARCHAR(50),
    `id_reserva_origem`            VARCHAR(100),
    `id_cliente_origem`            VARCHAR(100),
    `grupo_veiculo_desejado`       VARCHAR(100),
    `id_patio_retirada_origem`     VARCHAR(100),
    `data_reserva`                 DATETIME,
    `data_retirada_prevista`       DATETIME,
    `duracao_prevista_dias`        INTEGER
);
