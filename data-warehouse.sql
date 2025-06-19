/********************************************************************************
 * UFRJ/IM/DMA - Big Data & Data Warehouse
 * Avaliação 02, Parte II - Modelagem de Data Warehouse
 *
 * Grupo:
 * - Alice Duarte Faria Ribeiro (DRE 122058907)
 * - Beatriz Farias do Nascimento (DRE 122053127)
 * - Gustavo do Amaral Roxo Pereira (DRE 122081146)
 *
 * Cria as tabelas do Data Warehouse (Dimensões e Fatos).
 ********************************************************************************/

CREATE SCHEMA IF NOT EXISTS `dw`;
USE `dw`;

-- Tabela: Dim_Tempo
CREATE TABLE IF NOT EXISTS `dw`.`Dim_Tempo` (
    `SK_Data`          INT PRIMARY KEY, -- Chave no formato YYYYMMDD
    `Data_Completa`    DATE NOT NULL,
    `Ano`              INT NOT NULL,
    `Trimestre`        INT NOT NULL,
    `Mes`              INT NOT NULL,
    `Dia`              INT NOT NULL,
    `Nome_Mes`         VARCHAR(20) NOT NULL,
    `Nome_Dia_Semana`  VARCHAR(20) NOT NULL,
    `Flag_Fim_De_Semana` CHAR(3) NOT NULL
);

-- Tabela: Dim_Cliente
CREATE TABLE IF NOT EXISTS `dw`.`Dim_Cliente` (
    `SK_Cliente`       INT PRIMARY KEY AUTO_INCREMENT,
    `NK_Documento`     VARCHAR(20) NOT NULL,
    `Nome_Cliente`     VARCHAR(255) NOT NULL,
    `Email`            VARCHAR(255),
    `Cidade_Cliente`   VARCHAR(100),
    `Estado_Cliente`   VARCHAR(50),
    `Data_Inicio_DW`   DATE NOT NULL,
    `Data_Fim_DW`      DATE
);

-- Tabela: Dim_Veiculo
CREATE TABLE IF NOT EXISTS `dw`.`Dim_Veiculo` (
    `SK_Veiculo`           INT PRIMARY KEY AUTO_INCREMENT,
    `NK_Placa`             VARCHAR(12) NOT NULL,
    `Marca`                VARCHAR(100) NOT NULL,
    `Modelo`               VARCHAR(100) NOT NULL,
    `Ano_Fabricacao`       INT NOT NULL,
    `Cor`                  VARCHAR(50),
    `Grupo_Veiculo`        VARCHAR(100) NOT NULL,
    `Tipo_Mecanizacao`     VARCHAR(20) NOT NULL,
    `Empresa_Proprietaria` VARCHAR(100) NOT NULL
);

-- Tabela: Dim_Patio
CREATE TABLE IF NOT EXISTS `dw`.`Dim_Patio` (
    `SK_Patio`         INT PRIMARY KEY AUTO_INCREMENT,
    `Nome_Patio`       VARCHAR(255) NOT NULL,
    `Endereco_Patio`   VARCHAR(500),
    `Cidade_Patio`     VARCHAR(100) NOT NULL,
    `Empresa_Gestora`  VARCHAR(100) NOT NULL
);

-- Tabela: Fato_Locacoes
CREATE TABLE IF NOT EXISTS `dw`.`Fato_Locacoes` (
    `SK_Locacao`           INT PRIMARY KEY AUTO_INCREMENT,
    `SK_Cliente`           INT NOT NULL,
    `SK_Veiculo`           INT NOT NULL,
    `SK_Patio_Retirada`    INT NOT NULL,
    `SK_Patio_Entrega`     INT NOT NULL,
    `SK_Data_Retirada`     INT NOT NULL,
    `SK_Data_Entrega`      INT NOT NULL,
    `Valor_Total_Pago`     DECIMAL(12, 2) NOT NULL,
    `Duracao_Dias_Locacao` INT NOT NULL,
    `Origem_Veiculo`       VARCHAR(20) NOT NULL,
    FOREIGN KEY (`SK_Cliente`) REFERENCES `dw`.`Dim_Cliente`(`SK_Cliente`),
    FOREIGN KEY (`SK_Veiculo`) REFERENCES `dw`.`Dim_Veiculo`(`SK_Veiculo`),
    FOREIGN KEY (`SK_Patio_Retirada`) REFERENCES `dw`.`Dim_Patio`(`SK_Patio`),
    FOREIGN KEY (`SK_Patio_Entrega`) REFERENCES `dw`.`Dim_Patio`(`SK_Patio`),
    FOREIGN KEY (`SK_Data_Retirada`) REFERENCES `dw`.`Dim_Tempo`(`SK_Data`),
    FOREIGN KEY (`SK_Data_Entrega`) REFERENCES `dw`.`Dim_Tempo`(`SK_Data`)
);
