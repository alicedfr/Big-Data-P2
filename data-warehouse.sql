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

CREATE SCHEMA IF NOT EXISTS dw;
USE dw;

-- Dimensão Cliente
CREATE TABLE IF NOT EXISTS dim_cliente (
  id_cliente INT PRIMARY KEY AUTO_INCREMENT,
  nome VARCHAR(100),
  tipo_cliente VARCHAR(20),
  cpf_cnpj VARCHAR(20),
  cidade VARCHAR(100),
  estado VARCHAR(50),
  email VARCHAR(100),
  telefone VARCHAR(20)
);

-- Dimensão Veículo
CREATE TABLE IF NOT EXISTS dim_veiculo (
  id_veiculo INT PRIMARY KEY AUTO_INCREMENT,
  placa VARCHAR(10),
  chassi VARCHAR(20),
  marca VARCHAR(50),
  modelo VARCHAR(50),
  cor VARCHAR(30),
  tipo_mecanizacao VARCHAR(20),
  grupo VARCHAR(50),
  status_veiculo VARCHAR(20)
);

-- Dimensão Pátio
CREATE TABLE IF NOT EXISTS dim_patio (
  id_patio INT PRIMARY KEY AUTO_INCREMENT,
  nome VARCHAR(100),
  endereco VARCHAR(255),
  capacidade_estimada INT
);

-- Dimensão Tempo
CREATE TABLE IF NOT EXISTS dim_tempo (
  id_tempo INT PRIMARY KEY AUTO_INCREMENT,
  data_completa DATE,
  ano INT,
  mes INT,
  dia INT,
  dia_semana VARCHAR(10),
  trimestre INT
);

-- Fato Locações
CREATE TABLE IF NOT EXISTS fato_locacoes (
  id_locacao INT PRIMARY KEY AUTO_INCREMENT,
  id_cliente INT,
  id_veiculo INT,
  id_patio_retirada INT,
  id_patio_devolucao INT,
  id_tempo INT,
  tempo_locacao INT,
  tempo_restante INT,
  valor_previsto DECIMAL(10,2),
  valor_final DECIMAL(10,2),
  status_locacao VARCHAR(20),
  FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente),
  FOREIGN KEY (id_veiculo) REFERENCES dim_veiculo(id_veiculo),
  FOREIGN KEY (id_patio_retirada) REFERENCES dim_patio(id_patio),
  FOREIGN KEY (id_patio_devolucao) REFERENCES dim_patio(id_patio),
  FOREIGN KEY (id_tempo) REFERENCES dim_tempo(id_tempo)
);

-- Fato Reservas
CREATE TABLE IF NOT EXISTS fato_reservas (
  id_reserva INT PRIMARY KEY AUTO_INCREMENT,
  id_cliente INT,
  id_grupo VARCHAR(50),
  id_patio INT,
  id_tempo INT,
  tempo_antecedencia INT,
  tempo_duracao_previsto INT,
  status_reserva VARCHAR(20),
  FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente),
  FOREIGN KEY (id_patio) REFERENCES dim_patio(id_patio),
  FOREIGN KEY (id_tempo) REFERENCES dim_tempo(id_tempo)
);

-- Fato Ocupação de Pátio
CREATE TABLE IF NOT EXISTS fato_ocupacao_patio (
  id_fato INT PRIMARY KEY AUTO_INCREMENT,
  id_tempo INT,
  id_patio INT,
  grupo VARCHAR(50),
  origem_empresa VARCHAR(50),
  qtd_veiculos INT,
  FOREIGN KEY (id_patio) REFERENCES dim_patio(id_patio),
  FOREIGN KEY (id_tempo) REFERENCES dim_tempo(id_tempo)
);
