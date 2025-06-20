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
CREATE SCHEMA IF NOT EXISTS staging;
USE staging;

-- Tabela de Staging para Clientes
CREATE TABLE IF NOT EXISTS stg_clientes (
  id_cliente_origem VARCHAR(50),
  nome VARCHAR(100),
  tipo_cliente VARCHAR(2),
  cpf_cnpj VARCHAR(20),
  cidade VARCHAR(100),
  estado VARCHAR(50),
  email VARCHAR(100),
  telefone VARCHAR(20)
);

-- Tabela de Staging para Veículos
CREATE TABLE IF NOT EXISTS stg_veiculos (
  id_veiculo_origem VARCHAR(50),
  placa VARCHAR(10),
  chassi VARCHAR(20),
  marca VARCHAR(50),
  modelo VARCHAR(50),
  cor VARCHAR(30),
  tipo_mecanizacao VARCHAR(20),
  grupo VARCHAR(50),
  status_veiculo VARCHAR(20)
);

-- Tabela de Staging para Pátios
CREATE TABLE IF NOT EXISTS stg_patios (
  id_patio_origem VARCHAR(50),
  nome VARCHAR(100),
  endereco VARCHAR(255),
  capacidade_estimada INT
);

-- Tabela de Staging para Locações
CREATE TABLE IF NOT EXISTS stg_locacoes (
  id_locacao VARCHAR(50),
  id_cliente VARCHAR(50),
  id_veiculo VARCHAR(50),
  data_retirada_real DATETIME,
  data_devolucao_prevista DATETIME,
  data_devolucao_real DATETIME,
  valor_previsto DECIMAL(10,2),
  valor_final DECIMAL(10,2),
  status_locacao VARCHAR(20),
  patio_retirada VARCHAR(100),
  patio_devolucao VARCHAR(100),
  cpf_cnpj VARCHAR(20)
);


-- Tabela de Staging para Reservas
CREATE TABLE IF NOT EXISTS stg_reservas (
  id_reserva VARCHAR(50),
  id_cliente VARCHAR(50),
  id_grupo VARCHAR(50),
  data_reserva DATETIME,
  data_retirada_prevista DATETIME,
  data_devolucao_prevista DATETIME,
  patio_retirada VARCHAR(100),
  status_reserva VARCHAR(20)
);
