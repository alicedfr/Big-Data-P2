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


-- Limpa as tabelas do DW antes da carga para garantir a idempotência.
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE dw.Dim_Cliente;
TRUNCATE TABLE dw.Dim_Veiculo;
TRUNCATE TABLE dw.Dim_Patio;
SET FOREIGN_KEY_CHECKS = 1;

-- Passo 1: Carga da Dim_Patio
INSERT INTO dw.Dim_Patio (Nome_Patio, Cidade_Patio, Empresa_Gestora) VALUES
('Aeroporto do Galeão', 'Rio de Janeiro', 'Empresa 1'),
('Santos Dumont', 'Rio de Janeiro', 'Empresa 2'),
('Rodoviária', 'Rio de Janeiro', 'Empresa 3'),
('Shopping Rio Sul', 'Rio de Janeiro', 'Empresa 4'),
('Nova América', 'Rio de Janeiro', 'Empresa 5'),
('Barra Shopping', 'Rio de Janeiro', 'Empresa 6');


-- Passo 2: Carga da Dim_Cliente
INSERT INTO dw.Dim_Cliente (NK_Documento, Nome_Cliente, Email, Cidade_Cliente, Estado_Cliente, Data_Inicio_DW)
SELECT DISTINCT
    TRIM(COALESCE(cpf, cnpj, cpf_cnpj_unificado)) AS NK_Documento,
    TRIM(nome_razao_social) AS Nome_Cliente,
    LOWER(TRIM(email)) as Email,
    TRIM(cidade) AS Cidade_Cliente,
    TRIM(estado) AS Estado_Cliente,
    CURDATE() AS Data_Inicio_DW
FROM staging.clientes;


-- Passo 3: Carga da Dim_Veiculo
INSERT INTO dw.Dim_Veiculo (NK_Placa, Marca, Modelo, Ano_Fabricacao, Grupo_Veiculo, Tipo_Mecanizacao, Empresa_Proprietaria)
SELECT DISTINCT
    TRIM(UPPER(placa)) AS NK_Placa,
    TRIM(marca) as Marca,
    TRIM(modelo) as Modelo,
    ano_fabricacao,
    TRIM(grupo_veiculo) as Grupo_Veiculo,
    CASE
        WHEN mecanizacao_bool = 1 THEN 'AUTOMÁTICO'
        WHEN mecanizacao_bool = 0 THEN 'MANUAL'
        WHEN UPPER(mecanizacao_texto) LIKE '%AUTO%' THEN 'AUTOMÁTICO'
        ELSE 'MANUAL'
    END AS Tipo_Mecanizacao,
    TRIM(empresa_proprietaria) as Empresa_Proprietaria
FROM staging.veiculos;