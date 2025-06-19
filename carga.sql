/********************************************************************************
 * UFRJ/IM/DMA - Big Data & Data Warehouse
 * Avaliação 02, Parte II - Modelagem de Data Warehouse
 *
 * Grupo:
 * - Alice Duarte Faria Ribeiro (DRE 122058907)
 * - Beatriz Farias do Nascimento (DRE 122053127)
 * - Gustavo do Amaral Roxo Pereira (DRE 122081146)
 *
 * Fase de CARGA (Load).
 * Carrega a tabela de fatos (Fato_Locacoes) a partir da Staging Area
 * e das Dimensões já populadas no DW.
 ********************************************************************************/


SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE dw.Fato_Locacoes;
SET FOREIGN_KEY_CHECKS = 1;

-- Passo Final: Inserção na Fato_Locacoes
INSERT INTO dw.Fato_Locacoes (
    SK_Cliente, SK_Veiculo, SK_Patio_Retirada, SK_Patio_Entrega,
    SK_Data_Retirada, SK_Data_Entrega, Valor_Total_Pago, Duracao_Dias_Locacao, Origem_Veiculo
)
SELECT
    dc.SK_Cliente,
    dv.SK_Veiculo,
    dp_ret.SK_Patio AS SK_Patio_Retirada,
    dp_ent.SK_Patio AS SK_Patio_Entrega,
    -- Converte a data para o formato YYYYMMDD para a chave da Dim_Tempo
    CAST(DATE_FORMAT(sl.data_retirada, '%Y%m%d') AS SIGNED) AS SK_Data_Retirada,
    CAST(DATE_FORMAT(sl.data_devolucao, '%Y%m%d') AS SIGNED) AS SK_Data_Entrega,
    sl.valor_total_pago,
    -- Calcula a duração da locação em dias
    DATEDIFF(sl.data_devolucao, sl.data_retirada) AS Duracao_Dias_Locacao,
    -- Determina se o veículo é da empresa gestora do pátio ou de uma associada
    CASE
        WHEN dv.Empresa_Proprietaria = dp_ret.Empresa_Gestora THEN 'Próprio'
        ELSE 'Associado'
    END AS Origem_Veiculo
FROM staging.locacoes sl
-- JOINs para fazer o lookup das chaves substitutas (SK)
JOIN staging.clientes sc ON sl.id_cliente_origem = sc.id_cliente_origem AND sl.fonte_dados = sc.fonte_dados
JOIN dw.Dim_Cliente dc ON dc.NK_Documento = TRIM(COALESCE(sc.cpf, sc.cnpj, sc.cpf_cnpj_unificado))

JOIN staging.veiculos sv ON sl.id_veiculo_origem = sv.id_veiculo_origem AND sl.fonte_dados = sv.fonte_dados
JOIN dw.Dim_Veiculo dv ON dv.NK_Placa = TRIM(UPPER(sv.placa))

JOIN staging.patios sp_ret ON sl.id_patio_retirada_origem = sp_ret.id_patio_origem AND sl.fonte_dados = sp_ret.fonte_dados
JOIN dw.Dim_Patio dp_ret ON dp_ret.Nome_Patio = sp_ret.nome_patio

JOIN staging.patios sp_ent ON sl.id_patio_devolucao_origem = sp_ent.id_patio_origem AND sl.fonte_dados = sp_ent.fonte_dados
JOIN dw.Dim_Patio dp_ent ON dp_ent.Nome_Patio = sp_ent.nome_patio;