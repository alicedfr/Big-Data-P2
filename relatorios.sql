/********************************************************************************
 * UFRJ/IM/DMA - Big Data & Data Warehouse
 * Avaliação 02, Parte II - Modelagem de Data Warehouse
 *
 * Grupo:
 * - Alice Duarte Faria Ribeiro (DRE 122058907)
 * - Beatriz Farias do Nascimento (DRE 122053127)
 * - Gustavo do Amaral Roxo Pereira (DRE 122081146)
 *
 * Contém as consultas SQL para gerar os relatórios gerenciais
 * e a matriz de movimentação de Markov a partir do Data Warehouse.
 ********************************************************************************/

-- =============== Relatório 1: Controle de Pátio ===============--
SELECT
    dp.Nome_Patio,
    dv.Grupo_Veiculo,
    fl.Origem_Veiculo,
    COUNT(DISTINCT fl.SK_Veiculo) AS Quantidade_Veiculos_Entregues
FROM dw.Fato_Locacoes fl
JOIN dw.Dim_Veiculo dv ON fl.SK_Veiculo = dv.SK_Veiculo
JOIN dw.Dim_Patio dp ON fl.SK_Patio_Entrega = dp.SK_Patio
GROUP BY
    dp.Nome_Patio,
    dv.Grupo_Veiculo,
    fl.Origem_Veiculo
ORDER BY
    dp.Nome_Patio,
    dv.Grupo_Veiculo;


-- =============== Relatório 2: Controle das Locações ===============--
SELECT
    dv.Grupo_Veiculo,
    AVG(fl.Duracao_Dias_Locacao) AS Media_Dias_Locacao,
    MIN(fl.Duracao_Dias_Locacao) AS Min_Dias_Locacao,
    MAX(fl.Duracao_Dias_Locacao) AS Max_Dias_Locacao,
    COUNT(fl.SK_Locacao) AS Total_Locacoes
FROM dw.Fato_Locacoes fl
JOIN dw.Dim_Veiculo dv ON fl.SK_Veiculo = dv.SK_Veiculo
GROUP BY
    dv.Grupo_Veiculo
ORDER BY
    Total_Locacoes DESC;


-- =============== Relatório 3: Grupos de Veículos mais Alugados ===============--
SELECT
    dv.Grupo_Veiculo,
    dc.Estado_Cliente,
    dc.Cidade_Cliente,
    COUNT(fl.SK_Locacao) AS Numero_De_Alugueis
FROM dw.Fato_Locacoes fl
JOIN dw.Dim_Veiculo dv ON fl.SK_Veiculo = dv.SK_Veiculo
JOIN dw.Dim_Cliente dc ON fl.SK_Cliente = dc.SK_Cliente
GROUP BY
    dv.Grupo_Veiculo,
    dc.Estado_Cliente,
    dc.Cidade_Cliente
ORDER BY
    Numero_De_Alugueis DESC
LIMIT 20;


-- =============== Análise 1: Matriz de Movimentação (Cadeia de Markov) ===============--
WITH MovimentacaoTotal AS (
    SELECT
        dp_retirada.Nome_Patio AS Patio_Origem,
        COUNT(*) AS Total_Saidas
    FROM dw.Fato_Locacoes fl
    JOIN dw.Dim_Patio dp_retirada ON fl.SK_Patio_Retirada = dp_retirada.SK_Patio
    GROUP BY
        dp_retirada.Nome_Patio
),
MovimentacaoDestino AS (
    SELECT
        dp_retirada.Nome_Patio AS Patio_Origem,
        dp_entrega.Nome_Patio AS Patio_Destino,
        COUNT(*) AS Total_Movimentos
    FROM dw.Fato_Locacoes fl
    JOIN dw.Dim_Patio dp_retirada ON fl.SK_Patio_Retirada = dp_retirada.SK_Patio
    JOIN dw.Dim_Patio dp_entrega ON fl.SK_Patio_Entrega = dp_entrega.SK_Patio
    GROUP BY
        dp_retirada.Nome_Patio,
        dp_entrega.Nome_Patio
)
SELECT
    md.Patio_Origem,
    md.Patio_Destino,
    md.Total_Movimentos,
    mt.Total_Saidas,
    ROUND((md.Total_Movimentos / mt.Total_Saidas) * 100, 2) AS Percentual_Movimentacao
FROM MovimentacaoDestino md
JOIN MovimentacaoTotal mt ON md.Patio_Origem = mt.Patio_Origem
ORDER BY
    md.Patio_Origem,
    md.Patio_Destino;
