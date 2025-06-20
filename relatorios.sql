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

USE dw;

-- =============================================================================
-- RELATÓRIO 1: Controle de Pátio
-- Descrição: Apresenta o quantitativo de veículos no pátio por "grupo" e "origem".
-- Utiliza a tabela 'fato_ocupacao_patio' para obter um snapshot diário da ocupação.
-- =============================================================================
SELECT
    dp.nome AS Nome_Patio,
    fo.grupo AS Grupo_Veiculo,
    fo.origem_empresa AS Origem_Empresa,
    SUM(fo.qtd_veiculos) AS Saldo_Veiculos_No_Patio
FROM fato_ocupacao_patio fo
JOIN dim_patio dp ON fo.id_patio = dp.id_patio
-- Para ver a foto de um dia específico (ex: o dia de hoje)
-- JOIN dim_tempo dt ON fo.id_tempo = dt.id_tempo
-- WHERE dt.data_completa <= CURDATE()
GROUP BY
    dp.nome,
    fo.grupo,
    fo.origem_empresa
ORDER BY
    Nome_Patio,
    Saldo_Veiculos_No_Patio DESC;


-- =============================================================================
-- RELATÓRIO 2: Controle das Locações
-- Descrição: Mostra o tempo médio de locação e o tempo restante para
-- devolução dos veículos alugados, agrupados por grupo de veículo.
-- =============================================================================
SELECT
    dv.grupo AS Grupo_Veiculo,
    AVG(fl.tempo_locacao) AS Media_Dias_Locacao,
    -- Calcula a média de tempo restante apenas para locações ativas
    AVG(CASE WHEN fl.status_locacao = 'Ativa' THEN fl.tempo_restante ELSE NULL END) AS Media_Dias_Restantes_Devolucao,
    COUNT(fl.id_locacao) AS Total_Locacoes
FROM fato_locacoes fl
JOIN dim_veiculo dv ON fl.id_veiculo = dv.id_veiculo
GROUP BY
    dv.grupo
ORDER BY
    Total_Locacoes DESC;


-- =============================================================================
-- RELATÓRIO 3: Controle de Reservas
-- Descrição: Analisa as reservas por grupo de veículo, pátio de retirada
-- e cidade de origem do cliente, incluindo o tempo de antecedência da reserva.
-- =============================================================================
SELECT
    fr.id_grupo AS Grupo_Veiculo_Desejado,
    dp.nome AS Patio_Retirada,
    dc.cidade AS Cidade_Origem_Cliente,
    COUNT(fr.id_reserva) AS Total_Reservas,
    AVG(fr.tempo_antecedencia) AS Media_Antecedencia_Dias,
    AVG(fr.tempo_duracao_previsto) AS Media_Duracao_Prevista_Dias
FROM fato_reservas fr
JOIN dim_cliente dc ON fr.id_cliente = dc.id_cliente
JOIN dim_patio dp ON fr.id_patio = dp.id_patio
GROUP BY
    fr.id_grupo,
    dp.nome,
    dc.cidade
ORDER BY
    Total_Reservas DESC;


-- =============================================================================
-- RELATÓRIO 4: Grupos de Veículos Mais Alugados por Origem do Cliente
-- Descrição: Identifica os grupos de veículos mais populares, cruzando
-- com a cidade e o estado de origem dos clientes.
-- =============================================================================
SELECT
    dv.grupo AS Grupo_Veiculo,
    dc.estado AS Estado_Cliente,
    dc.cidade AS Cidade_Cliente,
    COUNT(fl.id_locacao) AS Numero_De_Alugueis
FROM fato_locacoes fl
JOIN dim_veiculo dv ON fl.id_veiculo = dv.id_veiculo
JOIN dim_cliente dc ON fl.id_cliente = dc.id_cliente
GROUP BY
    dv.grupo,
    dc.estado,
    dc.cidade
ORDER BY
    Numero_De_Alugueis DESC
LIMIT 25; -- Mostra as 25 combinações mais frequentes


-- =============================================================================
-- ANÁLISE 5: Matriz de Movimentação para Cadeia de Markov
-- Descrição: Calcula a matriz estocástica com os percentuais de movimentação
-- da frota entre os pátios (de onde saiu para onde foi entregue).
-- =============================================================================
WITH MovimentacaoTotal AS (
    -- Conta o total de veículos que saíram de cada pátio de retirada
    SELECT
        id_patio_retirada,
        COUNT(*) AS Total_Saidas
    FROM fato_locacoes
    GROUP BY
        id_patio_retirada
),
MovimentacaoDestino AS (
    -- Conta as movimentações de um pátio de origem para um pátio de destino
    SELECT
        id_patio_retirada,
        id_patio_devolucao,
        COUNT(*) AS Total_Movimentos
    FROM fato_locacoes
    WHERE id_patio_devolucao IS NOT NULL -- Considera apenas locações já concluídas
    GROUP BY
        id_patio_retirada,
        id_patio_devolucao
)
-- Calcula o percentual final para montar a matriz
SELECT
    p_origem.nome AS Patio_Origem,
    p_destino.nome AS Patio_Destino,
    md.Total_Movimentos,
    mt.Total_Saidas,
    ROUND((md.Total_Movimentos / mt.Total_Saidas) * 100, 2) AS Percentual_Movimentacao
FROM MovimentacaoDestino md
JOIN MovimentacaoTotal mt ON md.id_patio_retirada = mt.id_patio_retirada
JOIN dim_patio p_origem ON md.id_patio_retirada = p_origem.id_patio
JOIN dim_patio p_destino ON md.id_patio_devolucao = p_destino.id_patio
ORDER BY
    p_origem.nome,
    p_destino.nome;
