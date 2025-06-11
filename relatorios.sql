-- Script SQL para Geração de Relatórios e Matriz de Percentuais de Movimentação de Pátio

USE locadora_dw;

-- --- RELATÓRIOS GERENCIAIS ---

-- 1. Relatório: Controle de Pátio - Quantitativo de Veículos por Grupo e Origem
-- Este relatório mostra a quantidade de veículos atualmente em cada pátio, agrupados por
-- grupo de veículo e indicando se o veículo é da frota da empresa proprietária do pátio
-- ou de uma empresa associada.
SELECT
    dp.nome_patio AS Patio,
    dv.nome_grupo_veiculo AS Grupo_Veiculo,
    dv.marca AS Marca_Veiculo,
    dv.modelo AS Modelo_Veiculo,
    dv.tipo_mecanizacao AS Tipo_Mecanizacao,
    CASE
        WHEN dv.sistema_origem = dp.sistema_origem AND de_prop.nome_empresa = dp.nome_empresa_proprietaria THEN 'Frota da Empresa do Pátio'
        ELSE 'Frota de Empresa Associada'
    END AS Origem_Frota,
    COUNT(fmp.sk_veiculo) AS Quantidade_Veiculos_No_Patio
FROM Fato_Movimentacao_Patio fmp
JOIN Dim_Patio dp ON fmp.sk_patio_destino = dp.sk_patio -- Pátio onde o veículo está atualmente
JOIN Dim_Veiculo dv ON fmp.sk_veiculo = dv.sk_veiculo
JOIN Dim_Empresa de_prop ON fmp.sk_empresa_proprietaria_frota = de_prop.sk_empresa
WHERE fmp.sk_tipo_movimentacao = (SELECT sk_tipo_movimentacao FROM Dim_Tipo_Movimentacao_Patio WHERE nome_tipo_movimentacao = 'Entrada') -- Considerando veículos que entraram e ainda não saíram, ou a última movimentação é uma entrada
    AND NOT EXISTS (
        SELECT 1 FROM Fato_Movimentacao_Patio fmp_saida
        WHERE fmp_saida.sk_veiculo = fmp.sk_veiculo
        AND fmp_saida.sk_tempo_movimentacao > fmp.sk_tempo_movimentacao
        AND fmp_saida.sk_tipo_movimentacao = (SELECT sk_tipo_movimentacao FROM Dim_Tipo_Movimentacao_Patio WHERE nome_tipo_movimentacao = 'Saída')
    ) -- Esta subquery é simplificada, em um cenário real seria uma lógica mais complexa para "último estado"
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 6;


-- 2. Relatório: Controle de Locações - Quantitativo de Veículos Alugados por Grupo e Tempo
-- Este relatório mostra a quantidade de locações por grupo de veículo, com detalhes de duração
-- prevista e real da locação, e o tempo restante para devolução (para locações ativas).
SELECT
    dv.nome_grupo_veiculo AS Grupo_Veiculo,
    fl.id_locacao_origem AS ID_Locacao,
    dst_ret.data_completa AS Data_Retirada,
    dst_dev_prev.data_completa AS Data_Devolucao_Prevista,
    dst_dev_real.data_completa AS Data_Devolucao_Real,
    fl.duracao_locacao_horas_prevista AS Duracao_Prevista_Horas,
    fl.duracao_locacao_horas_real AS Duracao_Real_Horas,
    CASE
        WHEN dsl.nome_status_locacao = 'Ativa' THEN
            TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP, dst_dev_prev.data_completa + INTERVAL (dst_dev_prev.hora * 3600 + dst_dev_prev.minuto * 60 + dst_dev_prev.segundo) SECOND)
        ELSE 0
    END AS Tempo_Restante_Devolucao_Horas, -- Tempo restante para devolução de locações ativas
    dsl.nome_status_locacao AS Status_Locacao
FROM Fato_Locacao fl
JOIN Dim_Veiculo dv ON fl.sk_veiculo = dv.sk_veiculo
JOIN Dim_Tempo dst_ret ON fl.sk_tempo_retirada = dst_ret.sk_tempo
JOIN Dim_Tempo dst_dev_prev ON fl.sk_tempo_devolucao_prevista = dst_dev_prev.sk_tempo
LEFT JOIN Dim_Tempo dst_dev_real ON fl.sk_tempo_devolucao_real = dst_dev_real.sk_tempo
JOIN Dim_Status_Locacao dsl ON fl.sk_status_locacao = dsl.sk_status_locacao
ORDER BY Grupo_Veiculo, Data_Retirada;


-- 3. Relatório: Controle de Reservas - Quantidade de Reservas por Grupo de Veículo, Pátio, Tempo de Retirada Futura e Cidade do Cliente
SELECT
    dv.nome_grupo_veiculo AS Grupo_Veiculo,
    dp.nome_patio AS Patio_Retirada,
    dc.cidade_cliente AS Cidade_Cliente,
    dsr.nome_status_reserva AS Status_Reserva,
    CASE
        WHEN dt_ret_prev.data_completa BETWEEN CURDATE() AND CURDATE() + INTERVAL 7 DAY THEN 'Próxima Semana'
        WHEN dt_ret_prev.data_completa BETWEEN CURDATE() + INTERVAL 8 DAY AND CURDATE() + INTERVAL 30 DAY THEN 'Próximo Mês'
        ELSE 'Futuro Distante'
    END AS Tempo_Retirada_Futura,
    fr.duracao_reserva_horas_prevista AS Duracao_Reserva_Prevista_Horas,
    COUNT(fr.sk_reserva) AS Quantidade_Reservas
FROM Fato_Reserva fr
JOIN Dim_Veiculo dv ON fr.sk_grupo_veiculo = dv.sk_veiculo
JOIN Dim_Patio dp ON fr.sk_patio_retirada = dp.sk_patio
JOIN Dim_Cliente dc ON fr.sk_cliente = dc.sk_cliente AND dc.flag_ativo = TRUE
JOIN Dim_Status_Reserva dsr ON fr.sk_status_reserva = dsr.sk_status_reserva
JOIN Dim_Tempo dt_ret_prev ON fr.sk_tempo_retirada_prevista = dt_ret_prev.sk_tempo
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY Quantidade_Reservas DESC, Tempo_Retirada_Futura;

-- 4. Relatório: Grupos de Veículos Mais Alugados por Origem do Cliente
SELECT
    dv.nome_grupo_veiculo AS Grupo_Veiculo,
    dc.pais_cliente AS Pais_Origem_Cliente,
    dc.cidade_cliente AS Cidade_Origem_Cliente,
    COUNT(fl.sk_locacao) AS Total_Locacoes
FROM Fato_Locacao fl
JOIN Dim_Veiculo dv ON fl.sk_veiculo = dv.sk_veiculo
JOIN Dim_Cliente dc ON fl.sk_cliente = dc.sk_cliente AND dc.flag_ativo = TRUE
GROUP BY 1, 2, 3
ORDER BY Total_Locacoes DESC, Grupo_Veiculo, Pais_Origem_Cliente, Cidade_Origem_Cliente;


-- --- MATRIZ DE PERCENTUAIS DE MOVIMENTAÇÃO ENTRE PÁTIOS (CADEIA DE MARKOV) ---
-- Esta consulta calcula a matriz estocástica de movimentação entre pátios.
-- Para cada pátio de origem, ela mostra o percentual de veículos que foram
-- entregues em cada pátio de destino (incluindo o próprio pátio de origem, se houver devolução nele).

WITH Movimentacoes_Totais_Origem AS (
    SELECT
        dp_origem.nome_patio AS Patio_Origem,
        COUNT(fmp.sk_movimentacao) AS Total_Movimentacoes_Saindo_Do_Patio
    FROM Fato_Movimentacao_Patio fmp
    JOIN Dim_Patio dp_origem ON fmp.sk_patio_origem = dp_origem.sk_patio
    WHERE fmp.sk_patio_origem IS NOT NULL -- Apenas movimentações que saíram de um pátio conhecido
    GROUP BY 1
),
Movimentacoes_Por_Destino AS (
    SELECT
        dp_origem.nome_patio AS Patio_Origem,
        dp_destino.nome_patio AS Patio_Destino,
        COUNT(fmp.sk_movimentacao) AS Quantidade_Movimentacoes
    FROM Fato_Movimentacao_Patio fmp
    JOIN Dim_Patio dp_origem ON fmp.sk_patio_origem = dp_origem.sk_patio
    JOIN Dim_Patio dp_destino ON fmp.sk_patio_destino = dp_destino.sk_patio
    WHERE fmp.sk_patio_origem IS NOT NULL AND fmp.sk_patio_destino IS NOT NULL -- Apenas transições entre pátios conhecidos
    GROUP BY 1, 2
)
SELECT
    m.Patio_Origem,
    m.Patio_Destino,
    m.Quantidade_Movimentacoes,
    t.Total_Movimentacoes_Saindo_Do_Patio,
    (m.Quantidade_Movimentacoes / t.Total_Movimentacoes_Saindo_Do_Patio) AS Percentual_Movimentacao -- Probabilidade
FROM Movimentacoes_Por_Destino m
JOIN Movimentacoes_Totais_Origem t ON m.Patio_Origem = t.Patio_Origem
ORDER BY m.Patio_Origem, m.Patio_Destino;
