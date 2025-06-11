-- SQL Script para Geração dos Relatórios Gerenciais e da Matriz de Percentuais de Movimentação entre Pátios

USE locadora_dw;

-- --- RELATÓRIOS GERENCIAIS GERAIS ---

-- 1. Relatório: Controle de Pátio
-- Quantitativo de veículos no pátio por "grupo" e "origem".
-- Pode haver agrupamento por marca do veículo, modelos e tipo de mecanização.
-- Por "origem" entenda-se da frota da empresa dona do pátio, ou da frota das outras cinco empresas associadas.

SELECT
    dp.nome_patio AS Patio_Atual,
    dv.nome_grupo_veiculo AS Grupo_Veiculo,
    dv.marca AS Marca_Veiculo,
    dv.modelo AS Modelo_Veiculo,
    dv.tipo_mecanizacao AS Tipo_Mecanizacao,
    de_frota.nome_empresa AS Empresa_Proprietaria_Frota,
    COUNT(DISTINCT fl.sk_veiculo) AS Quantidade_Veiculos_No_Patio
FROM Fato_Locacao fl
JOIN Dim_Patio dp ON fl.sk_patio_devolucao_real = dp.sk_patio -- Considera o pátio de devolução como o "atual" para veículos devolvidos
JOIN Dim_Veiculo dv ON fl.sk_veiculo = dv.sk_veiculo
-- Para determinar a "origem" da frota, precisamos saber a qual empresa o veículo pertence.
-- A Dim_Empresa mapeia empresas originais. Assumimos que o id_empresa_proprietaria
-- do sistema OLTP do veículo (se disponível) mapeia para a Dim_Empresa.
LEFT JOIN Dim_Empresa de_frota ON dv.id_veiculo_origem IN (
    -- Subconsulta para encontrar o id_empresa_proprietaria_origem do veículo no staging
    -- Isso dependeria de como 'id_empresa_proprietaria' foi mapeado na stg_veiculo na extração.
    -- Para robustez, idealmente Dim_Veiculo teria uma FK direta para Dim_Empresa Proprietária.
    -- Aqui, vamos inferir da Empresa do Pátio de retirada como um proxy, ou buscar no OLTP se não estiver no DW.
    SELECT de.sk_empresa
    FROM Dim_Empresa de
    WHERE de.id_empresa_origem = (
        SELECT id_empresa_origem FROM locadora_dw_staging.stg_patio
        WHERE id_patio_origem = fl.id_locacao_origem AND sistema_origem = fl.sistema_origem LIMIT 1
    )
    LIMIT 1
) -- Simplificação: se a Dim_Veiculo não tem FK para Empresa Proprietária
WHERE fl.sk_tempo_devolucao_real IS NOT NULL -- Apenas veículos que foram devolvidos
  AND fl.sk_status_locacao = (SELECT sk_status_locacao FROM Dim_Status_Locacao WHERE nome_status_locacao = 'Concluida')
  AND dv.flag_ativo = TRUE -- Considera apenas a versão ativa do veículo na dimensão
GROUP BY
    dp.nome_patio,
    dv.nome_grupo_veiculo,
    dv.marca,
    dv.modelo,
    dv.tipo_mecanizacao,
    de_frota.nome_empresa
ORDER BY
    dp.nome_patio, Quantidade_Veiculos_No_Patio DESC;


-- 2. Relatório: Controle das Locações
-- Quantitativo de veículos alugados por "grupo", dimensão de tempo de locação e tempo restante para devolução.

SELECT
    dv.nome_grupo_veiculo AS Grupo_Veiculo,
    dt_retirada.ano AS Ano_Retirada,
    dt_retirada.nome_mes AS Mes_Retirada,
    dt_retirada.nome_dia_da_semana AS Dia_Semana_Retirada,
    fl.duracao_locacao_horas_real AS Duracao_Locacao_Horas_Real,
    fl.duracao_locacao_horas_prevista AS Duracao_Locacao_Horas_Prevista,
    -- Cálculo do tempo restante para devolução (apenas para locações ativas)
    CASE
        WHEN dsl.nome_status_locacao = 'Ativa' THEN
            TIMESTAMPDIFF(HOUR, NOW(), (SELECT data_completa FROM Dim_Tempo WHERE sk_tempo = fl.sk_tempo_devolucao_prevista) )
        ELSE 0
    END AS Tempo_Restante_Devolucao_Horas,
    COUNT(fl.sk_locacao) AS Quantidade_Locacoes
FROM Fato_Locacao fl
JOIN Dim_Veiculo dv ON fl.sk_veiculo = dv.sk_veiculo AND dv.flag_ativo = TRUE -- Veículo ativo
JOIN Dim_Tempo dt_retirada ON fl.sk_tempo_retirada = dt_retirada.sk_tempo
JOIN Dim_Tempo dt_dev_prev ON fl.sk_tempo_devolucao_prevista = dt_dev_prev.sk_tempo
JOIN Dim_Status_Locacao dsl ON fl.sk_status_locacao = dsl.sk_status_locacao
WHERE dsl.nome_status_locacao IN ('Ativa', 'Concluida') -- Considerar locações ativas e concluídas para análise de duração
GROUP BY
    dv.nome_grupo_veiculo,
    dt_retirada.ano,
    dt_retirada.nome_mes,
    dt_retirada.nome_dia_da_semana,
    fl.duracao_locacao_horas_real,
    fl.duracao_locacao_horas_prevista,
    Tempo_Restante_Devolucao_Horas
ORDER BY
    Quantidade_Locacoes DESC;


-- 3. Relatório: Controle de Reservas
-- Quantas reservas por "grupo" de veículo, "pátio", tempo de retirada futura e cidades de origem dos clientes.

SELECT
    dv.nome_grupo_veiculo AS Grupo_Veiculo_Reservado,
    dp.nome_patio AS Patio_Retirada_Previsto,
    CASE
        WHEN fr.dias_antecedencia_reserva <= 7 THEN 'Próxima Semana (0-7 dias)'
        WHEN fr.dias_antecedencia_reserva > 7 AND fr.dias_antecedencia_reserva <= 30 THEN 'Próximo Mês (8-30 dias)'
        ELSE 'Futuro Distante (> 30 dias)'
    END AS Faixa_Tempo_Retirada_Futura,
    dc.cidade_cliente AS Cidade_Origem_Cliente,
    COUNT(fr.sk_reserva) AS Quantidade_Reservas
FROM Fato_Reserva fr
JOIN Dim_Veiculo dv ON fr.sk_grupo_veiculo = dv.sk_veiculo AND dv.flag_ativo = TRUE -- Grupo de veículo (Dim_Veiculo representa também os grupos)
JOIN Dim_Patio dp ON fr.sk_patio_retirada = dp.sk_patio
JOIN Dim_Cliente dc ON fr.sk_cliente = dc.sk_cliente AND dc.flag_ativo = TRUE -- Cliente ativo
GROUP BY
    dv.nome_grupo_veiculo,
    dp.nome_patio,
    Faixa_Tempo_Retirada_Futura,
    dc.cidade_cliente
ORDER BY
    Quantidade_Reservas DESC;


-- 4. Relatório: Grupos de Veículos Mais Alugados
-- Cruzando, eventualmente, com a origem dos clientes.

SELECT
    dv.nome_grupo_veiculo AS Grupo_Veiculo,
    dc.cidade_cliente AS Cidade_Origem_Cliente,
    COUNT(fl.sk_locacao) AS Quantidade_Locacoes
FROM Fato_Locacao fl
JOIN Dim_Veiculo dv ON fl.sk_veiculo = dv.sk_veiculo AND dv.flag_ativo = TRUE
JOIN Dim_Cliente dc ON fl.sk_cliente = dc.sk_cliente AND dc.flag_ativo = TRUE
WHERE fl.sk_status_locacao = (SELECT sk_status_locacao FROM Dim_Status_Locacao WHERE nome_status_locacao = 'Concluida')
GROUP BY
    dv.nome_grupo_veiculo,
    dc.cidade_cliente
ORDER BY
    Quantidade_Locacoes DESC, dv.nome_grupo_veiculo;


-- --- ANÁLISE AVANÇADA: MATRIZ ESTOCÁSTICA PARA CADEIA DE MARKOV ---
-- Previsão de ocupação de pátio: matriz estocástica com os percentuais de movimentação da frota entre os pátios.
-- Para cada pátio, levantar o percentual de veículo que retorna ao mesmo pátio de onde foi retirado
-- e o percentual que é entregue em cada um dos outros pátios.

-- Passo 1: Calcular as transições absolutas entre pátios (Origem -> Destino)
CREATE TEMPORARY TABLE IF NOT EXISTS temp_transicoes_patio AS
SELECT
    fmp.sk_patio_origem,
    fmp.sk_patio_destino,
    COUNT(fmp.sk_movimentacao) AS num_transicoes
FROM Fato_Movimentacao_Patio fmp
WHERE fmp.sk_patio_origem IS NOT NULL -- Apenas transições com pátio de origem definido
  AND fmp.sk_patio_destino IS NOT NULL -- Apenas transições com pátio de destino definido
  AND fmp.sk_tipo_movimentacao IN (
      (SELECT sk_tipo_movimentacao FROM Dim_Tipo_Movimentacao_Patio WHERE nome_tipo_movimentacao = 'Saída'),
      (SELECT sk_tipo_movimentacao FROM Dim_Tipo_Movimentacao_Patio WHERE nome_tipo_movimentacao = 'Entrada'), -- Considera devoluções como entrada no pátio de destino
      (SELECT sk_tipo_movimentacao FROM Dim_Tipo_Movimentacao_Patio WHERE nome_tipo_movimentacao = 'Transferência')
  )
GROUP BY
    fmp.sk_patio_origem,
    fmp.sk_patio_destino;

-- Passo 2: Calcular o total de saídas (origens) para cada pátio para normalização
CREATE TEMPORARY TABLE IF NOT EXISTS temp_total_saidas_patio AS
SELECT
    sk_patio_origem,
    SUM(num_transicoes) AS total_saidas
FROM temp_transicoes_patio
GROUP BY
    sk_patio_origem;

-- Passo 3: Gerar a Matriz Estocástica com Percentuais
-- Representa a probabilidade de um veículo ir de um pátio para outro.
SELECT
    dp_origem.nome_patio AS Patio_Origem,
    COALESCE(dp_destino.nome_patio, 'N/A') AS Patio_Destino, -- COALESCE para casos onde destino pode ser NULL (saídas do sistema)
    tt.num_transicoes AS Numero_Transicoes,
    ts.total_saidas AS Total_Saidas_Do_Patio_Origem,
    ROUND((tt.num_transicoes * 100.0 / ts.total_saidas), 2) AS Percentual_Movimentacao
FROM temp_transicoes_patio tt
JOIN temp_total_saidas_patio ts ON tt.sk_patio_origem = ts.sk_patio_origem
JOIN Dim_Patio dp_origem ON tt.sk_patio_origem = dp_origem.sk_patio
LEFT JOIN Dim_Patio dp_destino ON tt.sk_patio_destino = dp_destino.sk_patio -- LEFT JOIN para incluir destinos que podem ser N/A
ORDER BY
    Patio_Origem, Percentual_Movimentacao DESC;


-- Limpeza de tabelas temporárias (Recomendado após a execução)
DROP TEMPORARY TABLE IF EXISTS temp_transicoes_patio;
DROP TEMPORARY TABLE IF EXISTS temp_total_saidas_patio;
