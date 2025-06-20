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
-- Seleciona o banco de dados do Data Warehouse
USE dw;

-- Em um ambiente de produção, toda a carga de fatos deve ser atômica.
-- Usamos uma transação para garantir que ou tudo é carregado com sucesso, ou nada é alterado.
START TRANSACTION;

-- Carga da fato_locacoes
-- Pré-requisito: As dimensões já devem ter sido carregadas na fase de Transformação.
-- Em um ambiente real, a lógica de SCD (Slowly Changing Dimensions) seria aplicada
-- na fase de Transformação para atualizar as dimensões antes desta carga.
INSERT INTO fato_locacoes (id_cliente, id_veiculo, id_patio_retirada, id_patio_devolucao, id_tempo, tempo_locacao, tempo_restante, valor_previsto, valor_final, status_locacao)
SELECT
    dc.id_cliente,
    dv.id_veiculo,
    dp_ret.id_patio,
    dp_dev.id_patio,
    dt.id_tempo,
    -- Calcula tempo_locacao em dias
    DATEDIFF(sl.data_devolucao_real, sl.data_retirada_real) AS tempo_locacao,
    -- Calcula tempo_restante em dias (se ativa, senão 0)
    IF(sl.status_locacao = 'Ativa', DATEDIFF(sl.data_devolucao_prevista, CURDATE()), 0) AS tempo_restante,
    sl.valor_previsto,
    sl.valor_final,
    sl.status_locacao
FROM staging.stg_locacoes sl
-- JOINs para buscar as chaves das dimensões (lookups)
JOIN dw.dim_cliente dc ON sl.cpf_cnpj = dc.cpf_cnpj
JOIN staging.stg_veiculos sv ON sl.id_veiculo = sv.id_veiculo_origem
JOIN dw.dim_veiculo dv ON sv.placa = dv.placa
JOIN dw.dim_patio dp_ret ON sl.patio_retirada = dp_ret.nome
LEFT JOIN dw.dim_patio dp_dev ON sl.patio_devolucao = dp_dev.nome -- LEFT JOIN para casos em que a devolução ainda não ocorreu
JOIN dw.dim_tempo dt ON DATE(sl.data_retirada_real) = dt.data_completa;


-- Carga da fato_reservas
INSERT INTO fato_reservas (id_cliente, id_grupo, id_patio, id_tempo, tempo_antecedencia, tempo_duracao_previsto, status_reserva)
SELECT
    dc.id_cliente,
    sr.id_grupo,
    dp.id_patio,
    dt.id_tempo,
    -- Calcula tempo_antecedencia da reserva em dias
    DATEDIFF(sr.data_retirada_prevista, sr.data_reserva) AS tempo_antecedencia,
    -- Calcula tempo_duracao_previsto da locação em dias
    DATEDIFF(sr.data_devolucao_prevista, sr.data_retirada_prevista) AS tempo_duracao_previsto,
    sr.status_reserva
FROM staging.stg_reservas sr
-- JOINs para buscar as chaves das dimensões
JOIN staging.stg_clientes sc ON sr.id_cliente = sc.id_cliente_origem
JOIN dw.dim_cliente dc ON sc.cpf_cnpj = dc.cpf_cnpj
JOIN dw.dim_patio dp ON sr.patio_retirada = dp.nome
JOIN dw.dim_tempo dt ON DATE(sr.data_reserva) = dt.data_completa;


-- Carga da fato_ocupacao_patio
-- ATENÇÃO: Em um ambiente real, criar um fato de snapshot (foto diária da ocupação) é complexo.
-- A abordagem a seguir é mais robusta e cria uma tabela de fatos de TRANSAÇÃO.
-- Ela registra cada entrada (+1) e saída (-1) de veículo nos pátios.
-- Uma ferramenta de BI pode então agregar esses valores para calcular a ocupação em qualquer data.
INSERT INTO fato_ocupacao_patio (id_tempo, id_patio, grupo, origem_empresa, qtd_veiculos)
-- Parte 1: Saídas de veículos (Retiradas, qtd = -1)
SELECT
    dt.id_tempo,
    dp.id_patio,
    dv.grupo,
    'N/A' AS origem_empresa, -- A origem da empresa não é relevante para a saída
    -1 AS qtd_veiculos -- Um veículo saiu, decrementa a ocupação
FROM staging.stg_locacoes sl
JOIN dw.dim_patio dp ON sl.patio_retirada = dp.nome
JOIN dw.dim_tempo dt ON DATE(sl.data_retirada_real) = dt.data_completa
JOIN staging.stg_veiculos sv ON sl.id_veiculo = sv.id_veiculo_origem
JOIN dw.dim_veiculo dv ON sv.placa = dv.placa

UNION ALL

-- Parte 2: Entradas de veículos (Devoluções, qtd = +1)
SELECT
    dt.id_tempo,
    dp.id_patio,
    dv.grupo,
    -- A origem da empresa (própria ou associada) seria determinada aqui
    -- com base em uma lógica mais complexa ou um campo que não existe na staging.
    'Empresa Associada' AS origem_empresa, -- Placeholder
    1 AS qtd_veiculos -- Um veículo entrou, incrementa a ocupação
FROM staging.stg_locacoes sl
WHERE sl.data_devolucao_real IS NOT NULL -- Apenas se a devolução realmente ocorreu
JOIN dw.dim_patio dp ON sl.patio_devolucao = dp.nome
JOIN dw.dim_tempo dt ON DATE(sl.data_devolucao_real) = dt.data_completa
JOIN staging.stg_veiculos sv ON sl.id_veiculo = sv.id_veiculo_origem
JOIN dw.dim_veiculo dv ON sv.placa = dv.placa;

-- Se todas as cargas ocorreram sem erro, a transação é confirmada.
COMMIT;