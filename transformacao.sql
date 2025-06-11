-- SQL Script para Transformação ETL
-- Este script realiza o tratamento e conformação dos dados da área de staging,
-- oriundos de diversos sistemas OLTP (incluindo o próprio e os outros grupos),
-- antes de serem carregados nas tabelas de fatos e dimensões do DW.

USE locadora_dw_staging; -- Trabalhando na área de staging para transformações intermediárias

-- NOTA IMPORTANTE: As chaves substitutas (sk_*) serão resolvidas na fase de CARGA (Load),
-- pois dependem da existência e população das tabelas de dimensão no DW final.
-- Aqui, nas tabelas temporárias de transformação, manteremos as chaves de origem
-- (id_origem, sistema_origem) e os atributos denormalizados.

-- Limpeza de tabelas temporárias existentes para garantir um reprocessamento limpo
DROP TEMPORARY TABLE IF EXISTS temp_dim_empresa;
DROP TEMPORARY TABLE IF EXISTS temp_dim_patio;
DROP TEMPORARY TABLE IF EXISTS temp_dim_cliente;
DROP TEMPORARY TABLE IF EXISTS temp_dim_veiculo;
DROP TEMPORARY TABLE IF EXISTS temp_dim_condutor;
DROP TEMPORARY TABLE IF EXISTS temp_dim_seguro;
DROP TEMPORARY TABLE IF EXISTS temp_fato_locacao;
DROP TEMPORARY TABLE IF EXISTS temp_fato_reserva;
DROP TEMPORARY TABLE IF EXISTS temp_fato_movimentacao_patio;


-- 1. Transformação para Dim_Empresa
-- Consolida dados de empresas de todos os grupos.
CREATE TEMPORARY TABLE temp_dim_empresa AS
SELECT
    t.id_empresa_origem,
    t.sistema_origem,
    COALESCE(t.nome_empresa, 'Empresa Desconhecida') AS nome_empresa,
    COALESCE(t.cnpj, '') AS cnpj,
    COALESCE(t.endereco, 'N/A') AS endereco_empresa, -- Padroniza nome da coluna
    COALESCE(t.telefone, '') AS telefone_empresa -- Garante existência da coluna
FROM stg_empresa t
GROUP BY t.id_empresa_origem, t.sistema_origem, t.nome_empresa, t.cnpj, t.endereco, t.telefone;


-- 2. Transformação para Dim_Patio
-- Consolida dados de pátios e denormaliza nome da empresa proprietária.
CREATE TEMPORARY TABLE temp_dim_patio AS
SELECT
    sp.id_patio_origem,
    sp.sistema_origem,
    COALESCE(sp.nome_patio, 'Pátio Desconhecido') AS nome_patio,
    COALESCE(sp.endereco, 'N/A') AS endereco_patio,
    -- Tentativa de extrair cidade e estado do endereço se não existirem campos específicos
    TRIM(COALESCE(sp.cidade, SUBSTRING_INDEX(SUBSTRING_INDEX(COALESCE(sp.endereco, ''), ',', 2), ',', -1))) AS cidade_patio, -- 'cidade' pode vir do DDL do Grupo 10
    TRIM(COALESCE(sp.estado_origem, SUBSTRING_INDEX(COALESCE(sp.endereco, ''), ',', -1))) AS estado_patio, -- 'estado_origem' pode vir do Grupo 10
    COALESCE(sp.capacidade_vagas, 0) AS capacidade_vagas_patio,
    tde.nome_empresa AS nome_empresa_proprietaria -- Denormaliza nome da empresa
FROM stg_patio sp
LEFT JOIN temp_dim_empresa tde ON sp.id_empresa_origem = tde.id_empresa_origem AND sp.sistema_origem = tde.sistema_origem;


-- 3. Transformação para Dim_Cliente
-- Padroniza tipo de cliente, consolida CPF/CNPJ e tenta extrair localização.
CREATE TEMPORARY TABLE temp_dim_cliente AS
SELECT
    sc.id_cliente_origem,
    sc.sistema_origem,
    CASE
        WHEN sc.tipo_cliente IN ('PF', 'F', 'FISICA', 'Pessoa Física') THEN 'PF'
        WHEN sc.tipo_cliente IN ('PJ', 'J', 'JURIDICA', 'Pessoa Jurídica') THEN 'PJ'
        ELSE 'Desconhecido'
    END AS tipo_cliente,
    COALESCE(sc.nome_razao_social, 'Cliente Desconhecido') AS nome_razao_social,
    COALESCE(sc.cpf, '') AS cpf,
    COALESCE(sc.cnpj, '') AS cnpj,
    COALESCE(sc.endereco, 'N/A') AS endereco,
    COALESCE(sc.telefone, '') AS telefone,
    COALESCE(sc.email, '') AS email,
    TRIM(COALESCE(sc.cidade_cliente, 'N/A')) AS cidade_cliente,
    TRIM(COALESCE(sc.estado_cliente, 'N/A')) AS estado_cliente,
    'Brasil' AS pais_cliente, -- Assumindo Brasil ou mapeamento mais complexo
    DATE(COALESCE(sc.data_cadastro, sc.data_carga)) AS data_cadastro -- Usa data de cadastro ou data de carga
FROM stg_cliente sc;


-- 4. Transformação para Dim_Veiculo
-- Denormaliza informações do grupo de veículo e infere acessórios/características.
CREATE TEMPORARY TABLE temp_dim_veiculo AS
SELECT
    sv.id_veiculo_origem,
    sv.sistema_origem,
    COALESCE(sv.placa, 'N/A') AS placa,
    COALESCE(sv.chassi, 'N/A') AS chassi,
    COALESCE(sv.marca, 'Desconhecida') AS marca,
    COALESCE(sv.modelo, 'Desconhecido') AS modelo,
    COALESCE(sv.ano_fabricacao, 1900) AS ano_fabricacao,
    COALESCE(sv.cor, 'Não Definida') AS cor,
    CASE
        WHEN sv.tipo_mecanizacao IN ('Automática', 'Auto', 'AUTOMATICA', 'A') THEN 'Automática'
        WHEN sv.tipo_mecanizacao IN ('Manual', 'MANUAL', 'M') THEN 'Manual'
        ELSE 'Desconhecida'
    END AS tipo_mecanizacao,
    COALESCE(sgv.nome_grupo, 'N/A') AS nome_grupo_veiculo,
    COALESCE(sgv.descricao, 'N/A') AS descricao_grupo_veiculo,
    COALESCE(sgv.valor_diaria_base, 0.00) AS valor_diaria_base_grupo,
    COALESCE(sv.url_foto_principal, '') AS url_foto_principal,
    -- Inferência de acessórios baseada em colunas de origem
    COALESCE(sv.tem_ar_condicionado, FALSE) AS tem_ar_condicionado,
    COALESCE(sv.tem_cadeirinha, FALSE) AS tem_cadeirinha
FROM stg_veiculo sv
LEFT JOIN stg_grupo_veiculo sgv ON sv.id_grupo_veiculo_origem = sgv.id_grupo_veiculo_origem AND sv.sistema_origem = sgv.sistema_origem;


-- 5. Transformação para Dim_Condutor
-- Padroniza nomes e garante dados de CNH.
CREATE TEMPORARY TABLE temp_dim_condutor AS
SELECT
    sc.id_condutor_origem,
    sc.sistema_origem,
    COALESCE(sc.nome_completo, 'Condutor Desconhecido') AS nome_completo_condutor,
    COALESCE(sc.numero_cnh, 'N/A') AS numero_cnh_condutor,
    COALESCE(sc.categoria_cnh, 'N/A') AS categoria_cnh_condutor,
    COALESCE(sc.data_expiracao_cnh, '1900-01-01') AS data_expiracao_cnh_condutor,
    COALESCE(sc.data_nascimento, '1900-01-01') AS data_nascimento_condutor,
    COALESCE(sc.nacionalidade, 'Desconhecida') AS nacionalidade_condutor,
    COALESCE(sc.tipo_documento_habilitacao, 'CNH Brasileira') AS tipo_documento_habilitacao,
    COALESCE(sc.pais_emissao_cnh, 'Brasil') AS pais_emissao_cnh,
    COALESCE(sc.data_entrada_brasil, '1900-01-01') AS data_entrada_brasil,
    COALESCE(sc.flag_traducao_juramentada, FALSE) AS flag_traducao_juramentada
FROM stg_condutor sc;


-- 6. Transformação para Dim_Seguro
-- Padroniza nomes e descrições de seguros.
CREATE TEMPORARY TABLE temp_dim_seguro AS
SELECT
    ss.id_seguro_origem,
    ss.sistema_origem,
    COALESCE(ss.nome_seguro, 'Seguro Padrão') AS nome_seguro,
    COALESCE(ss.descricao, 'N/A') AS descricao_seguro,
    COALESCE(ss.valor_diario, 0.00) AS valor_diario_seguro
FROM stg_seguro ss;


-- 7. Transformação para Fato_Locacao
-- Calcula medidas e prepara chaves de negócio para resolução de SKs na carga.
CREATE TEMPORARY TABLE temp_fato_locacao AS
SELECT
    sl.id_locacao_origem,
    sl.sistema_origem,
    -- Chaves de negócio para resolução de SKs de Dim_Tempo
    DATE(sl.data_hora_retirada_real) AS data_retirada_date,
    TIME(sl.data_hora_retirada_real) AS hora_retirada_time,
    DATE(sl.data_hora_devolucao_prevista) AS data_dev_prev_date,
    TIME(sl.data_hora_devolucao_prevista) AS hora_dev_prev_time,
    DATE(sl.data_hora_devolucao_real) AS data_dev_real_date,
    TIME(sl.data_hora_devolucao_real) AS hora_dev_real_time,
    -- Chaves de negócio para resolução de SKs de outras dimensões
    sl.id_cliente_origem,
    sl.id_veiculo_origem,
    sl.id_condutor_origem,
    sl.id_patio_retirada_real_origem,
    sl.id_patio_devolucao_prevista_origem,
    sl.id_patio_devolucao_real_origem,
    CASE
        WHEN sl.status_locacao IN ('Ativa', 'Ativo', 'EM_USO') THEN 'Ativa'
        WHEN sl.status_locacao IN ('Concluida', 'FINALIZADO', 'Finalizada') THEN 'Concluida'
        WHEN sl.status_locacao IN ('Cancelada', 'Cancelado') THEN 'Cancelada'
        ELSE 'Desconhecido'
    END AS status_locacao_nome,
    COALESCE(sl.id_seguro_contratado_origem, '-1') AS id_seguro_contratado_origem, -- Default para '-1' se não houver seguro

    -- Métricas:
    COALESCE(sl.valor_total_final, sl.valor_total_previsto, 0.00) AS valor_total_locacao,
    COALESCE(sl.valor_total_previsto, 0.00) AS valor_base_locacao,
    COALESCE((SELECT sc.valor_multas_taxas FROM stg_cobranca sc WHERE sc.id_locacao_origem = sl.id_locacao_origem AND sc.sistema_origem = sl.sistema_origem LIMIT 1), 0.00) AS valor_multas_taxas,
    COALESCE((SELECT sc.valor_seguro FROM stg_cobranca sc WHERE sc.id_locacao_origem = sl.id_locacao_origem AND sc.sistema_origem = sl.sistema_origem LIMIT 1), 0.00) AS valor_seguro,
    COALESCE((SELECT sc.valor_descontos FROM stg_cobranca sc WHERE sc.id_locacao_origem = sl.id_locacao_origem AND sc.sistema_origem = sl.sistema_origem LIMIT 1), 0.00) AS valor_descontos,

    IF(sl.quilometragem_devolucao IS NOT NULL AND sl.quilometragem_retirada IS NOT NULL, sl.quilometragem_devolucao - sl.quilometragem_retirada, NULL) AS quilometragem_percorrida,
    TIMESTAMPDIFF(HOUR, sl.data_hora_retirada_real, sl.data_hora_devolucao_prevista) AS duracao_locacao_horas_prevista,
    IF(sl.data_hora_devolucao_real IS NOT NULL, TIMESTAMPDIFF(HOUR, sl.data_hora_retirada_real, sl.data_hora_devolucao_real), NULL) AS duracao_locacao_horas_real,
    1 AS quant_locacoes
FROM stg_locacao sl;


-- 8. Transformação para Fato_Reserva
-- Prepara dados para o fato de reserva, calculando medidas como antecedência e duração.
CREATE TEMPORARY TABLE temp_fato_reserva AS
SELECT
    sr.id_reserva_origem,
    sr.sistema_origem,
    -- Chaves de negócio para resolução de SKs de Dim_Tempo
    DATE(sr.data_hora_reserva) AS data_reserva_date,
    TIME(sr.data_hora_reserva) AS hora_reserva_time,
    DATE(sr.data_hora_retirada_prevista) AS data_ret_prev_date,
    TIME(sr.data_hora_retirada_prevista) AS hora_ret_prev_time,
    DATE(sr.data_hora_devolucao_prevista) AS data_dev_prev_date,
    TIME(sr.data_hora_devolucao_prevista) AS hora_dev_prev_time,
    -- Chaves de negócio para resolução de SKs de outras dimensões
    sr.id_cliente_origem,
    COALESCE(sr.id_grupo_veiculo_origem, '-1') AS id_grupo_veiculo_origem, -- Default para '-1' se NULL (Grupo 2)
    sr.id_patio_retirada_previsto_origem,
    COALESCE(sr.status_reserva, 'Desconhecido') AS status_reserva_nome,

    -- Métricas:
    1 AS quant_reservas,
    DATEDIFF(sr.data_hora_retirada_prevista, sr.data_hora_reserva) AS dias_antecedencia_reserva,
    TIMESTAMPDIFF(HOUR, sr.data_hora_reserva, sr.data_hora_devolucao_prevista) AS duracao_reserva_horas_prevista
FROM stg_reserva sr;


-- 9. Transformação para Fato_Movimentacao_Patio
-- Deriva eventos de movimentação de pátio a partir de locações e logs de estado de veículo.
CREATE TEMPORARY TABLE temp_fato_movimentacao_patio AS
SELECT
    id_estado_veiculo_locacao_origem AS id_evento_origem,
    sistema_origem,
    DATE(data_hora_registro) AS data_mov_date,
    TIME(data_hora_registro) AS hora_mov_time,
    id_veiculo_origem,
    id_patio_origem,
    id_locacao_origem,
    CASE
        WHEN tipo_registro IN ('Entrega', 'Saída') THEN 'Saída'
        WHEN tipo_registro IN ('Devolucao', 'Entrada', 'Manutencao', 'Revisao', 'Prontuario', 'Foto') THEN 'Entrada' -- Mapeia diversos tipos para Entrada
        ELSE 'Desconhecido'
    END AS tipo_movimentacao_nome,
    quilometragem_evento,
    1 AS quant_movimentacoes,
    NULL AS ocupacao_veiculo_horas -- Calculado na carga para eventos de saída
FROM stg_estado_veiculo_locacao
WHERE id_veiculo_origem IS NOT NULL AND id_patio_origem IS NOT NULL -- Apenas eventos com veículo e pátio identificados
UNION ALL
-- Inferir eventos de entrada/saída de LOCACAO se não forem capturados por ESTADO_VEICULO_LOCACAO
SELECT
    sl.id_locacao_origem AS id_evento_origem, -- Usando ID da locação como ID do evento de movimentação
    sl.sistema_origem,
    DATE(sl.data_hora_retirada_real) AS data_mov_date,
    TIME(sl.data_hora_retirada_real) AS hora_mov_time,
    sl.id_veiculo_origem,
    sl.id_patio_retirada_real_origem AS id_patio_origem,
    NULL AS id_patio_destino, -- Saída do pátio
    'Saída' AS tipo_movimentacao_nome,
    NULL AS quilometragem_evento,
    1 AS quant_movimentacoes,
    NULL AS ocupacao_veiculo_horas
FROM stg_locacao sl
WHERE sl.id_locacao_origem NOT IN (SELECT id_locacao_origem FROM stg_estado_veiculo_locacao WHERE sistema_origem = sl.sistema_origem AND tipo_registro IN ('Entrega', 'Saída') AND id_locacao_origem IS NOT NULL) -- Evita duplicatas se já houver registro de saída na stg_estado_veiculo_locacao
UNION ALL
SELECT
    sl.id_locacao_origem AS id_evento_origem,
    sl.sistema_origem,
    DATE(sl.data_hora_devolucao_real) AS data_mov_date,
    TIME(sl.data_hora_devolucao_real) AS hora_mov_time,
    sl.id_veiculo_origem,
    sl.id_patio_retirada_real_origem AS id_patio_origem, -- Pátio de onde veio (retirada)
    sl.id_patio_devolucao_real_origem AS id_patio_destino, -- Pátio de destino (devolução)
    'Entrada' AS tipo_movimentacao_nome,
    NULL AS quilometragem_evento,
    1 AS quant_movimentacoes,
    NULL AS ocupacao_veiculo_horas
FROM stg_locacao sl
WHERE sl.data_hora_devolucao_real IS NOT NULL
AND sl.id_locacao_origem NOT IN (SELECT id_locacao_origem FROM stg_estado_veiculo_locacao WHERE sistema_origem = sl.sistema_origem AND tipo_registro IN ('Devolucao', 'Entrada') AND id_locacao_origem IS NOT NULL); -- Evita duplicatas


-- NOTA ADICIONAL: A lógica para 'sk_empresa_proprietaria_frota' e 'sk_empresa_patio'
-- na Fato_Movimentacao_Patio será resolvida na fase de carga, onde as SKs das empresas
-- e pátios já estarão disponíveis. Aqui, focamos em identificar o evento de movimentação.
