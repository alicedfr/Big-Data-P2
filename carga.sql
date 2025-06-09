-- SQL Script para Carga ETL (Fatos e Dimensões)
-- Este script carrega os dados transformados da área de staging (tabelas temporárias)
-- para as tabelas de fatos e dimensões do Data Warehouse.

USE locadora_dw;

-- NOTA: O banco de dados 'locadora_dw' e suas tabelas de dimensões e fatos
-- devem ter sido previamente criados (via 'dw_dimensional_creation.sql').
-- As dimensões fixas (Dim_Tempo, Dim_Status_Locacao, etc.) devem ser populadas
-- antes da execução deste script, especialmente Dim_Tempo que é vital para SKs.

-- --- CARGA DAS DIMENSÕES (SCD Type 1 e Type 2) ---

-- Carga da Dimensão de Empresa (Dim_Empresa) - SCD Tipo 1
-- ATUALIZA ou INSERE.
-- 'ON DUPLICATE KEY UPDATE' é ideal para SCD Tipo 1.
-- Uma UNIQUE INDEX na combinação (id_empresa_origem, sistema_origem) é necessária em Dim_Empresa
-- para que ON DUPLICATE KEY UPDATE funcione corretamente.
INSERT INTO Dim_Empresa (id_empresa_origem, sistema_origem, nome_empresa, cnpj_empresa, endereco_empresa)
SELECT
    id_empresa_origem,
    sistema_origem,
    nome_empresa,
    cnpj,
    endereco_empresa
FROM locadora_dw_staging.temp_dim_empresa
ON DUPLICATE KEY UPDATE -- Se a chave de negócio (id_origem, sistema_origem) já existe, atualiza
    nome_empresa = VALUES(nome_empresa),
    cnpj_empresa = VALUES(cnpj_empresa),
    endereco_empresa = VALUES(endereco_empresa);

-- Carga da Dimensão de Pátio (Dim_Patio) - SCD Tipo 1
-- ATUALIZA ou INSERE.
-- Uma UNIQUE INDEX na combinação (id_patio_origem, sistema_origem) é necessária em Dim_Patio.
INSERT INTO Dim_Patio (id_patio_origem, sistema_origem, nome_patio, endereco_patio, cidade_patio, estado_patio, capacidade_vagas_patio, nome_empresa_proprietaria)
SELECT
    id_patio_origem,
    sistema_origem,
    nome_patio,
    endereco_patio,
    cidade_patio,
    estado_patio,
    capacidade_vagas_patio,
    nome_empresa_proprietaria
FROM locadora_dw_staging.temp_dim_patio
ON DUPLICATE KEY UPDATE
    nome_patio = VALUES(nome_patio),
    endereco_patio = VALUES(endereco_patio),
    cidade_patio = VALUES(cidade_patio),
    estado_patio = VALUES(estado_patio),
    capacidade_vagas_patio = VALUES(capacidade_vagas_patio),
    nome_empresa_proprietaria = VALUES(nome_empresa_proprietaria);

-- Carga da Dimensão de Cliente (Dim_Cliente) - SCD Tipo 2
-- Lógica para inserir novas versões e desativar versões antigas.
INSERT INTO Dim_Cliente (id_cliente_origem, sistema_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, cidade_cliente, estado_cliente, pais_cliente, data_cadastro, data_inicio_vigencia, data_fim_vigencia, flag_ativo)
SELECT
    tdc.id_cliente_origem,
    tdc.sistema_origem,
    tdc.tipo_cliente,
    tdc.nome_razao_social,
    tdc.cpf,
    tdc.cnpj,
    tdc.endereco,
    tdc.telefone,
    tdc.email,
    tdc.cidade_cliente,
    tdc.estado_cliente,
    tdc.pais_cliente,
    tdc.data_cadastro,
    CURRENT_TIMESTAMP AS data_inicio_vigencia,
    '9999-12-31 23:59:59' AS data_fim_vigencia,
    TRUE AS flag_ativo
FROM locadora_dw_staging.temp_dim_cliente tdc
WHERE NOT EXISTS ( -- Insere se a chave de negócio (id_origem, sistema_origem) não existe ou se houve mudança em atributos rastreados
    SELECT 1
    FROM Dim_Cliente dc
    WHERE dc.id_cliente_origem = tdc.id_cliente_origem
    AND dc.sistema_origem = tdc.sistema_origem
    AND dc.flag_ativo = TRUE
    AND (
        dc.nome_razao_social = tdc.nome_razao_social AND
        dc.cpf = tdc.cpf AND
        dc.cnpj = tdc.cnpj AND
        dc.endereco = tdc.endereco AND
        dc.telefone = tdc.telefone AND
        dc.email = tdc.email AND
        dc.cidade_cliente = tdc.cidade_cliente AND
        dc.estado_cliente = tdc.estado_cliente AND
        dc.pais_cliente = tdc.pais_cliente
        -- Adicione TODAS as colunas que disparam uma nova versão aqui
    )
);

-- Desativar versões antigas (fechar a vigência) para clientes que tiveram alterações
UPDATE Dim_Cliente dc
JOIN locadora_dw_staging.temp_dim_cliente tdc
    ON dc.id_cliente_origem = tdc.id_cliente_origem
    AND dc.sistema_origem = tdc.sistema_origem
    AND dc.flag_ativo = TRUE
WHERE
    (   -- Condição para detectar mudança em qualquer atributo rastreado para SCD Tipo 2
        dc.nome_razao_social <> tdc.nome_razao_social OR
        dc.cpf <> tdc.cpf OR
        dc.cnpj <> tdc.cnpj OR
        dc.endereco <> tdc.endereco OR
        dc.telefone <> tdc.telefone OR
        dc.email <> tdc.email OR
        dc.cidade_cliente <> tdc.cidade_cliente OR
        dc.estado_cliente <> tdc.estado_cliente OR
        dc.pais_cliente <> tdc.pais_cliente
        -- Adicione TODAS as colunas que disparam uma nova versão aqui
    )
    AND dc.data_fim_vigencia = '9999-12-31 23:59:59';
-- Apenas para versões ativas atualmente

-- Carga da Dimensão de Veículo (Dim_Veiculo) - SCD Tipo 2
-- Lógica similar à de Cliente para inserir novas versões e desativar antigas.
INSERT INTO Dim_Veiculo (id_veiculo_origem, sistema_origem, placa, chassi, marca, modelo, ano_fabricacao, cor, tipo_mecanizacao, nome_grupo_veiculo, descricao_grupo_veiculo, valor_diaria_base_grupo, url_foto_principal, tem_ar_condicionado, tem_cadeirinha, data_inicio_vigencia, data_fim_vigencia, flag_ativo)
SELECT
    tdv.id_veiculo_origem,
    tdv.sistema_origem,
    tdv.placa,
    tdv.chassi,
    tdv.marca,
    tdv.modelo,
    tdv.ano_fabricacao,
    tdv.cor,
    tdv.tipo_mecanizacao,
    tdv.nome_grupo_veiculo,
    tdv.descricao_grupo_veiculo,
    tdv.valor_diaria_base_grupo,
    tdv.url_foto_principal,
    tdv.tem_ar_condicionado,
    tdv.tem_cadeirinha,
    CURRENT_TIMESTAMP AS data_inicio_vigencia,
    '9999-12-31 23:59:59' AS data_fim_vigencia,
    TRUE AS flag_ativo
FROM locadora_dw_staging.temp_dim_veiculo tdv
WHERE NOT EXISTS (
    SELECT 1
    FROM Dim_Veiculo dv
    WHERE dv.id_veiculo_origem = tdv.id_veiculo_origem
    AND dv.sistema_origem = tdv.sistema_origem
    AND dv.flag_ativo = TRUE
    AND (
        dv.placa = tdv.placa AND
        dv.chassi = tdv.chassi AND
        dv.marca = tdv.marca AND
        dv.modelo = tdv.modelo AND
        dv.ano_fabricacao = tdv.ano_fabricacao AND
        dv.cor = tdv.cor AND
        dv.tipo_mecanizacao = tdv.tipo_mecanizacao AND
        dv.nome_grupo_veiculo = tdv.nome_grupo_veiculo AND
        dv.descricao_grupo_veiculo = tdv.descricao_grupo_veiculo AND
        dv.valor_diaria_base_grupo = tdv.valor_diaria_base_grupo AND
        dv.url_foto_principal = tdv.url_foto_principal AND
        dv.tem_ar_condicionado = tdv.tem_ar_condicionado AND
        dv.tem_cadeirinha = tdv.tem_cadeirinha
        -- Adicione TODAS as colunas que disparam uma nova versão aqui
    )
);

-- Desativar versões antigas (fechar a vigência) para veículos que tiveram alterações
UPDATE Dim_Veiculo dv
JOIN locadora_dw_staging.temp_dim_veiculo tdv
    ON dv.id_veiculo_origem = tdv.id_veiculo_origem
    AND dv.sistema_origem = tdv.sistema_origem
    AND dv.flag_ativo = TRUE
WHERE
    (
        dv.placa <> tdv.placa OR
        dv.chassi <> tdv.chassi OR
        dv.marca <> tdv.marca OR
        dv.modelo <> tdv.modelo OR
        dv.ano_fabricacao <> tdv.ano_fabricacao OR
        dv.cor <> tdv.cor OR
        dv.tipo_mecanizacao <> tdv.tipo_mecanizacao OR
        dv.nome_grupo_veiculo <> tdv.nome_grupo_veiculo OR
        dv.descricao_grupo_veiculo <> tdv.descricao_grupo_veiculo OR
        dv.valor_diaria_base_grupo <> tdv.valor_diaria_base_grupo OR
        dv.url_foto_principal <> tdv.url_foto_principal OR
        dv.tem_ar_condicionado <> tdv.tem_ar_condicionado OR
        dv.tem_cadeirinha <> tdv.tem_cadeirinha
        -- Adicione TODAS as colunas que disparam uma nova versão aqui
    )
    AND dv.data_fim_vigencia = '9999-12-31 23:59:59';

-- Carga da Dimensão de Condutor (Dim_Condutor) - SCD Tipo 1
INSERT INTO Dim_Condutor (id_condutor_origem, sistema_origem, nome_completo_condutor, numero_cnh_condutor, categoria_cnh_condutor, data_expiracao_cnh_condutor, data_nascimento_condutor, nacionalidade_condutor, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada)
SELECT
    id_condutor_origem,
    sistema_origem,
    nome_completo_condutor,
    numero_cnh_condutor,
    categoria_cnh_condutor,
    data_expiracao_cnh_condutor,
    data_nascimento_condutor,
    nacionalidade_condutor,
    tipo_documento_habilitacao,
    pais_emissao_cnh,
    data_entrada_brasil,
    flag_traducao_juramentada
FROM locadora_dw_staging.temp_dim_condutor
ON DUPLICATE KEY UPDATE
    nome_completo_condutor = VALUES(nome_completo_condutor),
    numero_cnh_condutor = VALUES(numero_cnh_condutor),
    categoria_cnh_condutor = VALUES(categoria_cnh_condutor),
    data_expiracao_cnh_condutor = VALUES(data_expiracao_cnh_condutor),
    data_nascimento_condutor = VALUES(data_nascimento_condutor),
    nacionalidade_condutor = VALUES(nacionalidade_condutor),
    tipo_documento_habilitacao = VALUES(tipo_documento_habilitacao),
    pais_emissao_cnh = VALUES(pais_emissao_cnh),
    data_entrada_brasil = VALUES(data_entrada_brasil),
    flag_traducao_juramentada = VALUES(flag_traducao_juramentada);

-- Carga da Dimensão de Seguro (Dim_Seguro) - SCD Tipo 1
INSERT INTO Dim_Seguro (id_seguro_origem, sistema_origem, nome_seguro, descricao_seguro, valor_diario_seguro)
SELECT
    id_seguro_origem,
    sistema_origem,
    nome_seguro,
    descricao_seguro,
    valor_diario_seguro
FROM locadora_dw_staging.temp_dim_seguro
ON DUPLICATE KEY UPDATE
    nome_seguro = VALUES(nome_seguro),
    descricao_seguro = VALUES(descricao_seguro),
    valor_diario_seguro = VALUES(valor_diario_seguro);

-- --- CARGA DAS TABELAS DE FATOS ---

-- Carga da Tabela de Fatos: Fato_Locacao
INSERT INTO Fato_Locacao (sk_tempo_retirada, sk_tempo_devolucao_prevista, sk_tempo_devolucao_real, sk_cliente, sk_veiculo, sk_condutor, sk_patio_retirada, sk_patio_devolucao_prevista, sk_patio_devolucao_real, sk_status_locacao, sk_seguro, id_locacao_origem, sistema_origem, valor_total_locacao, valor_base_locacao, valor_multas_taxas, valor_seguro, valor_descontos, quilometragem_percorrida, duracao_locacao_horas_prevista, duracao_locacao_horas_real, quant_locacoes)
SELECT
    -- Resolução de SKs para Dim_Tempo (assumindo Dim_Tempo pré-populada com granularidade de dia e hora/minuto)
    (SELECT sk_tempo FROM Dim_Tempo WHERE data_completa = tfl.data_retirada_date AND HOUR = HOUR(tfl.hora_retirada_time) AND MINUTE = MINUTE(tfl.hora_retirada_time) LIMIT 1) AS sk_tempo_retirada,
    (SELECT sk_tempo FROM Dim_Tempo WHERE data_completa = tfl.data_dev_prev_date AND HOUR = HOUR(tfl.hora_dev_prev_time) AND MINUTE = MINUTE(tfl.hora_dev_prev_time) LIMIT 1) AS sk_tempo_devolucao_prevista,
    (SELECT sk_tempo FROM Dim_Tempo WHERE data_completa = tfl.data_dev_real_date AND HOUR = HOUR(tfl.hora_dev_real_time) AND MINUTE = MINUTE(tfl.hora_dev_real_time) LIMIT 1) AS sk_tempo_devolucao_real,
    -- Resolução de SKs para outras dimensões (usando as chaves de negócio + sistema_origem)
    (SELECT sk_cliente FROM Dim_Cliente WHERE id_cliente_origem = tfl.id_cliente_origem AND sistema_origem = tfl.sistema_origem AND flag_ativo = TRUE LIMIT 1) AS sk_cliente,
    (SELECT sk_veiculo FROM Dim_Veiculo WHERE id_veiculo_origem = tfl.id_veiculo_origem AND sistema_origem = tfl.sistema_origem AND flag_ativo = TRUE LIMIT 1) AS sk_veiculo,
    (SELECT sk_condutor FROM Dim_Condutor WHERE id_condutor_origem = tfl.id_condutor_origem AND sistema_origem = tfl.sistema_origem LIMIT 1) AS sk_condutor,
    (SELECT sk_patio FROM Dim_Patio WHERE id_patio_origem = tfl.id_patio_retirada_real_origem AND sistema_origem = tfl.sistema_origem LIMIT 1) AS sk_patio_retirada,
    (SELECT sk_patio FROM Dim_Patio WHERE id_patio_origem = tfl.id_patio_devolucao_prevista_origem AND sistema_origem = tfl.sistema_origem LIMIT 1) AS sk_patio_devolucao_prevista,
    (SELECT sk_patio FROM Dim_Patio WHERE id_patio_origem = tfl.id_patio_devolucao_real_origem AND sistema_origem = tfl.sistema_origem LIMIT 1) AS sk_patio_devolucao_real,
    (SELECT sk_status_locacao FROM Dim_Status_Locacao WHERE nome_status_locacao = tfl.status_locacao_nome LIMIT 1) AS sk_status_locacao,
    (SELECT sk_seguro FROM Dim_Seguro WHERE id_seguro_contratado_origem = tfl.id_seguro_contratado_origem AND sistema_origem = tfl.sistema_origem LIMIT 1) AS sk_seguro,
    tfl.id_locacao_origem,
    tfl.sistema_origem,
    tfl.valor_total_locacao,
    tfl.valor_base_locacao,
    tfl.valor_multas_taxas,
    tfl.valor_seguro,
    tfl.valor_descontos,
    tfl.quilometragem_percorrida,
    tfl.duracao_locacao_horas_prevista,
    tfl.duracao_locacao_horas_real,
    tfl.quant_locacoes
FROM locadora_dw_staging.temp_fato_locacao tfl
WHERE NOT EXISTS ( -- Evita duplicatas em recargas (incremental)
    SELECT 1
    FROM Fato_Locacao fl
    WHERE fl.id_locacao_origem = tfl.id_locacao_origem
    AND fl.sistema_origem = tfl.sistema_origem
);

-- Carga da Tabela de Fatos: Fato_Reserva
INSERT INTO Fato_Reserva (sk_tempo_reserva, sk_tempo_retirada_prevista, sk_tempo_devolucao_prevista, sk_cliente, sk_grupo_veiculo, sk_patio_retirada, sk_status_reserva, id_reserva_origem, sistema_origem, quant_reservas, dias_antecedencia_reserva, duracao_reserva_horas_prevista)
SELECT
    -- Resolução de SKs para Dim_Tempo
    (SELECT sk_tempo FROM Dim_Tempo WHERE data_completa = tfr.data_reserva_date AND HOUR = HOUR(tfr.hora_reserva_time) AND MINUTE = MINUTE(tfr.hora_reserva_time) LIMIT 1) AS sk_tempo_reserva,
    (SELECT sk_tempo FROM Dim_Tempo WHERE data_completa = tfr.data_ret_prev_date AND HOUR = HOUR(tfr.hora_ret_prev_time) AND MINUTE = MINUTE(tfr.hora_ret_prev_time) LIMIT 1) AS sk_tempo_retirada_prevista,
    (SELECT sk_tempo FROM Dim_Tempo WHERE data_completa = tfr.data_dev_prev_date AND HOUR = HOUR(tfr.hora_dev_prev_time) AND MINUTE = MINUTE(tfr.hora_dev_prev_time) LIMIT 1) AS sk_tempo_devolucao_prevista,
    -- Resolução de SKs para outras dimensões
    (SELECT sk_cliente FROM Dim_Cliente WHERE id_cliente_origem = tfr.id_cliente_origem AND sistema_origem = tfr.sistema_origem AND flag_ativo = TRUE LIMIT 1) AS sk_cliente,
    -- NOTA: sk_grupo_veiculo referencia Dim_Veiculo.
    (SELECT sk_veiculo FROM Dim_Veiculo WHERE id_veiculo_origem = tfr.id_grupo_veiculo_origem AND sistema_origem = tfr.sistema_origem AND flag_ativo = TRUE LIMIT 1) AS sk_grupo_veiculo,
    (SELECT sk_patio FROM Dim_Patio WHERE id_patio_origem = tfr.id_patio_retirada_previsto_origem AND sistema_origem = tfr.sistema_origem LIMIT 1) AS sk_patio_retirada,
    (SELECT sk_status_reserva FROM Dim_Status_Reserva WHERE nome_status_reserva = tfr.status_reserva_nome LIMIT 1) AS sk_status_reserva,
    tfr.id_reserva_origem,
    tfr.sistema_origem,
    tfr.quant_reservas,
    tfr.dias_antecedencia_reserva,
    tfr.duracao_reserva_horas_prevista
FROM locadora_dw_staging.temp_fato_reserva tfr
WHERE NOT EXISTS (
    SELECT 1
    FROM Fato_Reserva fr
    WHERE fr.id_reserva_origem = tfr.id_reserva_origem
    AND fr.sistema_origem = tfr.sistema_origem
);

-- Carga da Tabela de Fatos: Fato_Movimentacao_Patio
INSERT INTO Fato_Movimentacao_Patio (sk_tempo_movimentacao, sk_veiculo, sk_patio_origem, sk_patio_destino, sk_tipo_movimentacao, sk_empresa_proprietaria_frota, sk_empresa_patio, id_estado_veiculo_locacao_origem, id_locacao_origem, sistema_origem, quant_movimentacoes, ocupacao_veiculo_horas)
SELECT
    -- Resolução de SKs para Dim_Tempo
    (SELECT sk_tempo FROM Dim_Tempo WHERE data_completa = tfmp.data_mov_date AND HOUR = HOUR(tfmp.hora_mov_time) AND MINUTE = MINUTE(tfmp.hora_mov_time) LIMIT 1) AS sk_tempo_movimentacao,
    -- Resolução de SKs para outras dimensões
    (SELECT sk_veiculo FROM Dim_Veiculo WHERE id_veiculo_origem = tfmp.id_veiculo_origem AND sistema_origem = tfmp.sistema_origem AND flag_ativo = TRUE LIMIT 1) AS sk_veiculo,
    (SELECT sk_patio FROM Dim_Patio WHERE id_patio_origem = tfmp.id_patio_origem AND sistema_origem = tfmp.sistema_origem LIMIT 1) AS sk_patio_origem,
    (SELECT sk_patio FROM Dim_Patio WHERE id_patio_origem = tfmp.id_patio_destino AND sistema_origem = tfmp.sistema_origem LIMIT 1) AS sk_patio_destino,
    (SELECT sk_tipo_movimentacao FROM Dim_Tipo_Movimentacao_Patio WHERE nome_tipo_movimentacao = tfmp.tipo_movimentacao_nome LIMIT 1) AS sk_tipo_movimentacao,
    (SELECT sk_empresa FROM Dim_Empresa WHERE id_empresa_origem = tfmp.id_empresa_proprietaria_frota_origem AND sistema_origem = tfmp.sistema_origem LIMIT 1) AS sk_empresa_proprietaria_frota,
    (SELECT sk_empresa FROM Dim_Empresa WHERE id_empresa_origem = tfmp.id_empresa_patio_origem AND sistema_origem = tfmp.sistema_origem LIMIT 1) AS sk_empresa_patio,
    tfmp.id_evento_origem AS id_estado_veiculo_locacao_origem, -- Reuso de campo para rastreabilidade
    tfmp.id_locacao_origem, -- Pode ser NULL
    tfmp.sistema_origem,
    tfmp.quant_movimentacoes,
    tfmp.ocupacao_veiculo_horas
FROM locadora_dw_staging.temp_fato_movimentacao_patio tfmp
WHERE NOT EXISTS (
    SELECT 1
    FROM Fato_Movimentacao_Patio fmp
    WHERE fmp.id_estado_veiculo_locacao_origem = tfmp.id_evento_origem -- Assumindo que id_evento_origem é único para movimentação
    AND fmp.sistema_origem = tfmp.sistema_origem
);
