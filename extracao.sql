-- SQL Script para Extração ETL para a Área de Staging
-- Adaptado ao modelo físico do Grupo A (locadora_veiculos) e aos DDLs dos Grupos 2 a 10.

USE locadora_dw_staging;

-- NOTA SOBRE EXTRAÇÃO INCREMENTAL:
-- Para uma extração incremental robusta, as tabelas OLTP de origem (de todos os grupos)
-- DEVEM possuir colunas de timestamp (ex: `data_ultima_atualizacao`, `data_criacao`, `data_evento`).
-- As cláusulas `WHERE [coluna_timestamp] > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_tabela WHERE sistema_origem = '[origem]')`
-- são usadas para isso. Se a coluna de timestamp não existir em uma tabela de origem,
-- os dados serão extraídos integralmente (ou uma estratégia diferente, como baseada em IDs,
-- seria necessária para o delta). Estou assumindo a existência dessas colunas para a extração incremental.
-- Se uma tabela não tiver uma coluna de timestamp explícita, a condição WHERE será baseada
-- na primeira data de evento/criação/registro disponível ou omitida para uma carga completa.

-- ---
-- **Extração do NOSSO SISTEMA (locadora_veiculos - Grupo A)**
-- Frequência de Acionamento: Diária.

-- Extração de EMPRESA
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, endereco, telefone, sistema_origem, data_carga)
SELECT
    CAST(id_empresa AS VARCHAR(50)),
    nome_empresa,
    cnpj,
    endereco,
    telefone,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.EMPRESA
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'GrupoA');

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, capacidade_vagas, sistema_origem, data_carga)
SELECT
    CAST(id_patio AS VARCHAR(50)),
    CAST(id_empresa AS VARCHAR(50)),
    nome_patio,
    endereco,
    capacidade_vagas,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.PATIO
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'GrupoA');

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem, data_carga)
SELECT
    CAST(id_grupo_veiculo AS VARCHAR(50)),
    nome_grupo,
    descricao,
    valor_diaria_base,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.GRUPO_VEICULO
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'GrupoA');

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, id_patio_atual_origem, placa, chassi, marca, modelo, ano_fabricacao, cor, tipo_mecanizacao, quilometragem_atual, status_veiculo, url_foto_principal, sistema_origem, data_carga)
SELECT
    CAST(id_veiculo AS VARCHAR(50)),
    CAST(id_grupo_veiculo AS VARCHAR(50)),
    CAST(id_patio_atual AS VARCHAR(50)),
    placa,
    chassi,
    marca,
    modelo,
    ano_fabricacao,
    cor,
    tipo_mecanizacao,
    quilometragem_atual,
    status_veiculo,
    url_foto,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.VEICULO
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'GrupoA');

-- Extração de CLIENTE
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem, data_carga)
SELECT
    CAST(id_cliente AS VARCHAR(50)),
    tipo_cliente,
    nome_razao_social,
    cpf,
    cnpj,
    endereco,
    telefone,
    email,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.CLIENTE
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'GrupoA');

-- Extração de CONDUTOR
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(id_condutor AS VARCHAR(50)),
    CAST(id_cliente AS VARCHAR(50)),
    NULL AS id_funcionario_pj_origem, -- Ausente no DDL do Grupo A
    nome_completo,
    numero_cnh,
    categoria_cnh,
    data_expiracao_cnh,
    data_nascimento,
    NULL AS nacionalidade, -- Ausente no DDL do Grupo A
    NULL AS tipo_documento_habilitacao, -- Ausente no DDL do Grupo A
    NULL AS pais_emissao_cnh, -- Ausente no DDL do Grupo A
    NULL AS data_entrada_brasil, -- Ausente no DDL do Grupo A
    NULL AS flag_traducao_juramentada, -- Ausente no DDL do Grupo A
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.CONDUTOR
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'GrupoA');

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(id_reserva AS VARCHAR(50)),
    CAST(id_cliente AS VARCHAR(50)),
    CAST(id_grupo_veiculo AS VARCHAR(50)),
    CAST(id_patio_retirada_previsto AS VARCHAR(50)),
    NULL AS id_patio_devolucao_previsto_origem, -- Ausente no DDL do Grupo A
    data_hora_reserva,
    data_hora_retirada_prevista,
    data_hora_devolucao_prevista,
    status_reserva,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.RESERVA
WHERE data_hora_reserva > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'GrupoA');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_condutor_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(id_locacao AS VARCHAR(50)),
    CAST(id_reserva AS VARCHAR(50)),
    CAST(id_cliente AS VARCHAR(50)),
    CAST(id_veiculo AS VARCHAR(50)),
    CAST(id_condutor AS VARCHAR(50)),
    CAST(id_patio_retirada_real AS VARCHAR(50)),
    CAST(id_patio_devolucao_prevista AS VARCHAR(50)),
    CAST(id_patio_devolucao_real AS VARCHAR(50)),
    data_hora_retirada_real,
    data_hora_devolucao_prevista,
    data_hora_devolucao_real,
    quilometragem_retirada,
    quilometragem_devolucao,
    valor_total_previsto,
    valor_total_final,
    status_locacao,
    NULL AS id_seguro_contratado_origem, -- Ausente no DDL do Grupo A
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.LOCACAO
WHERE data_hora_retirada_real > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'GrupoA');

-- Extração de SEGURO
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem, data_carga)
SELECT
    CAST(id_seguro AS VARCHAR(50)),
    nome_seguro,
    descricao,
    valor_diario,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.SEGURO
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'GrupoA');

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_multas_taxas, valor_seguro, valor_descontos, valor_final_cobranca, status_pagamento, data_vencimento, data_pagamento, sistema_origem, data_carga)
SELECT
    CAST(id_cobranca AS VARCHAR(50)),
    CAST(id_locacao AS VARCHAR(50)),
    CAST(data_cobranca AS DATETIME),
    valor_base,
    valor_multas_taxas,
    valor_seguro,
    valor_descontos,
    valor_final_cobranca,
    status_pagamento,
    data_vencimento,
    CAST(data_pagamento AS DATETIME),
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.COBRANCA
WHERE data_cobranca > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'GrupoA');

-- Extração de ESTADO_VEICULO_LOCACAO (inferindo de PRONTUARIO e FOTO_VEICULO do Grupo A)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, nivel_combustivel, condicao_geral, observacoes, quilometragem_evento, sistema_origem, data_carga)
SELECT
    CAST(p.id_prontuario AS VARCHAR(50)),
    NULL AS id_locacao_origem, -- Prontuário pode não estar diretamente ligado à locação
    CAST(p.id_veiculo AS VARCHAR(50)),
    (SELECT CAST(v.id_patio_atual AS VARCHAR(50)) FROM locadora_veiculos.VEICULO v WHERE v.id_veiculo = p.id_veiculo LIMIT 1) AS id_patio_origem, -- Pátio atual do veículo no momento do prontuário
    'Manutencao' AS tipo_registro,
    CAST(p.data_ultima_revisao AS DATETIME),
    NULL AS nivel_combustivel,
    p.observacoes AS condicao_geral,
    p.observacoes,
    p.quilometragem_ultima_revisao,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.PRONTUARIO p
WHERE p.data_ultima_revisao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'GrupoA')
UNION ALL
SELECT
    CAST(fv.foto_id AS VARCHAR(50)),
    NULL AS id_locacao_origem, -- Fotos podem não estar ligadas diretamente à locação
    CAST(fv.id_veiculo AS VARCHAR(50)),
    (SELECT CAST(v.id_patio_atual AS VARCHAR(50)) FROM locadora_veiculos.VEICULO v WHERE v.id_veiculo = fv.id_veiculo LIMIT 1) AS id_patio_origem,
    fv.tipo_foto, -- Ex: 'Propaganda', 'Entrega', 'Devolucao'
    fv.data_ultima_atualizacao AS data_hora_registro, -- Assumindo data_ultima_atualizacao na FOTO_VEICULO
    NULL AS nivel_combustivel,
    fv.descricao_foto AS condicao_geral,
    NULL AS observacoes,
    NULL AS quilometragem_evento,
    'GrupoA' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_veiculos.FOTO_VEICULO fv
WHERE fv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'GrupoA');


---
-- **Extração de OUTROS SISTEMAS - Adaptado aos DDLs fornecidos**
-- Frequência de Acionamento: Diária ou com periodicidade definida com cada grupo.

-- ---
-- **GRUPO 2** (Assumimos banco de dados 'locadora_grupo_2')

-- Extração de CLIENTE (Condutor e Cliente combinados no DDL do Grupo 2)
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem, data_carga)
SELECT
    CAST(c.id_cliente AS VARCHAR(50)),
    c.tipo,
    c.nome,
    CASE WHEN c.tipo = 'PF' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo = 'PJ' THEN c.cpf_cnpj ELSE NULL END,
    NULL AS endereco, -- Ausente no DDL do Grupo 2
    c.telefone,
    c.email,
    'Grupo2' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_2.CLIENTE c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo2'); -- Assumindo data_ultima_atualizacao

-- Extração de CONDUTOR (derivado de CLIENTE do Grupo 2)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(c.id_cliente AS VARCHAR(50)), -- Usando id_cliente como id_condutor_origem
    CAST(c.id_cliente AS VARCHAR(50)),
    NULL AS id_funcionario_pj_origem,
    c.nome,
    c.cnh,
    c.categoria_cnh,
    c.validade_cnh,
    NULL AS data_nascimento,
    NULL AS nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo2' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_2.CLIENTE c
WHERE c.cnh IS NOT NULL AND c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo2'); -- Assumindo data_ultima_atualizacao

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, placa, chassi, marca, modelo, cor, sistema_origem, data_carga)
SELECT
    CAST(v.id_veiculo AS VARCHAR(50)),
    v.placa,
    v.chassi,
    v.marca,
    v.modelo,
    v.cor,
    'Grupo2' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_2.VEICULO v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo2'); -- Assumindo data_ultima_atualizacao

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, nome_patio, endereco, sistema_origem, data_carga)
SELECT
    CAST(p.id_patio AS VARCHAR(50)),
    p.localizacao,
    p.localizacao, -- Usando localização como endereço também
    'Grupo2' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_2.PATIO p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo2'); -- Assumindo data_ultima_atualizacao

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.id_reserva AS VARCHAR(50)),
    CAST(r.cliente_id AS VARCHAR(50)),
    CAST(r.veiculo_id AS VARCHAR(50)), -- Mapeando 'veiculo_id' para 'id_grupo_veiculo_origem' na staging
    CAST(r.patio_retirada_id AS VARCHAR(50)),
    NULL AS id_patio_devolucao_previsto_origem, -- Ausente no DDL do Grupo 2
    CAST(r.data_inicio AS DATETIME),
    CAST(r.data_inicio AS DATETIME),
    CAST(r.data_fim AS DATETIME),
    r.status,
    'Grupo2' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_2.RESERVA r
WHERE r.data_inicio > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo2');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_condutor_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(l.id_locacao AS VARCHAR(50)),
    CAST(l.reserva_id AS VARCHAR(50)),
    CAST(l.cliente_id AS VARCHAR(50)),
    NULL AS id_condutor_origem, -- Ausente no DDL do Grupo 2
    CAST(l.veiculo_id AS VARCHAR(50)),
    NULL AS id_patio_retirada_real_origem, -- Ausente no DDL do Grupo 2
    NULL AS id_patio_devolucao_prevista_origem, -- Ausente no DDL do Grupo 2
    CAST(l.patio_entrega_id AS VARCHAR(50)), -- Usando patio_entrega_id como pátio de devolução real
    NULL AS data_hora_retirada_real, -- Ausente no DDL do Grupo 2
    NULL AS data_hora_devolucao_prevista, -- Ausente no DDL do Grupo 2
    NULL AS data_hora_devolucao_real, -- Ausente no DDL do Grupo 2
    NULL AS quilometragem_retirada, -- Ausente no DDL do Grupo 2
    NULL AS quilometragem_devolucao, -- Ausente no DDL do Grupo 2
    l.valor_total,
    l.valor_total,
    'Desconhecido' AS status_locacao, -- Ausente no DDL do Grupo 2
    NULL AS id_seguro_contratado_origem,
    'Grupo2' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_2.LOCACAO l
WHERE l.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo2'); -- Assumindo data_ultima_atualizacao

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, data_pagamento, status_pagamento, sistema_origem, data_carga)
SELECT
    CAST(c.id_cobranca AS VARCHAR(50)),
    CAST(c.locacao_id AS VARCHAR(50)),
    CAST(c.data_pagamento AS DATETIME),
    c.valor_pago,
    c.valor_pago,
    CAST(c.data_pagamento AS DATETIME),
    c.forma_pagamento, -- Usando forma_pagamento como status (exige transformação posterior)
    'Grupo2' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_2.COBRANCA c
WHERE c.data_pagamento > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo2');

-- Extração de ESTADO_VEICULO_LOCACAO (Inferindo de VEICULO e LOCACAO do Grupo 2)
-- Grupo 2 não tem tabelas explícitas de estado de veículo ou movimentação de pátio detalhada.
-- Precisamos inferir eventos de locação/devolução.
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, sistema_origem, data_carga)
SELECT
    CAST(l.id_locacao AS VARCHAR(50)), -- Usando id_locacao como id_evento
    CAST(l.id_locacao AS VARCHAR(50)),
    CAST(l.veiculo_id AS VARCHAR(50)),
    CAST(l.patio_entrega_id AS VARCHAR(50)), -- Assumindo este é o pátio de onde o veículo foi devolvido
    'Devolucao' AS tipo_registro, -- Registrar a devolução
    CAST(l.data_ultima_atualizacao AS DATETIME), -- Assumindo data_ultima_atualizacao da LOCACAO para o timestamp do evento
    'Grupo2' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_2.LOCACAO l
WHERE l.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo2');


-- ---
-- **GRUPO 3** (Assumimos banco de dados 'locadora_grupo_3')

-- Extração de EMPRESA
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, sistema_origem, data_carga)
SELECT
    CAST(e.id AS VARCHAR(50)),
    e.nome_fantasia,
    CAST(e.cnpj AS VARCHAR(18)),
    NULL AS endereco, -- Ausente no DDL
    NULL AS telefone, -- Ausente no DDL
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.Empresa e
WHERE e.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo3'); -- Assumindo data_ultima_atualizacao

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, capacidade_vagas, sistema_origem, data_carga)
SELECT
    CAST(p.id AS VARCHAR(50)),
    CAST(p.empresa_id AS VARCHAR(50)),
    p.nome,
    p.endereco,
    p.total_vagas,
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.Patio p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo3'); -- Assumindo data_ultima_atualizacao

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem, data_carga)
SELECT
    CAST(gv.id AS VARCHAR(50)),
    gv.codigo_grupo,
    gv.descricao,
    gv.preco_diario,
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.GrupoVeiculo gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo3'); -- Assumindo data_ultima_atualizacao

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, ano_fabricacao, quilometragem_atual, tem_ar_condicionado, sistema_origem, data_carga)
SELECT
    CAST(v.id AS VARCHAR(50)),
    CAST(v.grupo_id AS VARCHAR(50)),
    CAST(v.placa AS VARCHAR(10)),
    CAST(v.chassi AS VARCHAR(20)),
    v.marca,
    v.modelo,
    v.cor,
    v.transmissao, -- Já é 'automatico' ou 'manual'
    v.ano,
    v.quilometragem,
    v.ar_condicionado,
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.Veiculo v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo3'); -- Assumindo data_ultima_atualizacao

-- Extração de CLIENTE
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem, data_carga)
SELECT
    CAST(c.id AS VARCHAR(50)),
    c.tipo,
    c.nome_razao,
    CASE WHEN c.tipo = 'PF' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo = 'PJ' THEN c.cpf_cnpj ELSE NULL END,
    c.endereco,
    COALESCE(c.telefone1, c.telefone2),
    c.email,
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.Cliente c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo3'); -- Assumindo data_ultima_atualizacao

-- Extração de CONDUTOR
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(c.id AS VARCHAR(50)),
    CAST(c.cliente_id AS VARCHAR(50)),
    NULL AS id_funcionario_pj_origem, -- Ausente no DDL
    c.nome,
    c.cnh,
    c.categoria_cnh,
    c.validade_cnh,
    NULL AS data_nascimento,
    NULL AS nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.Condutor c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo3'); -- Assumindo data_ultima_atualizacao

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.id AS VARCHAR(50)),
    CAST(r.cliente_id AS VARCHAR(50)),
    CAST(r.grupo_id AS VARCHAR(50)),
    CAST(r.patio_retirada_id AS VARCHAR(50)),
    CAST(r.patio_devolucao_id AS VARCHAR(50)),
    CAST(r.data_prev_retirada AS DATETIME), -- Usando data_prev_retirada como data da reserva
    CAST(r.data_prev_retirada AS DATETIME),
    CAST(r.data_prev_devolucao AS DATETIME),
    r.status,
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.Reserva r
WHERE r.data_prev_retirada > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo3');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_condutor_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(l.id AS VARCHAR(50)),
    CAST(l.reserva_id AS VARCHAR(50)),
    NULL AS id_cliente_origem, -- Cliente ID não diretamente na LOCACAO, precisa de join Condutor->Cliente
    CAST(l.veiculo_id AS VARCHAR(50)),
    CAST(l.condutor_id AS VARCHAR(50)),
    CAST(l.patio_saida_id AS VARCHAR(50)),
    NULL AS id_patio_devolucao_prevista_origem, -- Ausente no DDL
    CAST(l.patio_chegada_id AS VARCHAR(50)),
    CAST(l.data_retirada AS DATETIME),
    NULL AS data_hora_devolucao_prevista, -- Ausente no DDL
    CAST(l.data_real_devolucao AS DATETIME),
    l.km_saida,
    l.km_chegada,
    NULL AS valor_total_previsto, -- Ausente no DDL
    NULL AS valor_total_final, -- Ausente no DDL
    l.status,
    NULL AS id_seguro_contratado_origem, -- Ausente no DDL
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.Locacao l
WHERE l.data_retirada > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo3');

-- Extração de PROTECAO_ADICIONAL (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem, data_carga)
SELECT
    CAST(pa.id AS VARCHAR(50)),
    pa.nome,
    pa.descricao,
    pa.preco_dia,
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.ProtecaoAdicional pa
WHERE pa.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo3'); -- Assumindo data_ultima_atualizacao

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, status_pagamento, data_pagamento, sistema_origem, data_carga)
SELECT
    CAST(c.id AS VARCHAR(50)),
    CAST(c.locacao_id AS VARCHAR(50)),
    CAST(c.data_cobranca AS DATETIME),
    c.valor_previsto,
    c.valor_final,
    c.metodo_pagamento, -- Usando metodo_pagamento como status (exige transformação posterior)
    NULL AS data_pagamento, -- Ausente no DDL
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.Cobranca c
WHERE c.data_cobranca > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo3');

-- Extração de ESTADO_VEICULO_LOCACAO (de FotoDevolucao do Grupo 3)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, condicao_geral, observacoes, sistema_origem, data_carga)
SELECT
    CAST(fd.id AS VARCHAR(50)),
    CAST(fd.locacao_id AS VARCHAR(50)),
    (SELECT CAST(l.veiculo_id AS VARCHAR(50)) FROM locadora_grupo_3.Locacao l WHERE l.id = fd.locacao_id LIMIT 1) AS id_veiculo_origem,
    (SELECT CAST(l.patio_chegada_id AS VARCHAR(50)) FROM locadora_grupo_3.Locacao l WHERE l.id = fd.locacao_id LIMIT 1) AS id_patio_origem, -- Pátio de chegada da locação
    'Devolucao' AS tipo_registro,
    CAST(fd.data_hora_foto AS DATETIME),
    fd.url AS condicao_geral, -- URL da foto como condição geral
    fd.observacoes,
    'Grupo3' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_3.FotoDevolucao fd
WHERE fd.data_hora_foto > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo3');


-- ---
-- **GRUPO 4** (Assumimos banco de dados 'locadora_grupo_4', esquema 'public')

-- Extração de EMPRESA (Tabela PJ)
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, endereco, sistema_origem, data_carga)
SELECT
    CAST(p.ID_PJ AS VARCHAR(50)),
    p.Nome,
    p.CNPJ,
    p.Endereco,
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_4.public.PJ p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo4'); -- Assumindo data_ultima_atualizacao

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, endereco, nome_patio, sistema_origem, data_carga)
SELECT
    CAST(p.ID_PATIO AS VARCHAR(50)),
    CAST(p.ID_PJ AS VARCHAR(50)),
    p.Endereco,
    NULL AS nome_patio, -- Ausente no DDL
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_4.public.PATIO p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo4'); -- Assumindo data_ultima_atualizacao

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, status_veiculo, tem_ar_condicionado, tem_cadeirinha, sistema_origem, data_carga, id_grupo_veiculo_origem)
SELECT
    CAST(v.ID_VEICULO AS VARCHAR(50)),
    CAST(v.Placa AS VARCHAR(10)),
    CAST(v.Chassi AS VARCHAR(20)),
    v.Marca,
    v.Modelo,
    v.Cor,
    'Desconhecido' AS tipo_mecanizacao, -- Sem mecanização explícita, assumir manual ou inferir
    'Desconhecido' AS status_veiculo, -- Sem status explícito
    v.AC,
    v.Crianca,
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP,
    (SELECT CAST(id_grupo_veiculo AS VARCHAR(50)) FROM locadora_veiculos.GRUPO_VEICULO WHERE nome_grupo = v.Grupo LIMIT 1) -- Mapeamento para o grupo do nosso sistema
FROM locadora_grupo_4.public.VEICULO v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo4'); -- Assumindo data_ultima_atualizacao

-- Extração de CLIENTE (Tabela PF)
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, endereco, telefone, email, sistema_origem, data_carga)
SELECT
    CAST(pf.ID_PF AS VARCHAR(50)),
    'PF',
    pf.Nome,
    pf.CPF,
    pf.Endereco,
    NULL AS telefone, -- Ausente no DDL
    NULL AS email, -- Ausente no DDL
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_4.public.PF pf
WHERE pf.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo4'); -- Assumindo data_ultima_atualizacao

-- Extração de CONDUTOR (derivado de PF do Grupo 4)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(pf.ID_PF AS VARCHAR(50)), -- ID do condutor
    CAST(pf.ID_PF AS VARCHAR(50)), -- Cliente PF associado
    NULL AS id_funcionario_pj_origem,
    pf.Nome,
    pf.CNH,
    pf.Categoria_CNH,
    NULL AS data_expiracao_cnh, -- Ausente o campo de expiração
    pf.Data_Nascimento,
    pf.Nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_4.public.PF pf
WHERE pf.CNH IS NOT NULL AND pf.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo4'); -- Assumindo data_ultima_atualizacao

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.ID_RESERVA AS VARCHAR(50)),
    COALESCE(CAST(r.ID_PF AS VARCHAR(50)), CAST(r.ID_PJ AS VARCHAR(50))), -- Cliente pode ser PF ou PJ
    CAST(r.ID_VEICULO AS VARCHAR(50)), -- Mapeando VEICULO.Grupo para id_grupo_veiculo_origem na fase de transformação. Aqui, puxamos o ID do veículo para depois resolver para o grupo.
    NULL AS id_patio_retirada_previsto_origem, -- Ausente no DDL
    NULL AS id_patio_devolucao_previsto_origem, -- Ausente no DDL
    CAST(r.Data_Inicio AS DATETIME),
    CAST(r.Data_Inicio AS DATETIME),
    CAST(r.Data_Fim AS DATETIME),
    'Confirmada' AS status_reserva, -- Sem status explícito
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_4.public.RESERVA r
WHERE r.Data_Inicio > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo4');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_condutor_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(l.ID_LOCACAO AS VARCHAR(50)),
    CAST(l.ID_RESERVA AS VARCHAR(50)),
    CAST(l.ID_PF AS VARCHAR(50)),
    CAST(ev.ID_VEICULO AS VARCHAR(50)), -- Obter veículo do estado do veículo
    NULL AS id_condutor_origem, -- Ausente na LOCACAO
    NULL AS id_patio_retirada_real_origem, -- Ausente no DDL
    NULL AS id_patio_devolucao_prevista_origem, -- Ausente no DDL
    NULL AS id_patio_devolucao_real_origem, -- Ausente no DDL
    CAST(l.Data_Retirada AS DATETIME),
    NULL AS data_hora_devolucao_prevista, -- Ausente no DDL
    CAST(l.Data_Devolucao AS DATETIME),
    NULL AS quilometragem_retirada, -- Ausente no DDL
    NULL AS quilometragem_devolucao, -- Ausente no DDL
    NULL AS valor_total_previsto, -- Ausente no DDL
    NULL AS valor_total_final, -- Ausente no DDL
    'Desconhecido' AS status_locacao, -- Ausente no DDL
    CAST(l.ID_SEGUROS AS VARCHAR(50)),
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_4.public.LOCACAO l
LEFT JOIN locadora_grupo_4.public.ESTADO_VEICULO ev ON l.ID_ESTADO_VEICULO_Retirada = ev.ID_ESTADO_VEICULO
WHERE l.Data_Retirada > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo4');

-- Extração de SEGUROS (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, sistema_origem, data_carga)
SELECT
    CAST(s.ID_SEGUROS AS VARCHAR(50)),
    CONCAT_WS(' - ', s.Vidros, s.Farois, s.Faixa_Indenizacao),
    CONCAT('Vidros: ', s.Vidros, ', Farois: ', s.Farois, ', Faixa: ', s.Faixa_Indenizacao),
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_4.public.SEGUROS s
WHERE s.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo4'); -- Assumindo data_ultima_atualizacao

-- Extração de ESTADO_VEICULO (para stg_estado_veiculo_locacao do Grupo 4)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, condicao_geral, observacoes, quilometragem_evento, sistema_origem, data_carga)
SELECT
    CAST(ev.ID_ESTADO_VEICULO AS VARCHAR(50)),
    CAST(ev.ID_LOCACAO AS VARCHAR(50)),
    CAST(ev.ID_VEICULO AS VARCHAR(50)),
    NULL AS id_patio_origem, -- Ausente no DDL
    'Revisao' AS tipo_registro, -- Assumindo tipo de evento
    CAST(ev.Data_Revisao AS DATETIME), -- Usando Data_Revisao como data do evento
    CONCAT_WS('; ', ev.Pressao_Pneu, ev.Nivel_Oleo, ev.Gasolina, ev.Motor, ev.Freios, ev.Estado_Pneu, ev.Vidros, ev.Bateria, ev.Estepe, ev.Pintura, ev.Retrovisor, ev.Limpador_Parabrisa) AS condicao_geral,
    ev.Quilometragem AS observacoes, -- Usando Quilometragem como observação
    CAST(REPLACE(ev.Quilometragem, ',', '.') AS DECIMAL(10,2)), -- Convertendo quilometragem para DECIMAL
    'Grupo4' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_4.public.ESTADO_VEICULO ev
WHERE ev.Data_Revisao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo4');


-- ---
-- **GRUPO 5** (Assumimos banco de dados 'locadora_grupo_5')

-- Extração de GRUPOS_VEICULOS
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem, data_carga)
SELECT
    CAST(gv.grupo_id AS VARCHAR(50)),
    gv.nome_grupo,
    gv.descricao,
    gv.valor_diaria_base,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.grupos_veiculos gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo5'); -- Assumindo data_ultima_atualizacao

-- Extração de PATIOS
INSERT INTO stg_patio (id_patio_origem, nome_patio, endereco, sistema_origem, data_carga)
SELECT
    CAST(p.patio_id AS VARCHAR(50)),
    p.nome,
    p.endereco,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.patios p
WHERE p.data_criacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo5');

-- Extração de VEICULOS
INSERT INTO stg_veiculo (id_veiculo_origem, placa, chassi, id_grupo_veiculo_origem, id_patio_atual_origem, marca, modelo, cor, ano_fabricacao, tipo_mecanizacao, status_veiculo, tem_ar_condicionado, sistema_origem, data_carga)
SELECT
    CAST(v.veiculo_id AS VARCHAR(50)),
    CAST(v.placa AS VARCHAR(10)),
    CAST(v.chassi AS VARCHAR(20)),
    CAST(v.grupo_id AS VARCHAR(50)),
    CAST((SELECT va.patio_id FROM locadora_grupo_5.vagas va WHERE va.vaga_id = v.vaga_atual_id LIMIT 1) AS VARCHAR(50)), -- Obter patio_id da vaga
    v.marca,
    v.modelo,
    v.cor,
    v.ano_fabricacao,
    v.mecanizacao,
    v.status,
    v.ar_condicionado,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.veiculos v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo5'); -- Assumindo data_ultima_atualizacao

-- Extração de CLIENTES
INSERT INTO stg_cliente (id_cliente_origem, nome_razao_social, cpf, cnpj, tipo_cliente, email, telefone, endereco, cidade_origem, estado_origem, sistema_origem, data_carga)
SELECT
    CAST(c.cliente_id AS VARCHAR(50)),
    c.nome_completo,
    CASE WHEN c.tipo_pessoa = 'F' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_pessoa = 'J' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_pessoa = 'F' THEN 'PF' ELSE 'PJ' END,
    c.email,
    c.telefone,
    CONCAT_WS(', ', c.endereco_cidade, c.endereco_estado),
    c.endereco_cidade,
    c.endereco_estado,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.clientes c
WHERE c.data_cadastro > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo5');

-- Extração de MOTORISTAS (para stg_condutor)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(m.motorista_id AS VARCHAR(50)),
    CAST(m.cliente_id AS VARCHAR(50)),
    NULL AS id_funcionario_pj_origem,
    m.nome_completo,
    m.cnh,
    m.cnh_categoria,
    m.cnh_validade,
    NULL AS data_nascimento,
    NULL AS nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.motoristas m
WHERE m.cnh_validade > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo5');

-- Extração de RESERVAS
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.reserva_id AS VARCHAR(50)),
    CAST(r.cliente_id AS VARCHAR(50)),
    CAST(r.grupo_id AS VARCHAR(50)),
    CAST(r.patio_retirada_id AS VARCHAR(50)),
    CAST(r.patio_devolucao_id AS VARCHAR(50)),
    CAST(r.data_reserva AS DATETIME),
    CAST(r.data_prevista_retirada AS DATETIME),
    CAST(r.data_prevista_devolucao AS DATETIME),
    r.status_reserva,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.reservas r
WHERE r.data_reserva > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo5');

-- Extração de LOCACOES
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(l.locacao_id AS VARCHAR(50)),
    CAST(l.reserva_id AS VARCHAR(50)),
    CAST(l.cliente_id AS VARCHAR(50)),
    CAST(l.motorista_id AS VARCHAR(50)),
    CAST(l.veiculo_id AS VARCHAR(50)),
    CAST(l.patio_retirada_id AS VARCHAR(50)),
    CAST(l.data_devolucao_prevista AS VARCHAR(50)), -- Using prev_dev_date as prev_dev_patio to fill the field in staging
    CAST(l.patio_devolucao_id AS VARCHAR(50)),
    CAST(l.data_retirada_real AS DATETIME),
    CAST(l.data_devolucao_prevista AS DATETIME),
    CAST(l.data_devolucao_real AS DATETIME),
    l.km_saida, -- Assuming km_saida exists
    l.km_chegada, -- Assuming km_chegada exists
    l.valor_total_previsto,
    l.valor_total_final,
    l.status,
    NULL AS id_seguro_contratado_origem, -- 'protecoes_adicionais' (TEXT) from DDL needs to be processed
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.locacoes l
WHERE l.data_retirada_real > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo5');

-- Extração de COBRANCAS
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, valor_base, valor_final_cobranca, data_cobranca, data_vencimento, data_pagamento, status_pagamento, sistema_origem, data_carga)
SELECT
    CAST(c.cobranca_id AS VARCHAR(50)),
    CAST(c.locacao_id AS VARCHAR(50)),
    c.valor,
    c.valor,
    CAST(c.data_emissao AS DATETIME),
    c.data_vencimento,
    CAST(c.data_pagamento AS DATETIME),
    c.status_pagamento,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.cobrancas c
WHERE c.data_emissao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo5');

-- Extração de FOTOS_VEICULOS (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, condicao_geral, observacoes, quilometragem_evento, sistema_origem, data_carga)
SELECT
    CAST(fv.foto_id AS VARCHAR(50)),
    CAST((SELECT locacao_id FROM locadora_grupo_5.locacoes WHERE veiculo_id = fv.veiculo_id AND data_devolucao_real IS NULL LIMIT 1) AS VARCHAR(50)), -- Tentar inferir locação ativa
    CAST(fv.veiculo_id AS VARCHAR(50)),
    (SELECT CAST(v.patio_id FROM locadora_grupo_5.vagas va JOIN locadora_grupo_5.veiculos v_orig ON va.vaga_id = v_orig.vaga_atual_id WHERE v_orig.veiculo_id = fv.veiculo_id LIMIT 1) AS VARCHAR(50)) AS id_patio_origem, -- Get current patio
    fv.tipo,
    CAST(fv.data_upload AS DATETIME),
    fv.url_foto,
    NULL AS observacoes,
    NULL AS quilometragem_evento,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.fotos_veiculos fv
WHERE fv.data_upload > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo5');

-- Extração de PRONTUARIOS_VEICULOS (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, condicao_geral, observacoes, quilometragem_evento, sistema_origem, data_carga)
SELECT
    CAST(pv.prontuario_id AS VARCHAR(50)),
    CAST((SELECT locacao_id FROM locadora_grupo_5.locacoes WHERE veiculo_id = pv.veiculo_id AND data_devolucao_real IS NULL LIMIT 1) AS VARCHAR(50)), -- Inferir locação ativa
    CAST(pv.veiculo_id AS VARCHAR(50)),
    (SELECT CAST(v.patio_id FROM locadora_grupo_5.vagas va JOIN locadora_grupo_5.veiculos v_orig ON va.vaga_id = v_orig.vaga_atual_id WHERE v_orig.veiculo_id = pv.veiculo_id LIMIT 1) AS VARCHAR(50)) AS id_patio_origem, -- Get current patio
    pv.tipo_evento,
    CAST(pv.data_ocorrencia AS DATETIME),
    pv.descricao,
    NULL AS observacoes,
    pv.quilometragem,
    'Grupo5' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_5.prontuarios_veiculos pv
WHERE pv.data_ocorrencia > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo5');


-- ---
-- **GRUPO 6** (Assumimos banco de dados 'locadora_grupo_6')

-- Extração de CLIENTE
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, telefone, email, sistema_origem, data_carga)
SELECT
    CAST(c.cliente_id AS VARCHAR(50)),
    c.tipo,
    c.nome_razao,
    CASE WHEN c.tipo = 'F' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo = 'J' THEN c.cpf_cnpj ELSE NULL END,
    c.telefone,
    c.email,
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.CLIENTE c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo6'); -- Assumindo data_ultima_atualizacao

-- Extração de CONDUTOR
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(c.condutor_id AS VARCHAR(50)),
    CAST(c.cliente_id AS VARCHAR(50)),
    NULL AS id_funcionario_pj_origem,
    c.nome,
    c.cnh_numero,
    c.cnh_categoria,
    c.cnh_validade,
    NULL AS data_nascimento, -- Ausente no DDL
    NULL AS nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.CONDUTOR c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo6'); -- Assumindo data_ultima_atualizacao

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem, data_carga)
SELECT
    CAST(gv.grupo_id AS VARCHAR(50)),
    gv.nome,
    NULL AS descricao, -- Ausente no DDL
    gv.tarifa_diaria,
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.GRUPO_VEICULO gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo6'); -- Assumindo data_ultima_atualizacao

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, tem_ar_condicionado, tem_cadeirinha, status_veiculo, sistema_origem, data_carga)
SELECT
    CAST(v.veiculo_id AS VARCHAR(50)),
    CAST(v.grupo_id AS VARCHAR(50)),
    CAST(v.placa AS VARCHAR(10)),
    CAST(v.chassis AS VARCHAR(20)),
    v.marca,
    v.modelo,
    v.cor,
    v.mecanizacao,
    v.ar_condicionado,
    v.cadeirinha,
    NULL AS status_veiculo, -- Ausente no DDL
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.VEICULO v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo6'); -- Assumindo data_ultima_atualizacao

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, nome_patio, endereco, sistema_origem, data_carga)
SELECT
    CAST(p.patio_id AS VARCHAR(50)),
    p.nome,
    p.localizacao,
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.PATIO p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo6'); -- Assumindo data_ultima_atualizacao

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.reserva_id AS VARCHAR(50)),
    CAST(r.cliente_id AS VARCHAR(50)),
    CAST(r.grupo_id AS VARCHAR(50)),
    CAST(r.patio_retirada_id AS VARCHAR(50)),
    NULL AS id_patio_devolucao_previsto_origem, -- Ausente no DDL
    CAST(r.data_inicio AS DATETIME),
    CAST(r.data_inicio AS DATETIME),
    CAST(r.data_fim_previsto AS DATETIME),
    r.status,
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.RESERVA r
WHERE r.data_inicio > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo6');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_condutor_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(l.locacao_id AS VARCHAR(50)),
    CAST(l.reserva_id AS VARCHAR(50)),
    NULL AS id_cliente_origem, -- Ausente no DDL
    CAST(l.veiculo_id AS VARCHAR(50)),
    CAST(l.condutor_id AS VARCHAR(50)),
    CAST(l.patio_saida_id AS VARCHAR(50)),
    CAST(l.data_devolucao_prevista AS VARCHAR(50)), -- Using datetime as patio id, will be NULL in transformation
    CAST(l.patio_chegada_id AS VARCHAR(50)),
    CAST(l.data_retirada AS DATETIME),
    CAST(l.data_devolucao_prevista AS DATETIME),
    CAST(l.data_devolucao_real AS DATETIME),
    NULL AS quilometragem_retirada, -- Ausente no DDL
    NULL AS quilometragem_devolucao, -- Ausente no DDL
    NULL AS valor_total_previsto, -- Ausente no DDL
    NULL AS valor_total_final, -- Ausente no DDL
    'Desconhecido' AS status_locacao, -- Ausente no DDL
    NULL AS id_seguro_contratado_origem,
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.LOCACAO l
WHERE l.data_retirada > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo6');

-- Extração de PROTECAO_ADICIONAL (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem, data_carga)
SELECT
    CAST(pa.protecao_id AS VARCHAR(50)),
    pa.descricao,
    pa.descricao,
    NULL AS valor_diario, -- Ausente no DDL
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.PROTECAO_ADICIONAL pa
WHERE pa.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo6'); -- Assumindo data_ultima_atualizacao

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, status_pagamento, data_vencimento, data_pagamento, sistema_origem, data_carga)
SELECT
    CAST(c.cobranca_id AS VARCHAR(50)),
    CAST(c.locacao_id AS VARCHAR(50)),
    CAST(c.data_cobranca AS DATETIME),
    c.valor_base,
    c.valor_final,
    c.status_pagamento,
    NULL AS data_vencimento, -- Ausente no DDL
    NULL AS data_pagamento, -- Ausente no DDL
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.COBRANCA c
WHERE c.data_cobranca > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo6');

-- Extração de ESTADO_VEICULO_LOCACAO (de PRONTUARIO e FOTO_VEICULO do Grupo 6)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, condicao_geral, observacoes, quilometragem_evento, sistema_origem, data_carga)
SELECT
    CAST(p.prontuario_id AS VARCHAR(50)),
    CAST((SELECT locacao_id FROM locadora_grupo_6.LOCACAO WHERE veiculo_id = p.veiculo_id AND data_devolucao_real IS NULL LIMIT 1) AS VARCHAR(50)), -- Inferir locação ativa
    CAST(p.veiculo_id AS VARCHAR(50)),
    NULL AS id_patio_origem, -- Ausente no DDL
    'Prontuario' AS tipo_registro,
    CAST(p.data_registro AS DATETIME),
    p.descricao,
    NULL AS observacoes,
    NULL AS quilometragem_evento, -- Ausente no DDL
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.PRONTUARIO p
WHERE p.data_registro > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo6')
UNION ALL
SELECT
    CAST(fv.foto_id AS VARCHAR(50)),
    CAST((SELECT locacao_id FROM locadora_grupo_6.LOCACAO WHERE veiculo_id = fv.veiculo_id AND data_devolucao_real IS NULL LIMIT 1) AS VARCHAR(50)), -- Inferir locação ativa
    CAST(fv.veiculo_id AS VARCHAR(50)),
    NULL AS id_patio_origem, -- Ausente no DDL
    fv.tipo,
    CAST(fv.data_foto AS DATETIME),
    fv.url,
    NULL AS observacoes,
    NULL AS quilometragem_evento, -- Ausente no DDL
    'Grupo6' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_6.FOTO_VEICULO fv
WHERE fv.data_foto > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo6');


-- ---
-- **GRUPO 7** (Assumimos banco de dados 'locadora_grupo_7')

-- Extração de CLIENTES
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, cidade_origem, estado_origem, sistema_origem, data_carga)
SELECT
    CAST(c.cliente_id AS VARCHAR(50)),
    c.tipo_pessoa,
    c.nome_completo,
    CASE WHEN c.tipo_pessoa = 'F' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_pessoa = 'J' THEN c.cpf_cnpj ELSE NULL END,
    CONCAT_WS(', ', c.endereco_cidade, c.endereco_estado),
    c.telefone,
    c.email,
    c.endereco_cidade,
    c.endereco_estado,
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.clientes c
WHERE c.data_cadastro > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo7');

-- Extração de MOTORISTAS (para stg_condutor)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(m.motorista_id AS VARCHAR(50)),
    CAST(m.cliente_id AS VARCHAR(50)),
    NULL AS id_funcionario_pj_origem,
    m.nome_completo,
    m.cnh,
    m.cnh_categoria,
    m.cnh_validade,
    NULL AS data_nascimento, -- Ausente no DDL
    NULL AS nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.motoristas m
WHERE m.cnh_validade > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo7');

-- Extração de PATIOS
INSERT INTO stg_patio (id_patio_origem, nome_patio, endereco, sistema_origem, data_carga)
SELECT
    CAST(p.patio_id AS VARCHAR(50)),
    p.nome,
    p.endereco,
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.patios p
WHERE p.criado_em > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo7');

-- Extração de GRUPOS_VEICULOS
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem, data_carga)
SELECT
    CAST(gv.grupo_id AS VARCHAR(50)),
    gv.nome_grupo,
    gv.descricao_grupo,
    gv.tarifa_diaria_base,
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.grupos_veiculos gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo7'); -- Assumindo data_ultima_atualizacao

-- Extração de VEICULOS
INSERT INTO stg_veiculo (id_veiculo_origem, placa, chassi, id_grupo_veiculo_origem, id_patio_atual_origem, marca, modelo, cor, ano_fabricacao, tipo_mecanizacao, tem_ar_condicionado, status_veiculo, sistema_origem, data_carga)
SELECT
    CAST(v.veiculo_id AS VARCHAR(50)),
    CAST(v.placa AS VARCHAR(10)),
    CAST(v.chassi AS VARCHAR(20)),
    CAST(v.grupo_id AS VARCHAR(50)),
    CAST((SELECT va.patio_id FROM locadora_grupo_7.vagas va WHERE va.vaga_id = v.vaga_atual_id LIMIT 1) AS VARCHAR(50)),
    v.marca,
    v.modelo,
    v.cor,
    v.ano_fabricacao,
    v.cambio,
    v.possui_ar_cond,
    v.situacao,
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.veiculos v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo7'); -- Assumindo data_ultima_atualizacao

-- Extração de RESERVAS
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.reserva_id AS VARCHAR(50)),
    CAST(r.cliente_id AS VARCHAR(50)),
    CAST(r.grupo_id AS VARCHAR(50)),
    CAST(r.patio_retirada_id AS VARCHAR(50)),
    CAST(r.patio_devolucao_id AS VARCHAR(50)),
    CAST(r.criado_em AS DATETIME),
    CAST(r.retirada_prevista AS DATETIME),
    CAST(r.devolucao_prevista AS DATETIME),
    r.situacao_reserva,
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.reservas r
WHERE r.criado_em > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo7');

-- Extração de LOCACOES
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(l.locacao_id AS VARCHAR(50)),
    CAST(l.reserva_id AS VARCHAR(50)),
    CAST(l.cliente_id AS VARCHAR(50)),
    CAST(l.motorista_id AS VARCHAR(50)),
    CAST(l.veiculo_id AS VARCHAR(50)),
    CAST(l.patio_retirada_id AS VARCHAR(50)),
    CAST(l.devolucao_prevista AS VARCHAR(50)), -- Using prev_dev_date as prev_dev_patio to fill the field in staging
    CAST(l.patio_devolucao_id AS VARCHAR(50)),
    CAST(l.retirada_real AS DATETIME),
    CAST(l.devolucao_prevista AS DATETIME),
    CAST(l.devolucao_real AS DATETIME),
    l.valor_previsto,
    l.valor_final,
    'Desconhecido' AS status_locacao, -- Ausente no DDL
    NULL AS id_seguro_contratado_origem, -- 'protecoes_extras' (TEXT) do DDL precisa ser processado
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.locacoes l
WHERE l.retirada_real > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo7');

-- Extração de COBRANCAS
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, valor_base, valor_final_cobranca, data_cobranca, data_vencimento, data_pagamento, status_pagamento, sistema_origem, data_carga)
SELECT
    CAST(c.cobranca_id AS VARCHAR(50)),
    CAST(c.locacao_id AS VARCHAR(50)),
    c.valor,
    c.valor,
    CAST(c.emitida_em AS DATETIME),
    c.vencimento,
    CAST(c.pago_em AS DATETIME),
    c.status_pago,
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.cobrancas c
WHERE c.emitida_em > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo7');

-- Extração de ESTADO_VEICULO_LOCACAO (de PRONTUARIOS_VEICULOS e FOTOS_VEICULOS do Grupo 7)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, condicao_geral, observacoes, quilometragem_evento, sistema_origem, data_carga)
SELECT
    CAST(pv.prontuario_id AS VARCHAR(50)),
    CAST((SELECT locacao_id FROM locadora_grupo_7.locacoes WHERE veiculo_id = pv.veiculo_id AND devolucao_real IS NULL LIMIT 1) AS VARCHAR(50)), -- Inferir locação ativa
    CAST(pv.veiculo_id AS VARCHAR(50)),
    (SELECT CAST(v.patio_id FROM locadora_grupo_7.vagas va JOIN locadora_grupo_7.veiculos v_orig ON va.vaga_id = v_orig.vaga_atual_id WHERE v_orig.veiculo_id = pv.veiculo_id LIMIT 1) AS VARCHAR(50)) AS id_patio_origem, -- Get current patio
    pv.tipo_evento,
    CAST(pv.data_evento AS DATETIME),
    pv.detalhes,
    NULL AS observacoes,
    pv.custo_evento, -- Usando custo_evento como quilometragem
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.prontuarios_veiculos pv
WHERE pv.data_evento > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo7')
UNION ALL
SELECT
    CAST(fv.foto_id AS VARCHAR(50)),
    CAST((SELECT locacao_id FROM locadora_grupo_7.locacoes WHERE veiculo_id = fv.veiculo_id AND devolucao_real IS NULL LIMIT 1) AS VARCHAR(50)), -- Inferir locação ativa
    CAST(fv.veiculo_id AS VARCHAR(50)),
    (SELECT CAST(v.patio_id FROM locadora_grupo_7.vagas va JOIN locadora_grupo_7.veiculos v_orig ON va.vaga_id = v_orig.vaga_atual_id WHERE v_orig.veiculo_id = fv.veiculo_id LIMIT 1) AS VARCHAR(50)) AS id_patio_origem, -- Get current patio
    fv.finalidade,
    CAST(fv.enviado_em AS DATETIME),
    fv.caminho_imagem,
    NULL AS observacoes,
    NULL AS quilometragem_evento,
    'Grupo7' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_7.fotos_veiculos fv
WHERE fv.enviado_em > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo7');


-- ---
-- **GRUPO 8** (Assumimos banco de dados 'locadora_grupo_8', esquema 'mydb')

-- Extração de EMPRESA
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, sistema_origem, data_carga)
SELECT
    CAST(e.ID_EMPRESA AS VARCHAR(50)),
    e.NOME_EMPRESA,
    CAST(e.CNPJ_EMPRESA AS VARCHAR(18)),
    NULL AS endereco, -- Ausente no DDL
    NULL AS telefone, -- Ausente no DDL
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.Empresa e
WHERE e.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo8'); -- Assumindo data_ultima_atualizacao

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, capacidade_vagas, sistema_origem, data_carga)
SELECT
    CAST(p.ID_PATIO AS VARCHAR(50)),
    CAST(p.FK_ID_EMPRESA AS VARCHAR(50)),
    p.NOME_PATIO,
    CONCAT_WS(', ', e.LOGRADOURO, e.NUMERO_LOGRADOURO, b.NOME_BAIRRO, c.NOME_CIDADE, s.NOME_ESTADO, CONCAT('CEP: ', e.CEP)),
    NULL AS capacidade_vagas, -- Ausente no DDL
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.Patio p
JOIN locadora_grupo_8.mydb.Endereco e ON p.FK_ID_ENDERECO = e.ID_ENDERECO
JOIN locadora_grupo_8.mydb.Bairro b ON e.FK_ID_BAIRRO = b.ID_BAIRRO
JOIN locadora_grupo_8.mydb.Cidade c ON b.FK_ID_CIDADE = c.ID_CIDADE
JOIN locadora_grupo_8.mydb.Estado s ON c.FK_SIGLA_ESTADO = s.SIGLA_ESTADO
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo8'); -- Assumindo data_ultima_atualizacao

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem, data_carga)
SELECT
    CAST(gv.ID_GRUPO_VEICULO AS VARCHAR(50)),
    gv.NOME_GRUPO,
    gv.DESCRICAO_GRUPO,
    gv.VALOR_DIARIA_BASE_GRUPO,
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.GrupoVeiculo gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo8'); -- Assumindo data_ultima_atualizacao

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, id_patio_atual_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, tem_ar_condicionado, tem_cadeirinha, status_veiculo, sistema_origem, data_carga)
SELECT
    CAST(v.ID_VEICULO AS VARCHAR(50)),
    CAST(v.FK_ID_GRUPO_VEICULO AS VARCHAR(50)),
    CAST(v.FK_ID_PATIO AS VARCHAR(50)),
    CAST(v.PLACA AS VARCHAR(10)),
    CAST(v.CHASSI AS VARCHAR(20)),
    m.NOME_MARCA,
    mo.NOME_MODELO,
    v.COR_VEICULO,
    v.TIPO_MECANIZACAO,
    v.POSSUI_AR_CONDICIONADO,
    v.POSSUI_CADEIRINHA_CRIANCA,
    v.STATUS_VEICULO,
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.Veiculo v
JOIN locadora_grupo_8.mydb.ModeloVeiculo mo ON v.FK_ID_MODELO_VEICULO = mo.ID_MODELO_VEICULO
JOIN locadora_grupo_8.mydb.MarcaVeiculo m ON mo.FK_ID_MARCA_VEICULO = m.ID_MARCA_VEICULO
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo8'); -- Assumindo data_ultima_atualizacao

-- Extração de CLIENTE
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem, data_carga)
SELECT
    CAST(c.ID_CLIENTE AS VARCHAR(50)),
    c.TIPO_PESSOA,
    c.NOME_CLIENTE,
    CASE WHEN c.TIPO_PESSOA = 'F' THEN c.DOCUMENTO ELSE NULL END,
    CASE WHEN c.TIPO_PESSOA = 'J' THEN c.DOCUMENTO ELSE NULL END,
    CONCAT_WS(', ', e.LOGRADOURO, e.NUMERO_LOGRADOURO, b.NOME_BAIRRO, ci.NOME_CIDADE, st.NOME_ESTADO, CONCAT('CEP: ', e.CEP)),
    c.TELEFONE,
    c.EMAIL,
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.Cliente c
JOIN locadora_grupo_8.mydb.Endereco e ON c.FK_ID_ENDERECO = e.ID_ENDERECO
JOIN locadora_grupo_8.mydb.Bairro b ON e.FK_ID_BAIRRO = b.ID_BAIRRO
JOIN locadora_grupo_8.mydb.Cidade ci ON b.FK_ID_CIDADE = ci.ID_CIDADE
JOIN locadora_grupo_8.mydb.Estado st ON ci.FK_SIGLA_ESTADO = st.SIGLA_ESTADO
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo8'); -- Assumindo data_ultima_atualizacao

-- Extração de CONDUTOR
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(co.ID_CONDUTOR AS VARCHAR(50)),
    CAST(co.FK_ID_CLIENTE AS VARCHAR(50)),
    NULL AS id_funcionario_pj_origem,
    co.NOME_CONDUTOR,
    co.NUMERO_CNH,
    co.CATEGORIA_CNH,
    co.DATA_VALIDADE_CNH,
    NULL AS data_nascimento, -- Ausente no DDL
    NULL AS nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.Condutor co
WHERE co.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo8'); -- Assumindo data_ultima_atualizacao

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(l.ID_LOCACAO AS VARCHAR(50)),
    CAST(l.FK_ID_CLIENTE AS VARCHAR(50)),
    CAST(l.FK_ID_CONDUTOR AS VARCHAR(50)),
    CAST(l.FK_ID_VEICULO AS VARCHAR(50)),
    CAST(l.FK_ID_PATIO_RETIRADA AS VARCHAR(50)),
    CAST(l.FK_ID_PATIO_DEVOLUCAO AS VARCHAR(50)), -- Dev Prevista é igual à Real aqui para preencher o campo
    CAST(l.FK_ID_PATIO_DEVOLUCAO AS VARCHAR(50)), -- Dev Real é igual à Dev Prevista
    CAST(l.DATA_HORA_RETIRADA_REALIZADA AS DATETIME),
    CAST(l.DATA_HORA_DEVOLUCAO_PREVISTA AS DATETIME),
    CAST(l.DATA_HORA_DEVOLUCAO_REALIZADA AS DATETIME),
    NULL AS quilometragem_retirada, -- Ausente no DDL
    NULL AS quilometragem_devolucao, -- Ausente no DDL
    l.VALOR_DIARIA_CONTRATADA,
    NULL AS valor_total_final, -- Ausente no DDL
    l.STATUS_LOCACAO,
    NULL AS id_seguro_contratado_origem, -- Não há campo direto, precisaria de join com LocacaoProtecao
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.Locacao l
WHERE l.DATA_HORA_RETIRADA_REALIZADA > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo8');

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, data_pagamento, status_pagamento, sistema_origem, data_carga)
SELECT
    CAST(c.ID_COBRANCA AS VARCHAR(50)),
    CAST(c.FK_ID_LOCACAO AS VARCHAR(50)),
    CAST(c.DATA_HORA_EMISSAO AS DATETIME),
    c.VALOR_COBRANCA,
    c.VALOR_COBRANCA,
    CAST(c.DATA_HORA_PAGAMENTO AS DATETIME),
    c.STATUS_COBRANCA,
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.Cobranca c
WHERE c.DATA_HORA_EMISSAO > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo8');

-- Extração de PROTECAO_ADICIONAL (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem, data_carga)
SELECT
    CAST(pa.ID_PROTECAO_ADICIONAL AS VARCHAR(50)),
    pa.NOME_PROTECAO,
    pa.DESCRICAO_PROTECAO,
    pa.VALOR_DIARIO_PROTECAO,
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.ProtecaoAdicional pa
WHERE pa.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo8'); -- Assumindo data_ultima_atualizacao

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.ID_RESERVA AS VARCHAR(50)),
    CAST(r.FK_ID_CLIENTE AS VARCHAR(50)),
    CAST(r.FK_ID_GRUPO_VEICULO AS VARCHAR(50)),
    CAST(r.FK_ID_PATIO_RETIRADA AS VARCHAR(50)),
    CAST(r.FK_ID_PATIO_DEVOLUCAO AS VARCHAR(50)),
    CAST(r.DATA_HORA_SOLICITACAO_RESERVA AS DATETIME),
    CAST(r.DATA_HORA_PREVISTA_RETIRADA AS DATETIME),
    CAST(r.DATA_HORA_PREVISTA_DEVOLUCAO AS DATETIME),
    r.STATUS_RESERVA,
    'Grupo8' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_8.mydb.Reserva r
WHERE r.DATA_HORA_SOLICITACAO_RESERVA > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo8');


-- ---
-- **GRUPO 9** (Assumimos banco de dados 'locadora_grupo_9')

-- Extração de EMPRESA (locadora)
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, sistema_origem, data_carga)
SELECT
    CAST(l.id_locadora AS VARCHAR(50)),
    l.nome_locadora,
    CAST(l.cnpj AS VARCHAR(18)),
    NULL AS endereco, -- Ausente no DDL
    NULL AS telefone, -- Ausente no DDL
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.locadora l
WHERE l.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo9'); -- Assumindo data_ultima_atualizacao

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, sistema_origem, data_carga)
SELECT
    CAST(p.id_patio AS VARCHAR(50)),
    CAST(p.id_locadora AS VARCHAR(50)),
    p.nome_patio,
    p.endereco_patio,
    NULL AS capacidade_vagas, -- Ausente no DDL
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.patio p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo9'); -- Assumindo data_ultima_atualizacao

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem, data_carga)
SELECT
    CAST(gv.id_grupo_veiculo AS VARCHAR(50)),
    gv.nome_grupo,
    NULL AS descricao, -- Ausente no DDL
    gv.faixa_valor,
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.grupo_veiculo gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo9'); -- Assumindo data_ultima_atualizacao

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, id_patio_atual_origem, placa, chassi, marca, modelo, cor, ano_fabricacao, tipo_mecanizacao, tem_ar_condicionado, status_veiculo, sistema_origem, data_carga)
SELECT
    CAST(v.id_veiculo AS VARCHAR(50)),
    CAST(v.id_grupo_veiculo AS VARCHAR(50)),
    CAST((SELECT va.patio_id FROM locadora_grupo_9.vaga va WHERE va.id_vaga = v.id_vaga_atual LIMIT 1) AS VARCHAR(50)),
    CAST(v.placa AS VARCHAR(10)),
    CAST(v.chassi AS VARCHAR(20)),
    v.marca,
    NULL AS modelo, -- Ausente no DDL
    v.cor,
    CASE WHEN v.mecanizacao THEN 'Automática' ELSE 'Manual' END,
    v.ar_condicionado,
    v.status_veiculo,
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.veiculo v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo9'); -- Assumindo data_ultima_atualizacao

-- Extração de CLIENTE (incluindo PF e PJ)
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem, data_carga)
SELECT
    CAST(c.id_cliente AS VARCHAR(50)),
    c.tipo_cliente,
    COALESCE(pf.nome_completo, pj.nome_empresa),
    pf.cpf,
    pj.cnpj,
    NULL AS endereco, -- Ausente no DDL
    c.telefone_principal,
    c.email,
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.cliente c
LEFT JOIN locadora_grupo_9.pessoa_fisica pf ON c.id_cliente = pf.id_cliente
LEFT JOIN locadora_grupo_9.pessoa_juridica pj ON c.id_cliente = pj.id_cliente
WHERE c.data_cadastro > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo9');

-- Extração de CONDUTOR (motorista + cnh)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(m.id_motorista AS VARCHAR(50)),
    CAST(m.id_pessoa_fisica AS VARCHAR(50)), -- id_cliente da pessoa física associada
    NULL AS id_funcionario_pj_origem,
    pf.nome_completo,
    cnh.numero_cnh,
    cnh.categoria_cnh,
    cnh.data_validade,
    NULL AS data_nascimento, -- Ausente no DDL
    NULL AS nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.motorista m
JOIN locadora_grupo_9.pessoa_fisica pf ON m.id_pessoa_fisica = pf.id_cliente
JOIN locadora_grupo_9.cnh cnh ON m.id_motorista = cnh.id_motorista
WHERE pf.data_nascimento > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo9'); -- Usando data de nascimento da PF

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.id_reserva AS VARCHAR(50)),
    NULL AS id_cliente_origem, -- Ausente no DDL de Reserva
    NULL AS id_grupo_veiculo_origem, -- Ausente no DDL de Reserva
    NULL AS id_patio_retirada_previsto_origem, -- Ausente no DDL de Reserva
    NULL AS id_patio_devolucao_previsto_origem, -- Ausente no DDL de Reserva
    CAST(r.data_hora_reserva_inicio AS DATETIME),
    CAST(r.data_hora_retirada_fim AS DATETIME), -- Usando retirada_fim como data_retirada_prevista
    NULL AS data_hora_devolucao_prevista, -- Ausente no DDL
    'Desconhecido' AS status_reserva, -- Ausente no DDL
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.reserva r
WHERE r.data_hora_reserva_inicio > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo9');

-- Extração de LOCACAO (contrato)
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_condutor_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(c.id_contrato AS VARCHAR(50)),
    CAST(c.id_reserva AS VARCHAR(50)),
    CAST(c.id_cliente AS VARCHAR(50)),
    CAST(c.id_veiculo AS VARCHAR(50)),
    CAST(c.id_motorista AS VARCHAR(50)),
    CAST(c.id_patio_retirada AS VARCHAR(50)),
    NULL AS id_patio_devolucao_prevista_origem, -- Ausente no DDL
    CAST(c.id_patio_devolucao_efetiva AS VARCHAR(50)),
    CAST(c.data_hora_contrato AS DATETIME),
    NULL AS data_hora_devolucao_prevista, -- Ausente no DDL
    NULL AS data_hora_devolucao_real, -- Ausente no DDL
    NULL AS quilometragem_retirada, -- Ausente no DDL
    NULL AS quilometragem_devolucao, -- Ausente no DDL
    NULL AS valor_total_previsto, -- Ausente no DDL
    NULL AS valor_total_final, -- Ausente no DDL
    c.status_locacao,
    NULL AS id_seguro_contratado_origem, -- Ausente no DDL
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.contrato c
WHERE c.data_hora_contrato > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo9');

-- Extração de PROTECAO_ADICIONAL (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem, data_carga)
SELECT
    CAST(pa.id_protecao_adicional AS VARCHAR(50)),
    pa.nome_protecao,
    pa.nome_protecao,
    pa.valor_cobrado, -- Assumindo valor cobrado é valor diário
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.protecao_adicional pa
WHERE pa.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo9'); -- Assumindo data_ultima_atualizacao

-- Extração de COBRANCA (fatura)
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, status_pagamento, data_vencimento, data_pagamento, sistema_origem, data_carga)
SELECT
    CAST(c.id_fatura AS VARCHAR(50)),
    CAST(c.id_contrato AS VARCHAR(50)),
    CAST(c.data_emissao AS DATETIME),
    c.valor,
    c.valor,
    c.status_fatura,
    NULL AS data_vencimento, -- Ausente no DDL
    NULL AS data_pagamento, -- Ausente no DDL
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.cobranca c
WHERE c.data_emissao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo9');

-- Extração de ESTADO_VEICULO_LOCACAO (de PRONTUARIO e FOTO_VEICULO do Grupo 9)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, condicao_geral, observacoes, quilometragem_evento, sistema_origem, data_carga)
SELECT
    CAST(p.id_registro_manutencao AS VARCHAR(50)),
    CAST((SELECT id_contrato FROM locadora_grupo_9.contrato WHERE id_veiculo = p.id_veiculo AND id_patio_devolucao_efetiva IS NULL LIMIT 1) AS VARCHAR(50)), -- Inferir locação ativa
    CAST(p.id_veiculo AS VARCHAR(50)),
    NULL AS id_patio_origem, -- Ausente no DDL
    'Manutencao' AS tipo_registro,
    CAST(p.data_ultima_manutencao AS DATETIME),
    p.estado_conservacao, -- Usando estado_conservacao como condicao_geral
    p.caracteristica_rodagem, -- Usando caracteristica_rodagem como observacoes
    NULL AS quilometragem_evento, -- Ausente no DDL
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.prontuario p
WHERE p.data_ultima_manutencao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo9')
UNION ALL
SELECT
    CAST(fv.id_foto_veiculo AS VARCHAR(50)),
    CAST((SELECT id_contrato FROM locadora_grupo_9.contrato WHERE id_veiculo = fv.id_veiculo AND id_patio_devolucao_efetiva IS NULL LIMIT 1) AS VARCHAR(50)), -- Inferir locação ativa
    CAST(fv.id_veiculo AS VARCHAR(50)),
    NULL AS id_patio_origem, -- Ausente no DDL
    fv.tipo_foto,
    CAST(fv.data_foto AS DATETIME),
    fv.url,
    NULL AS observacoes,
    NULL AS quilometragem_evento, -- Ausente no DDL
    'Grupo9' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_9.foto_veiculo fv
WHERE fv.data_foto > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo9');


-- ---
-- **GRUPO 10** (Assumimos banco de dados 'locadora_grupo_10')

-- Extração de Empresa
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, sistema_origem, data_carga)
SELECT
    CAST(e.id_empresa AS VARCHAR(50)),
    e.nome_fantasia,
    CAST(e.cnpj AS VARCHAR(18)),
    NULL AS endereco, -- Ausente no DDL
    NULL AS telefone, -- Ausente no DDL
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.Empresa e
WHERE e.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo10'); -- Assumindo data_ultima_atualizacao

-- Extração de Patio
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, sistema_origem, data_carga)
SELECT
    CAST(p.id_patio AS VARCHAR(50)),
    CAST(p.id_empresa_proprietaria AS VARCHAR(50)),
    p.nome,
    CONCAT_WS(', ', p.endereco, p.cidade, p.estado_origem),
    NULL AS capacidade_vagas, -- Ausente no DDL
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.Patio p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo10'); -- Assumindo data_ultima_atualizacao

-- Extração de GrupoVeiculo
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem, data_carga)
SELECT
    CAST(gv.id_grupo AS VARCHAR(50)),
    gv.nome,
    gv.descricao,
    gv.valor_diaria_base,
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.GrupoVeiculo gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo10'); -- Assumindo data_ultima_atualizacao

-- Extração de Veiculo
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, id_patio_atual_origem, placa, chassi, marca, modelo, cor, ano_fabricacao, tipo_mecanizacao, tem_ar_condicionado, status_veiculo, sistema_origem, data_carga)
SELECT
    CAST(v.id_veiculo AS VARCHAR(50)),
    CAST(v.id_grupo AS VARCHAR(50)),
    CAST((SELECT va.id_patio FROM locadora_grupo_10.Vaga va WHERE va.id_vaga = v.id_vaga_atual LIMIT 1) AS VARCHAR(50)),
    CAST(v.placa AS VARCHAR(10)),
    CAST(v.chassi AS VARCHAR(20)),
    (SELECT m.nome FROM locadora_grupo_10.Marca m JOIN locadora_grupo_10.Modelo mo ON m.id_marca = mo.id_marca WHERE mo.id_modelo = v.id_modelo LIMIT 1),
    (SELECT mo.nome FROM locadora_grupo_10.Modelo mo WHERE mo.id_modelo = v.id_modelo LIMIT 1),
    v.cor,
    v.ano_fabricacao,
    v.mecanizacao,
    v.tem_ar_condicionado,
    v.status_operacional,
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.Veiculo v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo10'); -- Assumindo data_ultima_atualizacao

-- Extração de Cliente
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, email, telefone, endereco, cidade_origem, estado_origem, sistema_origem, data_carga)
SELECT
    CAST(c.id_cliente AS VARCHAR(50)),
    c.tipo_cliente,
    c.nome_razao_social,
    CASE WHEN c.tipo_cliente = 'PF' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_cliente = 'PJ' THEN c.cpf_cnpj ELSE NULL END,
    c.email,
    c.telefone,
    c.endereco_cobranca,
    c.cidade_origem,
    c.estado_origem,
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.Cliente c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo10'); -- Assumindo data_ultima_atualizacao

-- Extração de Motorista (para stg_condutor)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, id_funcionario_pj_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada, sistema_origem, data_carga)
SELECT
    CAST(m.id_motorista AS VARCHAR(50)),
    CAST(m.id_cliente_associado AS VARCHAR(50)),
    NULL AS id_funcionario_pj_origem,
    m.nome_completo,
    m.cnh_numero,
    m.cnh_categoria,
    m.cnh_data_expiracao,
    NULL AS data_nascimento, -- Ausente no DDL
    NULL AS nacionalidade,
    NULL AS tipo_documento_habilitacao,
    NULL AS pais_emissao_cnh,
    NULL AS data_entrada_brasil,
    NULL AS flag_traducao_juramentada,
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.Motorista m
WHERE m.cnh_data_expiracao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo10');

-- Extração de Reserva
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem, data_carga)
SELECT
    CAST(r.id_reserva AS VARCHAR(50)),
    CAST(r.id_cliente AS VARCHAR(50)),
    CAST(r.id_grupo AS VARCHAR(50)),
    CAST(r.id_patio_retirada AS VARCHAR(50)),
    CAST(r.id_patio_devolucao AS VARCHAR(50)),
    CAST(r.data_criacao_reserva AS DATETIME),
    CAST(r.data_hora_retirada_prevista AS DATETIME),
    CAST(r.data_hora_devolucao_prevista AS DATETIME),
    r.status,
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.Reserva r
WHERE r.data_criacao_reserva > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo10');

-- Extração de Locacao
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem, data_carga)
SELECT
    CAST(l.id_locacao AS VARCHAR(50)),
    CAST(l.id_reserva AS VARCHAR(50)),
    CAST(l.id_cliente AS VARCHAR(50)),
    CAST(l.id_motorista AS VARCHAR(50)),
    CAST(l.id_veiculo AS VARCHAR(50)),
    CAST(l.id_patio_retirada AS VARCHAR(50)),
    NULL AS id_patio_devolucao_prevista_origem, -- Ausente no DDL
    CAST(l.id_patio_devolucao AS VARCHAR(50)),
    CAST(l.data_hora_retirada_real AS DATETIME),
    NULL AS data_hora_devolucao_prevista, -- Ausente no DDL
    CAST(l.data_hora_devolucao_real AS DATETIME),
    l.km_saida,
    l.km_chegada,
    l.valor_cobrado_final,
    l.valor_cobrado_final, -- Assuming value is final
    l.status,
    (SELECT CAST(lp.id_protecao AS VARCHAR(50)) FROM locadora_grupo_10.LocacaoProtecao lp WHERE lp.id_locacao = l.id_locacao LIMIT 1) AS id_seguro_contratado_origem, -- Mapeamento para seguro contratado
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.Locacao l
WHERE l.data_hora_retirada_real > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo10');

-- Extração de Protecao (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem, data_carga)
SELECT
    CAST(p.id_protecao AS VARCHAR(50)),
    p.nome,
    p.descricao,
    p.valor_diaria,
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.Protecao p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo10'); -- Assumindo data_ultima_atualizacao

-- Extração de ESTADO_VEICULO_LOCACAO (de ProntuarioManutencao e FotoVeiculoEstado do Grupo 10)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, id_veiculo_origem, id_patio_origem, tipo_registro, data_hora_registro, condicao_geral, observacoes, quilometragem_evento, sistema_origem, data_carga)
SELECT
    CAST(pm.id_prontuario AS VARCHAR(50)),
    CAST((SELECT id_locacao FROM locadora_grupo_10.Locacao WHERE id_veiculo = pm.id_veiculo AND data_hora_devolucao_real IS NULL LIMIT 1) AS VARCHAR(50)), -- Inferir locação ativa
    CAST(pm.id_veiculo AS VARCHAR(50)),
    (SELECT CAST(v.id_vaga_atual FROM locadora_grupo_10.Veiculo v WHERE v.id_veiculo = pm.id_veiculo LIMIT 1) AS VARCHAR(50)) AS id_patio_origem, -- Pátio da vaga atual do veículo
    'Manutencao' AS tipo_registro,
    CAST(pm.data_servico AS DATETIME),
    pm.descricao,
    NULL AS observacoes,
    pm.quilometragem,
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.ProntuarioManutencao pm
WHERE pm.data_servico > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo10')
UNION ALL
SELECT
    CAST(fve.id_foto AS VARCHAR(50)),
    CAST(fve.id_locacao AS VARCHAR(50)),
    CAST((SELECT id_veiculo FROM locadora_grupo_10.Locacao WHERE locacao_id = fve.id_locacao LIMIT 1) AS VARCHAR(50)), -- Obter veiculo_id da locação
    (SELECT CAST(l.id_patio_devolucao FROM locadora_grupo_10.Locacao l WHERE l.id_locacao = fve.id_locacao LIMIT 1) AS VARCHAR(50)) AS id_patio_origem, -- Pátio de devolução da locação
    fve.tipo_momento,
    CAST(fve.data_hora_foto AS DATETIME),
    fve.url_foto,
    NULL AS observacoes,
    NULL AS quilometragem_evento,
    'Grupo10' AS sistema_origem,
    CURRENT_TIMESTAMP
FROM locadora_grupo_10.FotoVeiculoEstado fve
WHERE fve.data_hora_foto > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo10');
