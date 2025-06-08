-- SQL Script para Extração ETL para a Área de Staging
-- Adaptado ao modelo físico do Grupo A (locadora_veiculos) e aos DDLs dos Grupos 2, 3, 4, 5, 6, 7, 8, 9 e 10.

USE locadora_dw_staging;

-- NOTA: O timestamp para controle incremental (`data_ultima_atualizacao` ou similar)
-- deve existir nas tabelas OLTP de origem. Caso contrário, a lógica WHERE não funcionará
-- como extração incremental.
-- A `data_carga` nas tabelas stg_* registra o timestamp da última extração bem-sucedida.

-- ---
-- **Extração do NOSSO SISTEMA (locadora_veiculos - Grupo A)**
-- Frequência de Acionamento: Diária, tipicamente durante a madrugada para evitar impacto no OLTP.

-- Extração de EMPRESA
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, endereco, telefone, sistema_origem)
SELECT
    id_empresa,
    nome_empresa,
    cnpj,
    endereco,
    telefone,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.EMPRESA
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'GrupoA');

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, capacidade_vagas, sistema_origem)
SELECT
    id_patio,
    id_empresa,
    nome_patio,
    endereco,
    capacidade_vagas,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.PATIO
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'GrupoA');

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem)
SELECT
    id_grupo_veiculo,
    nome_grupo,
    descricao,
    valor_diaria_base,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.GRUPO_VEICULO
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'GrupoA');

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, id_patio_atual_origem, placa, chassi, marca, modelo, ano_fabricacao, cor, tipo_mecanizacao, quilometragem_atual, status_veiculo, url_foto_principal, sistema_origem)
SELECT
    id_veiculo,
    id_grupo_veiculo,
    id_patio_atual,
    placa,
    chassi,
    marca,
    modelo,
    ano_fabricacao,
    cor,
    tipo_mecanizacao,
    quilometragem_atual,
    status_veiculo,
    url_foto, -- Coluna 'url_foto' no seu DDL
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.VEICULO
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'GrupoA');

-- Extração de CLIENTE
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem)
SELECT
    id_cliente,
    tipo_cliente,
    nome_razao_social,
    cpf,
    cnpj,
    endereco,
    telefone,
    email,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.CLIENTE
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'GrupoA');

-- Extração de CONDUTOR
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, sistema_origem)
SELECT
    id_condutor,
    id_cliente,
    nome_completo,
    numero_cnh,
    categoria_cnh,
    data_expiracao_cnh,
    data_nascimento,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.CONDUTOR
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'GrupoA');

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem)
SELECT
    id_reserva,
    id_cliente,
    id_grupo_veiculo,
    id_patio_retirada_previsto,
    data_hora_reserva,
    data_hora_retirada_prevista,
    data_hora_devolucao_prevista,
    status_reserva,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.RESERVA
WHERE data_hora_reserva > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'GrupoA');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_condutor_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_previsto, valor_total_final, status_locacao, id_seguro_contratado_origem, sistema_origem)
SELECT
    id_locacao,
    id_reserva,
    id_cliente,
    id_veiculo,
    id_condutor,
    id_patio_retirada_real,
    id_patio_devolucao_prevista,
    id_patio_devolucao_real,
    data_hora_retirada_real,
    data_hora_devolucao_prevista,
    data_hora_devolucao_real,
    quilometragem_retirada,
    quilometragem_devolucao,
    valor_total_previsto,
    valor_total_final,
    status_locacao,
    NULL AS id_seguro_contratado_origem, -- Não existe no seu DDL de LOCACAO
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.LOCACAO
WHERE data_hora_retirada_real > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'GrupoA');

-- Extração de SEGURO (agora é uma entidade simples de tipo de seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem)
SELECT
    id_seguro,
    nome_seguro,
    descricao,
    valor_diario,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.SEGURO
WHERE data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'GrupoA');

-- Extração de COBRANCA (pode ser fonte para dados de valor, mas não para movimentação de pátio)
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_multas_taxas, valor_seguro, valor_descontos, valor_final_cobranca, status_pagamento, data_vencimento, data_pagamento, sistema_origem)
SELECT
    id_cobranca,
    id_locacao,
    data_cobranca,
    valor_base,
    valor_multas_taxas,
    valor_seguro,
    valor_descontos,
    valor_final_cobranca,
    status_pagamento,
    data_vencimento,
    data_pagamento,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.COBRANCA
WHERE data_cobranca > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'GrupoA');

-- Extração de ESTADO_VEICULO_LOCACAO (inferindo de FOTO_VEICULO ou PRONTUARIO para seu grupo, se não houver tabela explícita)
-- NOTA: O DDL do seu grupo não possui uma tabela explícita `ESTADO_VEICULO_LOCACAO`.
-- Para a Fato_Movimentacao_Patio, precisamos de eventos de entrada/saída.
-- Vou usar 'FOTO_VEICULO' ou 'PRONTUARIO' como fontes potenciais, com a ressalva que
-- a granularidade pode não ser ideal para rastreamento de pátio. O ideal seria uma tabela de logs de movimentação.
-- Ajuste conforme a melhor fonte de eventos no seu sistema.
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, tipo_registro, data_hora_registro, nivel_combustivel, condicao_geral, observacoes, sistema_origem)
SELECT
    p.id_prontuario, -- Usando prontuário como fonte de evento de estado/movimentação
    NULL AS id_locacao_origem, -- Prontuário pode não estar diretamente ligado a uma locação específica aqui
    'Manutencao/Revisao' AS tipo_registro, -- Exemplo de tipo de registro
    p.data_ultima_revisao AS data_hora_registro,
    NULL AS nivel_combustivel,
    p.observacoes AS condicao_geral,
    p.observacoes,
    'GrupoA' AS sistema_origem
FROM locadora_veiculos.PRONTUARIO p
WHERE p.data_ultima_revisao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'GrupoA');


---
-- **Extração de OUTROS SISTEMAS (Hipotéticos) - Ajustado com os DDLs fornecidos**
-- Frequência de Acionamento: Diária ou com periodicidade definida com cada grupo.

-- ---
-- **GRUPO 2** (Assumimos banco de dados 'locadora_grupo_2')

-- Extração de CLIENTE (Condutor e Cliente combinados)
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem)
SELECT
    c.id_cliente,
    c.tipo,
    c.nome,
    CASE WHEN c.tipo = 'PF' THEN c.cpf_cnpj ELSE NULL END, -- CPF
    CASE WHEN c.tipo = 'PJ' THEN c.cpf_cnpj ELSE NULL END, -- CNPJ
    NULL AS endereco, -- Ausente no DDL
    c.telefone,
    c.email,
    'Grupo2' AS sistema_origem
FROM locadora_grupo_2.CLIENTE c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo2');

-- Extração de CONDUTOR (derivado de CLIENTE do Grupo 2)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, sistema_origem)
SELECT
    c.id_cliente, -- Usando id_cliente como id_condutor_origem
    c.id_cliente,
    c.nome,
    c.cnh,
    c.categoria_cnh,
    c.validade_cnh,
    NULL AS data_nascimento, -- Ausente no DDL
    'Grupo2' AS sistema_origem
FROM locadora_grupo_2.CLIENTE c
WHERE c.cnh IS NOT NULL AND c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo2');

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, placa, chassi, marca, modelo, cor, sistema_origem)
SELECT
    v.id_veiculo,
    v.placa,
    v.chassi,
    v.marca,
    v.modelo,
    v.cor,
    'Grupo2' AS sistema_origem
FROM locadora_grupo_2.VEICULO v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo2');

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, nome_patio, endereco, sistema_origem)
SELECT
    p.id_patio,
    p.localizacao,
    p.localizacao, -- Usando localização como endereço também
    'Grupo2' AS sistema_origem
FROM locadora_grupo_2.PATIO p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo2');

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_patio_retirada_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem)
SELECT
    r.id_reserva,
    r.cliente_id,
    r.veiculo_id,
    r.patio_retirada_id,
    r.data_inicio AS data_hora_reserva, -- Usando data_inicio como data_hora_reserva
    r.data_inicio AS data_hora_retirada_prevista,
    r.data_fim AS data_hora_devolucao_prevista,
    r.status AS status_reserva,
    'Grupo2' AS sistema_origem
FROM locadora_grupo_2.RESERVA r
WHERE r.data_inicio > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo2');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, id_patio_devolucao_real_origem, valor_total_previsto, valor_total_final, sistema_origem)
SELECT
    l.id_locacao,
    l.reserva_id,
    l.cliente_id,
    l.veiculo_id,
    l.patio_entrega_id,
    l.valor_total,
    l.valor_total, -- Usando valor_total para ambos
    'Grupo2' AS sistema_origem
FROM locadora_grupo_2.LOCACAO l
WHERE l.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo2');
-- Muitas datas e quilometragens ausentes, serão NULL.

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, data_pagamento, sistema_origem)
SELECT
    c.id_cobranca,
    c.locacao_id,
    c.data_pagamento,
    c.valor_pago,
    c.valor_pago, -- Usando valor_pago para ambos
    c.data_pagamento,
    'Grupo2' AS sistema_origem
FROM locadora_grupo_2.COBRANCA c
WHERE c.data_pagamento > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo2');


-- ---
-- **GRUPO 3** (Assumimos banco de dados 'locadora_grupo_3')

-- Extração de EMPRESA
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, sistema_origem)
SELECT
    e.id,
    e.nome_fantasia,
    e.cnpj,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.Empresa e
WHERE e.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo3');

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, capacidade_vagas, sistema_origem)
SELECT
    p.id,
    p.empresa_id,
    p.nome,
    p.endereco,
    p.total_vagas,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.Patio p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo3');

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem)
SELECT
    gv.id,
    gv.codigo_grupo,
    gv.descricao,
    gv.preco_diario,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.GrupoVeiculo gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo3');

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, ano_fabricacao, quilometragem_atual, sistema_origem)
SELECT
    v.id,
    v.grupo_id,
    v.placa,
    v.chassi,
    v.marca,
    v.modelo,
    v.cor,
    v.transmissao, -- Já é 'automatico' ou 'manual'
    v.ano,
    v.quilometragem,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.Veiculo v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo3');

-- Extração de CLIENTE
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem)
SELECT
    c.id,
    c.tipo,
    c.nome_razao,
    CASE WHEN c.tipo = 'PF' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo = 'PJ' THEN c.cpf_cnpj ELSE NULL END,
    c.endereco,
    c.telefone1,
    c.email,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.Cliente c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo3');

-- Extração de CONDUTOR
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, sistema_origem)
SELECT
    c.id,
    c.cliente_id,
    c.nome,
    c.cnh,
    c.categoria_cnh,
    c.validade_cnh,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.Condutor c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo3');

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem)
SELECT
    r.id,
    r.cliente_id,
    r.grupo_id,
    r.patio_retirada_id,
    r.patio_devolucao_id,
    r.data_prev_retirada AS data_hora_reserva, -- Usando data_prev_retirada como data da reserva
    r.data_prev_retirada,
    r.data_prev_devolucao,
    r.status,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.Reserva r
WHERE r.data_prev_retirada > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo3');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, status_locacao, sistema_origem)
SELECT
    l.id,
    l.reserva_id,
    l.condutor_id,
    l.veiculo_id,
    l.patio_saida_id,
    l.patio_chegada_id,
    l.data_retirada,
    l.data_real_devolucao,
    l.km_saida,
    l.km_chegada,
    l.status,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.Locacao l
WHERE l.data_retirada > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo3');

-- Extração de PROTECAO_ADICIONAL (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem)
SELECT
    pa.id,
    pa.nome,
    pa.descricao,
    pa.preco_dia,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.ProtecaoAdicional pa
WHERE pa.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo3');

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, sistema_origem)
SELECT
    c.id,
    c.locacao_id,
    c.data_cobranca,
    c.valor_previsto,
    c.valor_final,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.Cobranca c
WHERE c.data_cobranca > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo3');

-- Extração de FOTOS_DEVOLUCAO (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, tipo_registro, data_hora_registro, condicao_geral, sistema_origem)
SELECT
    fd.id,
    fd.locacao_id,
    'Devolucao' AS tipo_registro,
    fd.data_hora_foto AS data_hora_registro, -- Assumindo 'data_hora_foto' para FotoDevolucao
    fd.observacoes AS condicao_geral,
    'Grupo3' AS sistema_origem
FROM locadora_grupo_3.FotoDevolucao fd
WHERE fd.data_hora_foto > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo3');


-- ---
-- **GRUPO 4** (Assumimos banco de dados 'locadora_grupo_4', esquema 'public')

-- Extração de EMPRESA (Tabela PJ)
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, endereco, sistema_origem)
SELECT
    p.ID_PJ,
    p.Nome,
    p.CNPJ,
    p.Endereco,
    'Grupo4' AS sistema_origem
FROM locadora_grupo_4.public.PJ p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo4');

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, endereco, sistema_origem)
SELECT
    p.ID_PATIO,
    p.ID_PJ,
    p.Endereco,
    'Grupo4' AS sistema_origem
FROM locadora_grupo_4.public.PATIO p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo4');

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, quilometragem_atual, status_veiculo, sistema_origem, id_grupo_veiculo_origem)
SELECT
    v.ID_VEICULO,
    v.Placa,
    v.Chassi,
    v.Marca,
    v.Modelo,
    v.Cor,
    'Manual' AS tipo_mecanizacao, -- Sem mecanização explícita, assumir manual ou inferir
    NULL AS quilometragem_atual, -- Ausente no DDL
    'Disponivel' AS status_veiculo, -- Sem status explícito
    'Grupo4' AS sistema_origem,
    (SELECT id_grupo_veiculo FROM locadora_veiculos.GRUPO_VEICULO WHERE nome_grupo = v.Grupo LIMIT 1) AS id_grupo_veiculo_origem -- Mapeamento para o grupo do nosso sistema
FROM locadora_grupo_4.public.VEICULO v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo4');

-- Extração de CLIENTE (Tabela PF)
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, endereco, sistema_origem)
SELECT
    pf.ID_PF,
    'PF',
    pf.Nome,
    pf.CPF,
    pf.Endereco,
    'Grupo4' AS sistema_origem
FROM locadora_grupo_4.public.PF pf
WHERE pf.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo4');

-- Extração de CONDUTOR (derivado de PF do Grupo 4)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, data_nascimento, nacionalidade, sistema_origem)
SELECT
    pf.ID_PF, -- ID do condutor
    pf.ID_PF, -- Cliente PF associado
    pf.Nome,
    pf.CNH,
    pf.Categoria_CNH,
    NULL AS data_expiracao_cnh, -- Ausente o campo de expiração
    pf.Data_Nascimento,
    pf.Nacionalidade,
    'Grupo4' AS sistema_origem
FROM locadora_grupo_4.public.PF pf
WHERE pf.CNH IS NOT NULL AND pf.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo4');

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_veiculo_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, sistema_origem)
SELECT
    r.ID_RESERVA,
    COALESCE(r.ID_PF, r.ID_PJ) AS id_cliente_origem, -- Cliente pode ser PF ou PJ
    r.ID_VEICULO,
    r.Data_Inicio AS data_hora_reserva,
    r.Data_Inicio AS data_hora_retirada_prevista,
    r.Data_Fim AS data_hora_devolucao_prevista,
    'Confirmada' AS status_reserva, -- Sem status explícito
    'Grupo4' AS sistema_origem
FROM locadora_grupo_4.public.RESERVA r
WHERE r.Data_Inicio > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo4');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_veiculo_origem, data_hora_retirada_real, data_hora_devolucao_real, id_seguro_contratado_origem, sistema_origem)
SELECT
    l.ID_LOCACAO,
    l.ID_RESERVA,
    l.ID_PF,
    ev.ID_VEICULO, -- Obter veículo do estado do veículo
    l.Data_Retirada AS data_hora_retirada_real,
    l.Data_Devolucao AS data_hora_devolucao_real,
    l.ID_SEGUROS,
    'Grupo4' AS sistema_origem
FROM locadora_grupo_4.public.LOCACAO l
LEFT JOIN locadora_grupo_4.public.ESTADO_VEICULO ev ON l.ID_ESTADO_VEICULO_Retirada = ev.ID_ESTADO_VEICULO
WHERE l.Data_Retirada > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo4');

-- Extração de SEGUROS (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, sistema_origem)
SELECT
    s.ID_SEGUROS,
    CONCAT_WS(' - ', s.Vidros, s.Farois, s.Faixa_Indenizacao) AS nome_seguro, -- Criar nome composto
    CONCAT('Vidros: ', s.Vidros, ', Farois: ', s.Farois, ', Faixa: ', s.Faixa_Indenizacao) AS descricao,
    'Grupo4' AS sistema_origem
FROM locadora_grupo_4.public.SEGUROS s
WHERE s.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo4');

-- Extração de ESTADO_VEICULO (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, data_hora_registro, sistema_origem)
SELECT
    ev.ID_ESTADO_VEICULO,
    ev.ID_LOCACAO,
    ev.Data_Revisao AS data_hora_registro, -- Usando Data_Revisao como data do evento
    'Grupo4' AS sistema_origem
FROM locadora_grupo_4.public.ESTADO_VEICULO ev
WHERE ev.Data_Revisao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo4');


-- ---
-- **GRUPO 5** (Assumimos banco de dados 'locadora_grupo_5')

-- Extração de GRUPOS_VEICULOS
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem)
SELECT
    gv.grupo_id,
    gv.nome_grupo,
    gv.descricao,
    gv.valor_diaria_base,
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.grupos_veiculos gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo5');

-- Extração de PATIOS
INSERT INTO stg_patio (id_patio_origem, nome_patio, endereco, sistema_origem)
SELECT
    p.patio_id,
    p.nome,
    p.endereco,
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.patios p
WHERE p.data_criacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo5');

-- Extração de VEICULOS
INSERT INTO stg_veiculo (id_veiculo_origem, placa, chassi, id_grupo_veiculo_origem, id_patio_atual_origem, marca, modelo, cor, ano_fabricacao, tipo_mecanizacao, status_veiculo, sistema_origem)
SELECT
    v.veiculo_id,
    v.placa,
    v.chassi,
    v.grupo_id,
    (SELECT p.patio_id FROM locadora_grupo_5.vagas va JOIN locadora_grupo_5.patios p ON va.patio_id = p.patio_id WHERE va.vaga_id = v.vaga_atual_id LIMIT 1) AS id_patio_atual_origem, -- Obter patio_id da vaga
    v.marca,
    v.modelo,
    v.cor,
    v.ano_fabricacao,
    v.mecanizacao,
    v.status,
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.veiculos v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo5');

-- Extração de CLIENTES
INSERT INTO stg_cliente (id_cliente_origem, nome_razao_social, cpf, cnpj, tipo_cliente, email, telefone, endereco, cidade_origem, estado_origem, sistema_origem)
SELECT
    c.cliente_id,
    c.nome_completo,
    CASE WHEN c.tipo_pessoa = 'F' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_pessoa = 'J' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_pessoa = 'F' THEN 'PF' ELSE 'PJ' END,
    c.email,
    c.telefone,
    CONCAT(c.endereco_cidade, ', ', c.endereco_estado) AS endereco, -- Concatenar cidade e estado para o campo de endereço
    c.endereco_cidade,
    c.endereco_estado,
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.clientes c
WHERE c.data_cadastro > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo5');

-- Extração de MOTORISTAS (para stg_condutor)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, sistema_origem)
SELECT
    m.motorista_id,
    m.cliente_id,
    m.nome_completo,
    m.cnh,
    m.cnh_categoria,
    m.cnh_validade,
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.motoristas m
WHERE m.cnh_validade > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo5');

-- Extração de RESERVAS
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem)
SELECT
    r.reserva_id,
    r.cliente_id,
    r.grupo_id,
    r.patio_retirada_id,
    r.data_reserva,
    r.data_prevista_retirada,
    r.data_prevista_devolucao,
    r.status_reserva,
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.reservas r
WHERE r.data_reserva > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo5');

-- Extração de LOCACOES
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, valor_total_previsto, valor_total_final, sistema_origem)
SELECT
    l.locacao_id,
    l.reserva_id,
    l.cliente_id,
    l.motorista_id,
    l.veiculo_id,
    l.patio_retirada_id,
    l.patio_devolucao_id,
    l.data_retirada_real,
    l.data_devolucao_prevista,
    l.data_devolucao_real,
    l.valor_total_previsto,
    l.valor_total_final,
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.locacoes l
WHERE l.data_retirada_real > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo5');
-- 'protecoes_adicionais' (TEXT) seria tratada na fase de Transformação para popular Dim_Seguro ou fatos de seguro.

-- Extração de COBRANCAS
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, valor_base, valor_final_cobranca, data_cobranca, data_vencimento, data_pagamento, status_pagamento, sistema_origem)
SELECT
    c.cobranca_id,
    c.locacao_id,
    c.valor,
    c.valor, -- Usando valor para ambos
    c.data_emissao,
    c.data_vencimento,
    c.data_pagamento,
    c.status_pagamento,
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.cobrancas c
WHERE c.data_emissao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo5');

-- Extração de FOTOS_VEICULOS (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, tipo_registro, data_hora_registro, condicao_geral, sistema_origem)
SELECT
    fv.foto_id,
    (SELECT locacao_id FROM locadora_grupo_5.locacoes WHERE veiculo_id = fv.veiculo_id AND data_devolucao_real IS NULL LIMIT 1) AS id_locacao_origem, -- Tentar inferir locação ativa
    fv.tipo,
    fv.data_upload,
    fv.url_foto AS condicao_geral, -- Ou uma descrição inferida
    'Grupo5' AS sistema_origem
FROM locadora_grupo_5.fotos_veiculos fv
WHERE fv.data_upload > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo5');


-- ---
-- **GRUPO 6** (Assumimos banco de dados 'locadora_grupo_6')

-- Extração de CLIENTE
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, telefone, email, sistema_origem)
SELECT
    c.cliente_id,
    c.tipo,
    c.nome_razao,
    CASE WHEN c.tipo = 'F' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo = 'J' THEN c.cpf_cnpj ELSE NULL END,
    c.telefone,
    c.email,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.CLIENTE c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo6');

-- Extração de CONDUTOR
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, sistema_origem)
SELECT
    c.condutor_id,
    c.cliente_id,
    c.nome,
    c.cnh_numero,
    c.cnh_categoria,
    c.cnh_validade,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.CONDUTOR c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo6');

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, valor_diaria_base, sistema_origem)
SELECT
    gv.grupo_id,
    gv.nome,
    gv.tarifa_diaria,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.GRUPO_VEICULO gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo6');

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, tem_ar_condicionado, tem_cadeirinha, sistema_origem)
SELECT
    v.veiculo_id,
    v.grupo_id,
    v.placa,
    v.chassis,
    v.marca,
    v.modelo,
    v.cor,
    v.mecanizacao,
    v.ar_condicionado,
    v.cadeirinha,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.VEICULO v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo6');

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, nome_patio, endereco, sistema_origem)
SELECT
    p.patio_id,
    p.nome,
    p.localizacao,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.PATIO p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo6');

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem)
SELECT
    r.reserva_id,
    r.cliente_id,
    r.grupo_id,
    r.patio_retirada_id,
    r.data_inicio AS data_hora_reserva,
    r.data_inicio AS data_hora_retirada_prevista,
    r.data_fim_previsto AS data_hora_devolucao_prevista,
    r.status,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.RESERVA r
WHERE r.data_inicio > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo6');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, status_locacao, sistema_origem)
SELECT
    l.locacao_id,
    l.reserva_id,
    l.condutor_id,
    l.veiculo_id,
    l.patio_saida_id,
    l.patio_chegada_id,
    l.data_retirada,
    l.data_devolucao_prevista,
    l.data_devolucao_real,
    'Ativa' AS status_locacao, -- Status não explícito, assumir 'Ativa' ou inferir.
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.LOCACAO l
WHERE l.data_retirada > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo6');

-- Extração de PROTECAO_ADICIONAL (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, sistema_origem)
SELECT
    pa.protecao_id,
    pa.descricao, -- Usando descrição como nome
    pa.descricao,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.PROTECAO_ADICIONAL pa
WHERE pa.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo6');

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, status_pagamento, sistema_origem)
SELECT
    c.cobranca_id,
    c.locacao_id,
    c.data_cobranca,
    c.valor_base,
    c.valor_final,
    c.status_pagamento,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.COBRANCA c
WHERE c.data_cobranca > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo6');

-- Extração de PRONTUARIO (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, tipo_registro, data_hora_registro, condicao_geral, sistema_origem)
SELECT
    p.prontuario_id,
    (SELECT locacao_id FROM locadora_grupo_6.LOCACAO WHERE veiculo_id = p.veiculo_id AND data_devolucao_real IS NULL LIMIT 1) AS id_locacao_origem, -- Inferir locação ativa
    'Manutencao' AS tipo_registro,
    p.data_registro,
    p.descricao AS condicao_geral,
    'Grupo6' AS sistema_origem
FROM locadora_grupo_6.PRONTUARIO p
WHERE p.data_registro > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo6');


-- ---
-- **GRUPO 7** (Assumimos banco de dados 'locadora_grupo_7')

-- Extração de CLIENTES
INSERT INTO stg_cliente (id_cliente_origem, nome_razao_social, cpf, cnpj, tipo_cliente, email, telefone, endereco, cidade_origem, estado_origem, sistema_origem)
SELECT
    c.cliente_id,
    c.nome_completo,
    CASE WHEN c.tipo_pessoa = 'F' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_pessoa = 'J' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_pessoa = 'F' THEN 'PF' ELSE 'PJ' END,
    c.email,
    c.telefone,
    CONCAT(c.endereco_cidade, ', ', c.endereco_estado),
    c.endereco_cidade,
    c.endereco_estado,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.clientes c
WHERE c.data_cadastro > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo7');

-- Extração de MOTORISTAS (para stg_condutor)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, sistema_origem)
SELECT
    m.motorista_id,
    m.cliente_id,
    m.nome_completo,
    m.cnh,
    m.cnh_categoria,
    m.cnh_validade,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.motoristas m
WHERE m.cnh_validade > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo7');

-- Extração de PATIOS
INSERT INTO stg_patio (id_patio_origem, nome_patio, endereco, sistema_origem)
SELECT
    p.patio_id,
    p.nome,
    p.endereco,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.patios p
WHERE p.criado_em > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo7');

-- Extração de GRUPOS_VEICULOS
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem)
SELECT
    gv.grupo_id,
    gv.nome_grupo,
    gv.descricao_grupo,
    gv.tarifa_diaria_base,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.grupos_veiculos gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo7');

-- Extração de VEICULOS
INSERT INTO stg_veiculo (id_veiculo_origem, placa, chassi, id_grupo_veiculo_origem, id_patio_atual_origem, marca, modelo, cor, ano_fabricacao, tipo_mecanizacao, tem_ar_condicionado, status_veiculo, sistema_origem)
SELECT
    v.veiculo_id,
    v.placa,
    v.chassi,
    v.grupo_id,
    (SELECT va.patio_id FROM locadora_grupo_7.vagas va WHERE va.vaga_id = v.vaga_atual_id LIMIT 1) AS id_patio_atual_origem, -- Obter patio_id da vaga
    v.marca,
    v.modelo,
    v.cor,
    v.ano_fabricacao,
    v.cambio,
    v.possui_ar_cond,
    v.situacao,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.veiculos v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo7');

-- Extração de RESERVAS
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem)
SELECT
    r.reserva_id,
    r.cliente_id,
    r.grupo_id,
    r.patio_retirada_id,
    r.criado_em AS data_hora_reserva,
    r.retirada_prevista,
    r.devolucao_prevista,
    r.situacao_reserva,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.reservas r
WHERE r.criado_em > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo7');

-- Extração de LOCACOES
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, valor_total_previsto, valor_total_final, sistema_origem)
SELECT
    l.locacao_id,
    l.reserva_id,
    l.cliente_id,
    l.motorista_id,
    l.veiculo_id,
    l.patio_retirada_id,
    l.patio_devolucao_id,
    l.retirada_real,
    l.devolucao_prevista,
    l.devolucao_real,
    l.valor_previsto,
    l.valor_final,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.locacoes l
WHERE l.retirada_real > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo7');
-- 'protecoes_extras' (TEXT) seria tratada na fase de Transformação.

-- Extração de COBRANCAS
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, valor_base, valor_final_cobranca, data_cobranca, data_vencimento, data_pagamento, status_pagamento, sistema_origem)
SELECT
    c.cobranca_id,
    c.locacao_id,
    c.valor,
    c.valor, -- Usando valor para ambos
    c.emitida_em AS data_cobranca,
    c.vencimento AS data_vencimento,
    c.pago_em AS data_pagamento,
    c.status_pago AS status_pagamento,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.cobrancas c
WHERE c.emitida_em > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo7');

-- Extração de PRONTUARIOS_VEICULOS (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, tipo_registro, data_hora_registro, condicao_geral, sistema_origem)
SELECT
    pv.prontuario_id,
    (SELECT locacao_id FROM locadora_grupo_7.locacoes WHERE veiculo_id = pv.veiculo_id AND devolucao_real IS NULL LIMIT 1) AS id_locacao_origem, -- Inferir locação ativa
    pv.tipo_evento,
    pv.data_evento,
    pv.detalhes AS condicao_geral,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.prontuarios_veiculos pv
WHERE pv.data_evento > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo7');

-- Extração de FOTOS_VEICULOS (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, tipo_registro, data_hora_registro, condicao_geral, sistema_origem)
SELECT
    fv.foto_id,
    (SELECT locacao_id FROM locadora_grupo_7.locacoes WHERE veiculo_id = fv.veiculo_id AND devolucao_real IS NULL LIMIT 1) AS id_locacao_origem, -- Inferir locação ativa
    fv.finalidade,
    fv.enviado_em,
    fv.caminho_imagem AS condicao_geral,
    'Grupo7' AS sistema_origem
FROM locadora_grupo_7.fotos_veiculos fv
WHERE fv.enviado_em > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo7');


-- ---
-- **GRUPO 8** (Assumimos banco de dados 'locadora_grupo_8', esquema 'mydb')

-- Extração de EMPRESA
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, sistema_origem)
SELECT
    e.ID_EMPRESA,
    e.NOME_EMPRESA,
    e.CNPJ_EMPRESA,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.Empresa e
WHERE e.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo8');

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, sistema_origem)
SELECT
    p.ID_PATIO,
    p.FK_ID_EMPRESA,
    p.NOME_PATIO,
    CONCAT(e.LOGRADOURO, ', ', e.NUMERO_LOGRADOURO, ' - ', b.NOME_BAIRRO, ', ', c.NOME_CIDADE, ' - ', s.NOME_ESTADO, ' CEP: ', e.CEP) AS endereco,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.Patio p
JOIN locadora_grupo_8.mydb.Endereco e ON p.FK_ID_ENDERECO = e.ID_ENDERECO
JOIN locadora_grupo_8.mydb.Bairro b ON e.FK_ID_BAIRRO = b.ID_BAIRRO
JOIN locadora_grupo_8.mydb.Cidade c ON b.FK_ID_CIDADE = c.ID_CIDADE
JOIN locadora_grupo_8.mydb.Estado s ON c.FK_SIGLA_ESTADO = s.SIGLA_ESTADO
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo8');

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem)
SELECT
    gv.ID_GRUPO_VEICULO,
    gv.NOME_GRUPO,
    gv.DESCRICAO_GRUPO,
    gv.VALOR_DIARIA_BASE_GRUPO,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.GrupoVeiculo gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo8');

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, id_patio_atual_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, tem_ar_condicionado, tem_cadeirinha, status_veiculo, sistema_origem)
SELECT
    v.ID_VEICULO,
    v.FK_ID_GRUPO_VEICULO,
    v.FK_ID_PATIO,
    v.PLACA,
    v.CHASSI,
    m.NOME_MARCA,
    mo.NOME_MODELO,
    v.COR_VEICULO,
    v.TIPO_MECANIZACAO,
    v.POSSUI_AR_CONDICIONADO,
    v.POSSUI_CADEIRINHA_CRIANCA,
    v.STATUS_VEICULO,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.Veiculo v
JOIN locadora_grupo_8.mydb.ModeloVeiculo mo ON v.FK_ID_MODELO_VEICULO = mo.ID_MODELO_VEICULO
JOIN locadora_grupo_8.mydb.MarcaVeiculo m ON mo.FK_ID_MARCA_VEICULO = m.ID_MARCA_VEICULO
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo8');

-- Extração de CLIENTE
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, sistema_origem)
SELECT
    c.ID_CLIENTE,
    c.TIPO_PESSOA,
    c.NOME_CLIENTE,
    CASE WHEN c.TIPO_PESSOA = 'F' THEN c.DOCUMENTO ELSE NULL END,
    CASE WHEN c.TIPO_PESSOA = 'J' THEN c.DOCUMENTO ELSE NULL END,
    CONCAT(e.LOGRADOURO, ', ', e.NUMERO_LOGRADOURO, ' - ', b.NOME_BAIRRO, ', ', ci.NOME_CIDADE, ' - ', st.NOME_ESTADO, ' CEP: ', e.CEP) AS endereco,
    c.TELEFONE,
    c.EMAIL,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.Cliente c
JOIN locadora_grupo_8.mydb.Endereco e ON c.FK_ID_ENDERECO = e.ID_ENDERECO
JOIN locadora_grupo_8.mydb.Bairro b ON e.FK_ID_BAIRRO = b.ID_BAIRRO
JOIN locadora_grupo_8.mydb.Cidade ci ON b.FK_ID_CIDADE = ci.ID_CIDADE
JOIN locadora_grupo_8.mydb.Estado st ON ci.FK_SIGLA_ESTADO = st.SIGLA_ESTADO
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo8');

-- Extração de CONDUTOR
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, sistema_origem)
SELECT
    co.ID_CONDUTOR,
    co.FK_ID_CLIENTE,
    co.NOME_CONDUTOR,
    co.NUMERO_CNH,
    co.CATEGORIA_CNH,
    co.DATA_VALIDADE_CNH,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.Condutor co
WHERE co.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo8');

-- Extração de LOCACAO
INSERT INTO stg_locacao (id_locacao_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_prevista, data_hora_devolucao_real, valor_total_previsto, status_locacao, sistema_origem)
SELECT
    l.ID_LOCACAO,
    l.FK_ID_CLIENTE,
    l.FK_ID_CONDUTOR,
    l.FK_ID_VEICULO,
    l.FK_ID_PATIO_RETIRADA,
    l.FK_ID_PATIO_DEVOLUCAO,
    l.FK_ID_PATIO_DEVOLUCAO, -- Assumindo devolução prevista é igual à real aqui
    l.DATA_HORA_RETIRADA_REALIZADA,
    l.DATA_HORA_DEVOLUCAO_PREVISTA,
    l.DATA_HORA_DEVOLUCAO_REALIZADA,
    l.VALOR_DIARIA_CONTRATADA,
    l.STATUS_LOCACAO,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.Locacao l
WHERE l.DATA_HORA_RETIRADA_REALIZADA > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo8');

-- Extração de COBRANCA
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, data_pagamento, status_pagamento, sistema_origem)
SELECT
    c.ID_COBRANCA,
    c.FK_ID_LOCACAO,
    c.DATA_HORA_EMISSAO,
    c.VALOR_COBRANCA,
    c.VALOR_COBRANCA, -- Assumindo base e final são o mesmo aqui
    c.DATA_HORA_PAGAMENTO,
    c.STATUS_COBRANCA,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.Cobranca c
WHERE c.DATA_HORA_EMISSAO > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo8');

-- Extração de PROTECAO_ADICIONAL (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem)
SELECT
    pa.ID_PROTECAO_ADICIONAL,
    pa.NOME_PROTECAO,
    pa.DESCRICAO_PROTECAO,
    pa.VALOR_DIARIO_PROTECAO,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.ProtecaoAdicional pa
WHERE pa.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo8');

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem)
SELECT
    r.ID_RESERVA,
    r.FK_ID_CLIENTE,
    r.FK_ID_GRUPO_VEICULO,
    r.FK_ID_PATIO_RETIRADA,
    r.FK_ID_PATIO_DEVOLUCAO,
    r.DATA_HORA_SOLICITACAO_RESERVA,
    r.DATA_HORA_PREVISTA_RETIRADA,
    r.DATA_HORA_PREVISTA_DEVOLUCAO,
    r.STATUS_RESERVA,
    'Grupo8' AS sistema_origem
FROM locadora_grupo_8.mydb.Reserva r
WHERE r.DATA_HORA_SOLICITACAO_RESERVA > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo8');


-- ---
-- **GRUPO 9** (Assumimos banco de dados 'locadora_grupo_9')

-- Extração de EMPRESA (locadora)
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, sistema_origem)
SELECT
    l.id_locadora,
    l.nome_locadora,
    l.cnpj,
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.locadora l
WHERE l.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo9');

-- Extração de PATIO
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, sistema_origem)
SELECT
    p.id_patio,
    p.id_locadora,
    p.nome_patio,
    p.endereco_patio,
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.patio p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo9');

-- Extração de GRUPO_VEICULO
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, valor_diaria_base, sistema_origem)
SELECT
    gv.id_grupo_veiculo,
    gv.nome_grupo,
    gv.faixa_valor,
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.grupo_veiculo gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo9');

-- Extração de VEICULO
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, id_patio_atual_origem, placa, chassi, marca, modelo, cor, tipo_mecanizacao, tem_ar_condicionado, status_veiculo, sistema_origem)
SELECT
    v.id_veiculo,
    v.id_grupo_veiculo,
    (SELECT va.id_patio FROM locadora_grupo_9.vaga va WHERE va.id_vaga = v.id_vaga_atual LIMIT 1) AS id_patio_atual_origem,
    v.placa,
    v.chassi,
    v.marca,
    NULL AS modelo, -- Ausente no DDL
    v.cor,
    CASE WHEN v.mecanizacao THEN 'Automatica' ELSE 'Manual' END, -- Convert BOOLEAN to VARCHAR
    v.ar_condicionado,
    v.status_veiculo,
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.veiculo v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo9');

-- Extração de CLIENTE (incluindo PF e PJ)
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, email, telefone, sistema_origem)
SELECT
    c.id_cliente,
    c.tipo_cliente,
    COALESCE(pf.nome_completo, pj.nome_empresa) AS nome_razao_social,
    pf.cpf,
    pj.cnpj,
    c.email,
    c.telefone_principal,
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.cliente c
LEFT JOIN locadora_grupo_9.pessoa_fisica pf ON c.id_cliente = pf.id_cliente
LEFT JOIN locadora_grupo_9.pessoa_juridica pj ON c.id_cliente = pj.id_cliente
WHERE c.data_cadastro > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo9');

-- Extração de CONDUTOR (motorista + cnh)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, sistema_origem)
SELECT
    m.id_motorista,
    m.id_pessoa_fisica, -- id_cliente da pessoa física associada
    pf.nome_completo,
    cnh.numero_cnh,
    cnh.categoria_cnh,
    cnh.data_validade,
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.motorista m
JOIN locadora_grupo_9.pessoa_fisica pf ON m.id_pessoa_fisica = pf.id_cliente
JOIN locadora_grupo_9.cnh cnh ON m.id_motorista = cnh.id_motorista
WHERE pf.data_nascimento > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo9');

-- Extração de RESERVA
INSERT INTO stg_reserva (id_reserva_origem, id_veiculo_origem, data_hora_reserva, data_hora_retirada_prevista, status_reserva, sistema_origem)
SELECT
    r.id_reserva,
    r.id_veiculo,
    r.data_hora_reserva_inicio,
    r.data_hora_retirada_fim,
    'Confirmada' AS status_reserva, -- Status não explícito
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.reserva r
WHERE r.data_hora_reserva_inicio > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo9');

-- Extração de LOCACAO (contrato)
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, status_locacao, sistema_origem)
SELECT
    c.id_contrato,
    c.id_reserva,
    c.id_cliente,
    c.id_motorista,
    c.id_veiculo,
    c.id_patio_retirada,
    c.id_patio_devolucao_efetiva,
    c.data_hora_contrato AS data_hora_retirada_real,
    c.status_locacao,
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.contrato c
WHERE c.data_hora_contrato > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo9');

-- Extração de PROTECAO_ADICIONAL (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem)
SELECT
    pa.id_protecao_adicional,
    pa.nome_protecao,
    pa.nome_protecao AS descricao, -- Usando nome como descrição
    pa.valor_cobrado AS valor_diario, -- Assumindo valor cobrado é valor diário
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.protecao_adicional pa
WHERE pa.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo9');

-- Extração de COBRANCA (fatura)
INSERT INTO stg_cobranca (id_cobranca_origem, id_locacao_origem, data_cobranca, valor_base, valor_final_cobranca, status_pagamento, sistema_origem)
SELECT
    c.id_fatura,
    c.id_contrato,
    c.data_emissao,
    c.valor AS valor_base,
    c.valor AS valor_final_cobranca,
    c.status_fatura,
    'Grupo9' AS sistema_origem
FROM locadora_grupo_9.cobranca c
WHERE c.data_emissao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cobranca WHERE sistema_origem = 'Grupo9');


-- ---
-- **GRUPO 10** (Assumimos banco de dados 'locadora_grupo_10')

-- Extração de Empresa
INSERT INTO stg_empresa (id_empresa_origem, nome_empresa, cnpj, sistema_origem)
SELECT
    e.id_empresa,
    e.nome_fantasia,
    e.cnpj,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.Empresa e
WHERE e.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_empresa WHERE sistema_origem = 'Grupo10');

-- Extração de Patio
INSERT INTO stg_patio (id_patio_origem, id_empresa_origem, nome_patio, endereco, sistema_origem)
SELECT
    p.id_patio,
    p.id_empresa_proprietaria,
    p.nome,
    CONCAT_WS(', ', p.endereco, p.cidade, p.estado_origem) AS endereco,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.Patio p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_patio WHERE sistema_origem = 'Grupo10');

-- Extração de GrupoVeiculo
INSERT INTO stg_grupo_veiculo (id_grupo_veiculo_origem, nome_grupo, descricao, valor_diaria_base, sistema_origem)
SELECT
    gv.id_grupo,
    gv.nome,
    gv.descricao,
    gv.valor_diaria_base,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.GrupoVeiculo gv
WHERE gv.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_grupo_veiculo WHERE sistema_origem = 'Grupo10');

-- Extração de Veiculo
INSERT INTO stg_veiculo (id_veiculo_origem, id_grupo_veiculo_origem, id_patio_atual_origem, placa, chassi, marca, modelo, cor, ano_fabricacao, tipo_mecanizacao, tem_ar_condicionado, status_veiculo, sistema_origem)
SELECT
    v.id_veiculo,
    v.id_grupo,
    (SELECT va.id_patio FROM locadora_grupo_10.Vaga va WHERE va.id_vaga = v.id_vaga_atual LIMIT 1) AS id_patio_atual_origem,
    v.placa,
    v.chassi,
    (SELECT m.nome FROM locadora_grupo_10.Marca m JOIN locadora_grupo_10.Modelo mo ON m.id_marca = mo.id_marca WHERE mo.id_modelo = v.id_modelo LIMIT 1) AS marca,
    (SELECT mo.nome FROM locadora_grupo_10.Modelo mo WHERE mo.id_modelo = v.id_modelo LIMIT 1) AS modelo,
    v.cor,
    v.ano_fabricacao,
    v.mecanizacao,
    v.tem_ar_condicionado,
    v.status_operacional,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.Veiculo v
WHERE v.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_veiculo WHERE sistema_origem = 'Grupo10');

-- Extração de Cliente
INSERT INTO stg_cliente (id_cliente_origem, tipo_cliente, nome_razao_social, cpf, cnpj, email, telefone, endereco, cidade_origem, estado_origem, sistema_origem)
SELECT
    c.id_cliente,
    c.tipo_cliente,
    c.nome_razao_social,
    CASE WHEN c.tipo_cliente = 'PF' THEN c.cpf_cnpj ELSE NULL END,
    CASE WHEN c.tipo_cliente = 'PJ' THEN c.cpf_cnpj ELSE NULL END,
    c.email,
    c.telefone,
    c.endereco_cobranca,
    c.cidade_origem,
    c.estado_origem,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.Cliente c
WHERE c.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_cliente WHERE sistema_origem = 'Grupo10');

-- Extração de Motorista (para stg_condutor)
INSERT INTO stg_condutor (id_condutor_origem, id_cliente_origem, nome_completo, numero_cnh, categoria_cnh, data_expiracao_cnh, sistema_origem)
SELECT
    m.id_motorista,
    m.id_cliente_associado,
    m.nome_completo,
    m.cnh_numero,
    m.cnh_categoria,
    m.cnh_data_expiracao,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.Motorista m
WHERE m.cnh_data_expiracao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_condutor WHERE sistema_origem = 'Grupo10');

-- Extração de Reserva
INSERT INTO stg_reserva (id_reserva_origem, id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem, id_patio_devolucao_previsto_origem, data_hora_reserva, data_hora_retirada_prevista, data_hora_devolucao_prevista, status_reserva, sistema_origem)
SELECT
    r.id_reserva,
    r.id_cliente,
    r.id_grupo,
    r.id_patio_retirada,
    r.id_patio_devolucao,
    r.data_criacao_reserva,
    r.data_hora_retirada_prevista,
    r.data_hora_devolucao_prevista,
    r.status,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.Reserva r
WHERE r.data_criacao_reserva > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_reserva WHERE sistema_origem = 'Grupo10');

-- Extração de Locacao
INSERT INTO stg_locacao (id_locacao_origem, id_reserva_origem, id_cliente_origem, id_condutor_origem, id_veiculo_origem, id_patio_retirada_real_origem, id_patio_devolucao_real_origem, data_hora_retirada_real, data_hora_devolucao_real, quilometragem_retirada, quilometragem_devolucao, valor_total_final, status_locacao, sistema_origem)
SELECT
    l.id_locacao,
    l.id_reserva,
    l.id_cliente,
    l.id_motorista,
    l.id_veiculo,
    l.id_patio_retirada,
    l.id_patio_devolucao,
    l.data_hora_retirada_real,
    l.data_hora_devolucao_real,
    l.km_saida,
    l.km_chegada,
    l.valor_cobrado_final,
    l.status,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.Locacao l
WHERE l.data_hora_retirada_real > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_locacao WHERE sistema_origem = 'Grupo10');

-- Extração de Protecao (para stg_seguro)
INSERT INTO stg_seguro (id_seguro_origem, nome_seguro, descricao, valor_diario, sistema_origem)
SELECT
    p.id_protecao,
    p.nome,
    p.descricao,
    p.valor_diaria,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.Protecao p
WHERE p.data_ultima_atualizacao > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_seguro WHERE sistema_origem = 'Grupo10');

-- Extração de ProntuarioManutencao (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, tipo_registro, data_hora_registro, quilometragem_atual, sistema_origem)
SELECT
    pm.id_prontuario,
    (SELECT locacao_id FROM locadora_grupo_10.Locacao WHERE id_veiculo = pm.id_veiculo AND data_hora_devolucao_real IS NULL LIMIT 1) AS id_locacao_origem, -- Inferir locação ativa
    'Manutencao' AS tipo_registro,
    pm.data_servico,
    pm.quilometragem,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.ProntuarioManutencao pm
WHERE pm.data_servico > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo10');

-- Extração de FotoVeiculoEstado (para stg_estado_veiculo_locacao)
INSERT INTO stg_estado_veiculo_locacao (id_estado_veiculo_locacao_origem, id_locacao_origem, tipo_registro, data_hora_registro, condicao_geral, sistema_origem)
SELECT
    fve.id_foto,
    fve.id_locacao,
    fve.tipo_momento,
    fve.data_hora_foto,
    fve.url_foto AS condicao_geral,
    'Grupo10' AS sistema_origem
FROM locadora_grupo_10.FotoVeiculoEstado fve
WHERE fve.data_hora_foto > (SELECT IFNULL(MAX(data_carga), '1900-01-01') FROM stg_estado_veiculo_locacao WHERE sistema_origem = 'Grupo10');