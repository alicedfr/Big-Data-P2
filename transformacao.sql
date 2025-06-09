-- Script SQL de Transformação ETL para a Área de Staging

-- Criação das tabelas temporárias na área de staging
USE locadora_dw_staging;

-- 1. Tabela temporária para Dim_Empresa
CREATE TABLE IF NOT EXISTS temp_dim_empresa (
    id_empresa_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    nome_empresa VARCHAR(255) NOT NULL,
    cnpj VARCHAR(18),
    endereco_empresa VARCHAR(255),
    PRIMARY KEY (id_empresa_origem, sistema_origem)
);

-- 2. Tabela temporária para Dim_Patio
CREATE TABLE IF NOT EXISTS temp_dim_patio (
    id_patio_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    nome_patio VARCHAR(100) NOT NULL,
    endereco_patio VARCHAR(255),
    cidade_patio VARCHAR(100),
    estado_patio VARCHAR(100),
    capacidade_vagas_patio INT,
    nome_empresa_proprietaria VARCHAR(255),
    PRIMARY KEY (id_patio_origem, sistema_origem)
);

-- 3. Tabela temporária para Dim_Cliente
CREATE TABLE IF NOT EXISTS temp_dim_cliente (
    id_cliente_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    tipo_cliente VARCHAR(10) NOT NULL,
    nome_razao_social VARCHAR(255) NOT NULL,
    cpf VARCHAR(14),
    cnpj VARCHAR(18),
    endereco VARCHAR(255),
    telefone VARCHAR(20),
    email VARCHAR(100),
    cidade_cliente VARCHAR(100),
    estado_cliente VARCHAR(100),
    pais_cliente VARCHAR(100),
    data_cadastro DATE,
    PRIMARY KEY (id_cliente_origem, sistema_origem)
);

-- 4. Tabela temporária para Dim_Veiculo
CREATE TABLE IF NOT EXISTS temp_dim_veiculo (
    id_veiculo_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    placa VARCHAR(10) NOT NULL,
    chassi VARCHAR(50) NOT NULL,
    marca VARCHAR(50),
    modelo VARCHAR(100),
    ano_fabricacao SMALLINT,
    cor VARCHAR(50),
    tipo_mecanizacao VARCHAR(50),
    nome_grupo_veiculo VARCHAR(100),
    descricao_grupo_veiculo VARCHAR(255),
    valor_diaria_base_grupo DECIMAL(10, 2),
    url_foto_principal VARCHAR(255),
    tem_ar_condicionado BOOLEAN,
    tem_cadeirinha BOOLEAN,
    PRIMARY KEY (id_veiculo_origem, sistema_origem)
);

-- 5. Tabela temporária para Dim_Condutor
CREATE TABLE IF NOT EXISTS temp_dim_condutor (
    id_condutor_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    nome_completo_condutor VARCHAR(255) NOT NULL,
    numero_cnh_condutor VARCHAR(50),
    categoria_cnh_condutor VARCHAR(10),
    data_expiracao_cnh_condutor DATE,
    data_nascimento_condutor DATE,
    nacionalidade_condutor VARCHAR(100),
    tipo_documento_habilitacao VARCHAR(50),
    pais_emissao_cnh VARCHAR(100),
    data_entrada_brasil DATE,
    flag_traducao_juramentada BOOLEAN,
    PRIMARY KEY (id_condutor_origem, sistema_origem)
);

-- 6. Tabela temporária para Dim_Seguro
CREATE TABLE IF NOT EXISTS temp_dim_seguro (
    id_seguro_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    nome_seguro VARCHAR(100) NOT NULL,
    descricao_seguro VARCHAR(255),
    valor_diario_seguro DECIMAL(10, 2),
    PRIMARY KEY (id_seguro_origem, sistema_origem)
);

-- 7. Tabela temporária para Fato_Locacao
CREATE TABLE IF NOT EXISTS temp_fato_locacao (
    id_locacao_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    data_retirada_date DATE,
    hora_retirada_time TIME,
    data_dev_prev_date DATE,
    hora_dev_prev_time TIME,
    data_dev_real_date DATE,
    hora_dev_real_time TIME,
    id_cliente_origem VARCHAR(50) NOT NULL,
    id_veiculo_origem VARCHAR(50) NOT NULL,
    id_condutor_origem VARCHAR(50),
    id_patio_retirada_real_origem VARCHAR(50) NOT NULL,
    id_patio_devolucao_prevista_origem VARCHAR(50) NOT NULL,
    id_patio_devolucao_real_origem VARCHAR(50),
    status_locacao_nome VARCHAR(50) NOT NULL,
    id_seguro_contratado_origem VARCHAR(50),
    valor_total_locacao DECIMAL(10, 2),
    valor_base_locacao DECIMAL(10, 2),
    valor_multas_taxas DECIMAL(10, 2),
    valor_seguro DECIMAL(10, 2),
    valor_descontos DECIMAL(10, 2),
    quilometragem_percorrida DECIMAL(10, 2),
    duracao_locacao_horas_prevista DECIMAL(10, 2),
    duracao_locacao_horas_real DECIMAL(10, 2),
    quant_locacoes INT NOT NULL DEFAULT 1,
    PRIMARY KEY (id_locacao_origem, sistema_origem)
);

-- 8. Tabela temporária para Fato_Reserva
CREATE TABLE IF NOT EXISTS temp_fato_reserva (
    id_reserva_origem VARCHAR(50) NOT NULL,
    sistema_origem VARCHAR(50) NOT NULL,
    data_reserva_date DATE,
    hora_reserva_time TIME,
    data_ret_prev_date DATE,
    hora_ret_prev_time TIME,
    data_dev_prev_date DATE,
    hora_dev_prev_time TIME,
    id_cliente_origem VARCHAR(50) NOT NULL,
    id_grupo_veiculo_origem VARCHAR(50) NOT NULL, -- Chave de negócio para o grupo do veículo
    id_patio_retirada_previsto_origem VARCHAR(50) NOT NULL,
    status_reserva_nome VARCHAR(50) NOT NULL,
    quant_reservas INT NOT NULL DEFAULT 1,
    dias_antecedencia_reserva INT,
    duracao_reserva_horas_prevista DECIMAL(10, 2),
    PRIMARY KEY (id_reserva_origem, sistema_origem)
);

-- 9. Tabela temporária para Fato_Movimentacao_Patio
CREATE TABLE IF NOT EXISTS temp_fato_movimentacao_patio (
    id_evento_origem VARCHAR(50) NOT NULL, -- Identificador único do evento de movimentação
    sistema_origem VARCHAR(50) NOT NULL,
    data_mov_date DATE,
    hora_mov_time TIME,
    id_veiculo_origem VARCHAR(50) NOT NULL,
    id_patio_origem VARCHAR(50), -- Pode ser NULL
    id_patio_destino VARCHAR(50), -- Pode ser NULL
    tipo_movimentacao_nome VARCHAR(50) NOT NULL,
    id_empresa_proprietaria_frota_origem VARCHAR(50) NOT NULL,
    id_empresa_patio_origem VARCHAR(50) NOT NULL,
    id_locacao_origem VARCHAR(50), -- Opcional, para linkar à locação se for uma movimentação de retirada/devolução
    quant_movimentacoes INT NOT NULL DEFAULT 1,
    ocupacao_veiculo_horas DECIMAL(10, 2),
    PRIMARY KEY (id_evento_origem, sistema_origem)
);

-- Limpeza das tabelas temporárias (garantir que não há dados antigos)
TRUNCATE TABLE temp_dim_empresa;
TRUNCATE TABLE temp_dim_patio;
TRUNCATE TABLE temp_dim_cliente;
TRUNCATE TABLE temp_dim_veiculo;
TRUNCATE TABLE temp_dim_condutor;
TRUNCATE TABLE temp_dim_seguro;
TRUNCATE TABLE temp_fato_locacao;
TRUNCATE TABLE temp_fato_reserva;
TRUNCATE TABLE temp_fato_movimentacao_patio;

-- Exemplo de extração e transformação de dados de um sistema de origem (Sistema A)
-- **NOTA:** Você precisaria substituir 'seu_banco_de_origem_sistema_a' e as tabelas
-- por suas tabelas OLTP reais. Este é um EXEMPLO.

-- Inserção de dados na temp_dim_empresa (do Sistema A)
INSERT INTO temp_dim_empresa (id_empresa_origem, sistema_origem, nome_empresa, cnpj, endereco_empresa)
SELECT
    e.id_empresa,
    'SistemaA',
    e.nome,
    e.cnpj,
    e.endereco
FROM seu_banco_de_origem_sistema_a.empresas e
ON DUPLICATE KEY UPDATE -- Para garantir que as inserções de múltiplos sistemas funcionem sem erro de PK
    nome_empresa = VALUES(nome_empresa),
    cnpj = VALUES(cnpj),
    endereco_empresa = VALUES(endereco_empresa);

-- Inserção de dados na temp_dim_patio (do Sistema A)
INSERT INTO temp_dim_patio (id_patio_origem, sistema_origem, nome_patio, endereco_patio, cidade_patio, estado_patio, capacidade_vagas_patio, nome_empresa_proprietaria)
SELECT
    p.id_patio,
    'SistemaA',
    p.nome_patio,
    p.endereco,
    p.cidade,
    p.estado,
    p.capacidade,
    (SELECT nome FROM seu_banco_de_origem_sistema_a.empresas WHERE id_empresa = p.id_empresa_proprietaria)
FROM seu_banco_de_origem_sistema_a.patios p
ON DUPLICATE KEY UPDATE
    nome_patio = VALUES(nome_patio),
    endereco_patio = VALUES(endereco_patio),
    cidade_patio = VALUES(cidade_patio),
    estado_patio = VALUES(estado_patio),
    capacidade_vagas_patio = VALUES(capacidade_vagas_patio),
    nome_empresa_proprietaria = VALUES(nome_empresa_proprietaria);

-- Inserção de dados na temp_dim_cliente (do Sistema A)
INSERT INTO temp_dim_cliente (id_cliente_origem, sistema_origem, tipo_cliente, nome_razao_social, cpf, cnpj, endereco, telefone, email, cidade_cliente, estado_cliente, pais_cliente, data_cadastro)
SELECT
    c.id_cliente,
    'SistemaA',
    CASE WHEN c.cpf IS NOT NULL THEN 'PF' ELSE 'PJ' END,
    c.nome_razao_social,
    c.cpf,
    c.cnpj,
    c.endereco,
    c.telefone,
    c.email,
    c.cidade,
    c.estado,
    c.pais,
    c.data_cadastro
FROM seu_banco_de_origem_sistema_a.clientes c
ON DUPLICATE KEY UPDATE
    tipo_cliente = VALUES(tipo_cliente),
    nome_razao_social = VALUES(nome_razao_social),
    cpf = VALUES(cpf),
    cnpj = VALUES(cnpj),
    endereco = VALUES(endereco),
    telefone = VALUES(telefone),
    email = VALUES(email),
    cidade_cliente = VALUES(cidade_cliente),
    estado_cliente = VALUES(estado_cliente),
    pais_cliente = VALUES(pais_cliente),
    data_cadastro = VALUES(data_cadastro);

-- Inserção de dados na temp_dim_veiculo (do Sistema A)
INSERT INTO temp_dim_veiculo (id_veiculo_origem, sistema_origem, placa, chassi, marca, modelo, ano_fabricacao, cor, tipo_mecanizacao, nome_grupo_veiculo, descricao_grupo_veiculo, valor_diaria_base_grupo, url_foto_principal, tem_ar_condicionado, tem_cadeirinha)
SELECT
    v.id_veiculo,
    'SistemaA',
    v.placa,
    v.chassi,
    v.marca,
    v.modelo,
    v.ano_fabricacao,
    v.cor,
    v.tipo_mecanizacao,
    gv.nome_grupo,
    gv.descricao,
    gv.valor_diaria,
    v.url_foto,
    v.tem_ar_condicionado,
    v.tem_cadeirinha
FROM seu_banco_de_origem_sistema_a.veiculos v
JOIN seu_banco_de_origem_sistema_a.grupos_veiculos gv ON v.id_grupo_veiculo = gv.id_grupo_veiculo
ON DUPLICATE KEY UPDATE
    placa = VALUES(placa),
    chassi = VALUES(chassi),
    marca = VALUES(marca),
    modelo = VALUES(modelo),
    ano_fabricacao = VALUES(ano_fabricacao),
    cor = VALUES(cor),
    tipo_mecanizacao = VALUES(tipo_mecanizacao),
    nome_grupo_veiculo = VALUES(nome_grupo_veiculo),
    descricao_grupo_veiculo = VALUES(descricao_grupo_veiculo),
    valor_diaria_base_grupo = VALUES(valor_diaria_base_grupo),
    url_foto_principal = VALUES(url_foto_principal),
    tem_ar_condicionado = VALUES(tem_ar_condicionado),
    tem_cadeirinha = VALUES(tem_cadeirinha);

-- Inserção de dados na temp_dim_condutor (do Sistema A)
INSERT INTO temp_dim_condutor (id_condutor_origem, sistema_origem, nome_completo_condutor, numero_cnh_condutor, categoria_cnh_condutor, data_expiracao_cnh_condutor, data_nascimento_condutor, nacionalidade_condutor, tipo_documento_habilitacao, pais_emissao_cnh, data_entrada_brasil, flag_traducao_juramentada)
SELECT
    c.id_condutor,
    'SistemaA',
    c.nome_completo,
    c.numero_cnh,
    c.categoria_cnh,
    c.data_expiracao_cnh,
    c.data_nascimento,
    c.nacionalidade,
    c.tipo_documento_habilitacao,
    c.pais_emissao_cnh,
    c.data_entrada_brasil,
    c.flag_traducao_juramentada
FROM seu_banco_de_origem_sistema_a.condutores c
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

-- Inserção de dados na temp_dim_seguro (do Sistema A)
INSERT INTO temp_dim_seguro (id_seguro_origem, sistema_origem, nome_seguro, descricao_seguro, valor_diario_seguro)
SELECT
    s.id_seguro,
    'SistemaA',
    s.nome,
    s.descricao,
    s.valor_diario
FROM seu_banco_de_origem_sistema_a.seguros s
ON DUPLICATE KEY UPDATE
    nome_seguro = VALUES(nome_seguro),
    descricao_seguro = VALUES(descricao_seguro),
    valor_diario_seguro = VALUES(valor_diario_seguro);

-- Inserção de dados na temp_fato_locacao (do Sistema A)
INSERT INTO temp_fato_locacao (
    id_locacao_origem, sistema_origem, data_retirada_date, hora_retirada_time,
    data_dev_prev_date, hora_dev_prev_time, data_dev_real_date, hora_dev_real_time,
    id_cliente_origem, id_veiculo_origem, id_condutor_origem,
    id_patio_retirada_real_origem, id_patio_devolucao_prevista_origem, id_patio_devolucao_real_origem,
    status_locacao_nome, id_seguro_contratado_origem,
    valor_total_locacao, valor_base_locacao, valor_multas_taxas, valor_seguro, valor_descontos,
    quilometragem_percorrida, duracao_locacao_horas_prevista, duracao_locacao_horas_real
)
SELECT
    l.id_locacao,
    'SistemaA',
    DATE(l.data_hora_retirada), TIME(l.data_hora_retirada),
    DATE(l.data_hora_devolucao_prevista), TIME(l.data_hora_devolucao_prevista),
    DATE(l.data_hora_devolucao_real), TIME(l.data_hora_devolucao_real),
    l.id_cliente,
    l.id_veiculo,
    l.id_condutor_extra,
    l.id_patio_retirada,
    l.id_patio_devolucao_prevista,
    l.id_patio_devolucao_real,
    sl.nome_status,
    l.id_seguro_contratado,
    l.valor_total,
    l.valor_base,
    l.valor_multas,
    l.valor_seguro,
    l.valor_descontos,
    (l.quilometragem_devolucao - l.quilometragem_retirada),
    TIMESTAMPDIFF(HOUR, l.data_hora_retirada, l.data_hora_devolucao_prevista),
    TIMESTAMPDIFF(HOUR, l.data_hora_retirada, l.data_hora_devolucao_real)
FROM seu_banco_de_origem_sistema_a.locacoes l
JOIN seu_banco_de_origem_sistema_a.status_locacao sl ON l.id_status = sl.id_status
ON DUPLICATE KEY UPDATE -- Para evitar erros em re-execuções, embora fatos geralmente não sejam atualizados.
    data_retirada_date = VALUES(data_retirada_date),
    hora_retirada_time = VALUES(hora_retirada_time),
    data_dev_prev_date = VALUES(data_dev_prev_date),
    hora_dev_prev_time = VALUES(hora_dev_prev_time),
    data_dev_real_date = VALUES(data_dev_real_date),
    hora_dev_real_time = VALUES(hora_dev_real_time),
    id_cliente_origem = VALUES(id_cliente_origem),
    id_veiculo_origem = VALUES(id_veiculo_origem),
    id_condutor_origem = VALUES(id_condutor_origem),
    id_patio_retirada_real_origem = VALUES(id_patio_retirada_real_origem),
    id_patio_devolucao_prevista_origem = VALUES(id_patio_devolucao_prevista_origem),
    id_patio_devolucao_real_origem = VALUES(id_patio_devolucao_real_origem),
    status_locacao_nome = VALUES(status_locacao_nome),
    id_seguro_contratado_origem = VALUES(id_seguro_contratado_origem),
    valor_total_locacao = VALUES(valor_total_locacao),
    valor_base_locacao = VALUES(valor_base_locacao),
    valor_multas_taxas = VALUES(valor_multas_taxas),
    valor_seguro = VALUES(valor_seguro),
    valor_descontos = VALUES(valor_descontos),
    quilometragem_percorrida = VALUES(quilometragem_percorrida),
    duracao_locacao_horas_prevista = VALUES(duracao_locacao_horas_prevista),
    duracao_locacao_horas_real = VALUES(duracao_locacao_horas_real);

-- Inserção de dados na temp_fato_reserva (do Sistema A)
INSERT INTO temp_fato_reserva (
    id_reserva_origem, sistema_origem, data_reserva_date, hora_reserva_time,
    data_ret_prev_date, hora_ret_prev_time, data_dev_prev_date, hora_dev_prev_time,
    id_cliente_origem, id_grupo_veiculo_origem, id_patio_retirada_previsto_origem,
    status_reserva_nome, dias_antecedencia_reserva, duracao_reserva_horas_prevista
)
SELECT
    r.id_reserva,
    'SistemaA',
    DATE(r.data_hora_reserva), TIME(r.data_hora_reserva),
    DATE(r.data_hora_retirada_prevista), TIME(r.data_hora_retirada_prevista),
    DATE(r.data_hora_devolucao_prevista), TIME(r.data_hora_devolucao_prevista),
    r.id_cliente,
    r.id_grupo_veiculo, -- Assumindo que a reserva é feita por grupo de veículo
    r.id_patio_retirada_previsto,
    sr.nome_status,
    DATEDIFF(r.data_hora_retirada_prevista, r.data_hora_reserva),
    TIMESTAMPDIFF(HOUR, r.data_hora_retirada_prevista, r.data_hora_devolucao_prevista)
FROM seu_banco_de_origem_sistema_a.reservas r
JOIN seu_banco_de_origem_sistema_a.status_reserva sr ON r.id_status_reserva = sr.id_status
ON DUPLICATE KEY UPDATE
    data_reserva_date = VALUES(data_reserva_date),
    hora_reserva_time = VALUES(hora_reserva_time),
    data_ret_prev_date = VALUES(data_ret_prev_date),
    hora_ret_prev_time = VALUES(hora_ret_prev_time),
    data_dev_prev_date = VALUES(data_dev_prev_date),
    hora_dev_prev_time = VALUES(hora_dev_prev_time),
    id_cliente_origem = VALUES(id_cliente_origem),
    id_grupo_veiculo_origem = VALUES(id_grupo_veiculo_origem),
    id_patio_retirada_previsto_origem = VALUES(id_patio_retirada_previsto_origem),
    status_reserva_nome = VALUES(status_reserva_nome),
    dias_antecedencia_reserva = VALUES(dias_antecedencia_reserva),
    duracao_reserva_horas_prevista = VALUES(duracao_reserva_horas_prevista);

-- Inserção de dados na temp_fato_movimentacao_patio (do Sistema A)
INSERT INTO temp_fato_movimentacao_patio (
    id_evento_origem, sistema_origem, data_mov_date, hora_mov_time,
    id_veiculo_origem, id_patio_origem, id_patio_destino, tipo_movimentacao_nome,
    id_empresa_proprietaria_frota_origem, id_empresa_patio_origem, id_locacao_origem, ocupacao_veiculo_horas
)
SELECT
    mp.id_movimentacao_patio,
    'SistemaA',
    DATE(mp.data_hora_movimentacao), TIME(mp.data_hora_movimentacao),
    mp.id_veiculo,
    mp.id_patio_origem,
    mp.id_patio_destino,
    tmp.nome_tipo_movimentacao,
    (SELECT id_empresa_proprietaria FROM seu_banco_de_origem_sistema_a.veiculos WHERE id_veiculo = mp.id_veiculo) AS id_empresa_frota,
    mp.id_empresa_patio_origem,
    mp.id_locacao_referencia, -- Se a movimentação está ligada a uma locação
    mp.horas_ocupacao -- Assumindo que este campo existe ou é calculado na origem
FROM seu_banco_de_origem_sistema_a.movimentacao_patio mp
JOIN seu_banco_de_origem_sistema_a.tipo_movimentacao_patio tmp ON mp.id_tipo_movimentacao = tmp.id_tipo
ON DUPLICATE KEY UPDATE
    data_mov_date = VALUES(data_mov_date),
    hora_mov_time = VALUES(hora_mov_time),
    id_veiculo_origem = VALUES(id_veiculo_origem),
    id_patio_origem = VALUES(id_patio_origem),
    id_patio_destino = VALUES(id_patio_destino),
    tipo_movimentacao_nome = VALUES(tipo_movimentacao_nome),
    id_empresa_proprietaria_frota_origem = VALUES(id_empresa_proprietaria_frota_origem),
    id_empresa_patio_origem = VALUES(id_empresa_patio_origem),
    id_locacao_origem = VALUES(id_locacao_origem),
    ocupacao_veiculo_horas = VALUES(ocupacao_veiculo_horas);

-- NOTA: Repita os blocos INSERT para cada Sistema de Origem (Sistema B, Sistema C, etc.),
-- ajustando os nomes das tabelas e as colunas de acordo com o esquema de cada sistema.
-- Exemplo para Sistema B:
/*
-- Inserção de dados na temp_dim_empresa (do Sistema B)
INSERT INTO temp_dim_empresa (id_empresa_origem, sistema_origem, nome_empresa, cnpj, endereco_empresa)
SELECT
    e.emp_id,
    'SistemaB',
    e.emp_nome,
    e.emp_cnpj,
    e.emp_end
FROM seu_banco_de_origem_sistema_b.empresas_b e;

-- ... e assim por diante para todas as tabelas temporárias e sistemas de origem.
*/