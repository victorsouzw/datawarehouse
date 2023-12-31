DROP DATABASE backroom;
CREATE DATABASE backroom;
USE backroom;

CREATE TABLE dim_secao (
    codigo_secao VARCHAR(255) PRIMARY KEY,
    descricao_secao VARCHAR(255)
);

CREATE TABLE dim_pais (
    id_pais INTEGER PRIMARY KEY,
    pais VARCHAR(255)
);

CREATE TABLE dim_sh4 (
    codigo_sh4 INTEGER PRIMARY KEY,
    descricao_sh4 VARCHAR(255)
);

CREATE TABLE dim_sh2 (
    codigo_sh2 INTEGER PRIMARY KEY,
    descricao_sh2 VARCHAR(255)
);


CREATE TABLE fact_importacoes (
    codigo_secao VARCHAR(255),
    codigo_sh2 INTEGER,
    codigo_sh4 INTEGER,
    ano VARCHAR(255),
    mes VARCHAR(255),
    id_pais INTEGER,
    nome_da_origem VARCHAR(255),
    quilograma_liquido BIGINT,
    valor_fob_dolar DECIMAL(15,2),
    FOREIGN KEY (codigo_secao) REFERENCES dim_secao(codigo_secao),
    FOREIGN KEY (codigo_sh2) REFERENCES dim_sh2(codigo_sh2),
    FOREIGN KEY (codigo_sh4) REFERENCES dim_sh4(codigo_sh4),
    FOREIGN KEY (id_pais) REFERENCES dim_pais(id_pais)
);

CREATE TABLE fact_exportacoes (
    codigo_secao VARCHAR(255),
    codigo_sh2 INTEGER,
    codigo_sh4 INTEGER,
    ano VARCHAR(255),
    mes VARCHAR(255),
    id_pais INTEGER,
    nome_da_origem VARCHAR(255),
    quilograma_liquido BIGINT,
    valor_fob_dolar DECIMAL(15,2),
    FOREIGN KEY (codigo_secao) REFERENCES dim_secao(codigo_secao),
    FOREIGN KEY (codigo_sh2) REFERENCES dim_sh2(codigo_sh2),
    FOREIGN KEY (codigo_sh4) REFERENCES dim_sh4(codigo_sh4),
    FOREIGN KEY (id_pais) REFERENCES dim_pais(id_pais)
);
