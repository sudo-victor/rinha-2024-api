CREATE DATABASE bank;

CREATE TABLE clientes (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100),
    limite INTEGER,
    saldo INTEGER
    versao INTEGER DEFAULT 0
);

CREATE TABLE transacoes (
    id SERIAL PRIMARY KEY,
    cliente_id INT,
    valor INTEGER,
    tipo CHAR,
    descricao VARCHAR(10),
    realizada_em TIMESTAMPTZ,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id)
);