CREATE TABLE IF NOT EXISTS fato_vendas (
    id_transacao        VARCHAR(50)     PRIMARY KEY,
    cpf_aluno_anonimo   VARCHAR(20)     NOT NULL,
    plano_adquirido     VARCHAR(255),
    valor_brl           NUMERIC(10, 2),
    data_transacao      DATE,
    cep_cobranca        VARCHAR(10),
    cidade              VARCHAR(100),
    estado              VARCHAR(2),
    bairro              VARCHAR(100),
    venda_em_feriado    BOOLEAN         DEFAULT FALSE,
    horas_assistidas    NUMERIC(10, 2),
    tickets_suporte     INTEGER,
    nps_score           INTEGER,
    mes_referencia      VARCHAR(7),
    dt_carga            TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_data    ON fato_vendas (data_transacao);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_estado  ON fato_vendas (estado);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_feriado ON fato_vendas (venda_em_feriado);
