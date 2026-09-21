CREATE OR REPLACE TABLE PIX_FRAUD_DB.GOLD.EFICACIA_RISCO_PIX AS
SELECT
    nivel_risco,
    COUNT(*) AS transacoes,
    SUM(fraude) AS fraudes,
    ROUND(100.0 * SUM(fraude) / NULLIF(COUNT(*), 0), 2) AS taxa_fraude_pct,
    SUM(valor_brl) AS valor_total_brl,
    AVG(valor_brl) AS valor_medio_brl
FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES
GROUP BY nivel_risco;

CREATE OR REPLACE TABLE PIX_FRAUD_DB.GOLD.OPERACIONAL AS
SELECT
    dia_util_calculado AS dia_util,
    horario_noturno_calculado AS horario_noturno,
    COUNT(*) AS transacoes,
    SUM(fraude) AS fraudes,
    ROUND(100.0 * SUM(fraude) / NULLIF(COUNT(*), 0), 2) AS taxa_fraude_pct
FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES
GROUP BY dia_util_calculado, horario_noturno_calculado;

CREATE OR REPLACE TABLE PIX_FRAUD_DB.GOLD.ALERTAS_PIX AS
SELECT
    transaction_id, datetime_brasilia, id_pagador, id_recebedor,
    valor_brl, score_risco, nivel_risco, threshold_version
FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES
WHERE score_risco >= 3;

SELECT *
FROM PIX_FRAUD_DB.GOLD.EFICACIA_RISCO_PIX
ORDER BY taxa_fraude_pct DESC;
