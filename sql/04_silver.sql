-- Regras do prototipo Snowflake executado: processamento batch por CTAS.
-- Exige uma unica linha ativa em CONTROL.RISK_THRESHOLDS (VALID_TO IS NULL).
-- Reconciliar as formulas com a documentacao da fonte antes de producao.

CREATE OR REPLACE TABLE PIX_FRAUD_DB.SILVER.PIX_TRANSACOES AS
WITH threshold_ativo AS (
    SELECT *
    FROM PIX_FRAUD_DB.CONTROL.RISK_THRESHOLDS
    WHERE valid_to IS NULL
),
features AS (
    SELECT
        b.transaction_id,
        b.id_pagador,
        b.id_recebedor,
        b.tipo_transacao,
        b.valor_brl,
        b.saldo_anterior_pagador,
        b.saldo_posterior_pagador,
        b.saldo_anterior_recebedor,
        b.saldo_posterior_recebedor,
        b.datetime_brasilia,
        b.fraude,
        b.source_file,
        b.ingestion_timestamp,
        b.ingestion_date,
        HOUR(b.datetime_brasilia) AS hora_dia,
        CASE DAYOFWEEKISO(b.datetime_brasilia)
            WHEN 1 THEN 'segunda-feira'
            WHEN 2 THEN 'terca-feira'
            WHEN 3 THEN 'quarta-feira'
            WHEN 4 THEN 'quinta-feira'
            WHEN 5 THEN 'sexta-feira'
            WHEN 6 THEN 'sabado'
            WHEN 7 THEN 'domingo'
        END AS dia_semana,
        DAYOFWEEKISO(b.datetime_brasilia) BETWEEN 1 AND 5 AS dia_util,
        HOUR(b.datetime_brasilia) <= 5
            OR HOUR(b.datetime_brasilia) >= 22 AS horario_noturno,
        COALESCE(
            b.saldo_posterior_pagador
            / NULLIF(b.saldo_anterior_pagador, 0),
            0
        ) AS razao_saldo_residual,
        COALESCE(
            b.valor_brl
            / NULLIF(b.saldo_anterior_recebedor, 0),
            0
        ) AS proporcao_valor_recebedor,
        t.threshold_version,
        t.p95_valor_brl,
        t.p95_razao_saldo_residual,
        t.p95_proporcao_valor_recebedor
    FROM PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES b
    CROSS JOIN threshold_ativo t
),
flags AS (
    SELECT
        *,
        horario_noturno::INT AS flag_horario_noturno,
        (horario_noturno AND valor_brl > p95_valor_brl)::INT
            AS flag_limite_noturno,
        (valor_brl > p95_valor_brl)::INT AS flag_valor_alto,
        (razao_saldo_residual > p95_razao_saldo_residual)::INT
            AS flag_saldo_anormal,
        (proporcao_valor_recebedor > p95_proporcao_valor_recebedor)::INT
            AS flag_recebedor_anormal
    FROM features
),
scored AS (
    SELECT
        *,
        flag_horario_noturno
          + flag_limite_noturno
          + flag_valor_alto
          + flag_saldo_anormal
          + flag_recebedor_anormal AS score_risco
    FROM flags
)
SELECT
    * EXCLUDE (
        p95_valor_brl,
        p95_razao_saldo_residual,
        p95_proporcao_valor_recebedor
    ),
    CASE
        WHEN score_risco >= 4 THEN 'CRITICO'
        WHEN score_risco >= 3 THEN 'ALTO'
        WHEN score_risco >= 2 THEN 'MEDIO'
        WHEN score_risco >= 1 THEN 'BAIXO'
        ELSE 'NORMAL'
    END AS nivel_risco
FROM scored;
