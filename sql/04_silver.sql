-- Atenção: este script reproduz as regras usadas no protótipo.
-- Reconciliar as fórmulas com a documentação da fonte antes de produção.

CREATE OR REPLACE TABLE PIX_FRAUD_DB.SILVER.PIX_TRANSACOES AS
WITH threshold_ativo AS (
    SELECT *
    FROM PIX_FRAUD_DB.CONTROL.RISK_THRESHOLDS
    WHERE valid_to IS NULL
    QUALIFY ROW_NUMBER() OVER (ORDER BY valid_from DESC) = 1
),
features AS (
    SELECT
        b.*,
        HOUR(b.datetime_brasilia) AS hora_dia_calculada,
        CASE DAYOFWEEKISO(b.datetime_brasilia)
            WHEN 1 THEN 'segunda-feira'
            WHEN 2 THEN 'terça-feira'
            WHEN 3 THEN 'quarta-feira'
            WHEN 4 THEN 'quinta-feira'
            WHEN 5 THEN 'sexta-feira'
            WHEN 6 THEN 'sábado'
            WHEN 7 THEN 'domingo'
        END AS dia_semana_calculado,
        DAYOFWEEKISO(b.datetime_brasilia) BETWEEN 1 AND 5 AS dia_util_calculado,
        (HOUR(b.datetime_brasilia) <= 5 OR HOUR(b.datetime_brasilia) >= 22) AS horario_noturno_calculado,
        b.saldo_posterior_pagador / NULLIF(b.saldo_anterior_pagador, 0) AS razao_saldo_residual_calculada,
        b.valor_brl / NULLIF(b.saldo_anterior_recebedor, 0) AS proporcao_valor_recebedor_calculada,
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
        IFF(horario_noturno_calculado, 1, 0) AS flag_horario_noturno,
        IFF(horario_noturno_calculado AND valor_brl > 1000, 1, 0) AS flag_acima_limite_noturno,
        IFF(valor_brl > p95_valor_brl, 1, 0) AS flag_valor_alto,
        IFF(razao_saldo_residual_calculada < p95_razao_saldo_residual, 1, 0) AS flag_saldo_residual,
        IFF(proporcao_valor_recebedor_calculada > p95_proporcao_valor_recebedor, 1, 0) AS flag_proporcao_recebedor
    FROM features
),
scored AS (
    SELECT
        *,
        flag_horario_noturno + flag_acima_limite_noturno + flag_valor_alto
        + flag_saldo_residual + flag_proporcao_recebedor AS score_risco
    FROM flags
)
SELECT
    * EXCLUDE (p95_valor_brl, p95_razao_saldo_residual, p95_proporcao_valor_recebedor),
    CASE
        WHEN score_risco >= 4 THEN 'CRITICO'
        WHEN score_risco >= 3 THEN 'ALTO'
        WHEN score_risco >= 2 THEN 'MEDIO'
        WHEN score_risco >= 1 THEN 'BAIXO'
        ELSE 'NORMAL'
    END AS nivel_risco
FROM scored;
