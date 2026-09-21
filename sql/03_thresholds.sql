-- Primeira calibração. Antes de reexecutar em produção, use a
-- procedure do arquivo 07_tasks.sql para preservar o histórico.

CREATE OR REPLACE TABLE PIX_FRAUD_DB.CONTROL.RISK_THRESHOLDS AS
SELECT
    'v_inicial' AS threshold_version,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL::TIMESTAMP_NTZ AS valid_to,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY valor_brl) AS p95_valor_brl,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY razao_saldo_residual) AS p95_razao_saldo_residual,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY proporcao_valor_recebedor) AS p95_proporcao_valor_recebedor
FROM PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES;

SELECT * FROM PIX_FRAUD_DB.CONTROL.RISK_THRESHOLDS;
