-- Persiste o resultado dos controles depois de criar a Silver.
INSERT INTO PIX_FRAUD_DB.CONTROL.QUALITY_CHECK_RESULTS
WITH metricas AS (
    SELECT
        (SELECT COUNT(*) FROM PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES) AS qtd_bronze,
        (SELECT COUNT(*) FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES) AS qtd_silver,
        (SELECT COUNT_IF(fraude IS NULL) FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES) AS fraude_nula,
        (SELECT COUNT_IF(fraude NOT IN (0, 1)) FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES) AS fraude_fora_dominio,
        (SELECT COUNT_IF(score_risco IS NULL) FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES) AS score_nulo,
        (SELECT COUNT_IF(valor_brl IS NULL OR valor_brl <= 0) FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES) AS valor_invalido
)
SELECT CURRENT_TIMESTAMP(), 'CONTAGEM_BRONZE_SILVER',
       IFF(qtd_bronze = qtd_silver, 'PASS', 'FAIL'),
       qtd_bronze || ' Bronze / ' || qtd_silver || ' Silver',
       'A quantidade de registros deve ser igual'
FROM metricas
UNION ALL
SELECT CURRENT_TIMESTAMP(), 'FRAUDE_NULA', IFF(fraude_nula = 0, 'PASS', 'FAIL'),
       fraude_nula::STRING, 'Nenhuma transação pode ter fraude nula'
FROM metricas
UNION ALL
SELECT CURRENT_TIMESTAMP(), 'FRAUDE_FORA_DO_DOMINIO', IFF(fraude_fora_dominio = 0, 'PASS', 'FAIL'),
       fraude_fora_dominio::STRING, 'fraude deve conter apenas 0 ou 1'
FROM metricas
UNION ALL
SELECT CURRENT_TIMESTAMP(), 'SCORE_RISCO_NULO', IFF(score_nulo = 0, 'PASS', 'FAIL'),
       score_nulo::STRING, 'Toda transação deve possuir score de risco'
FROM metricas
UNION ALL
SELECT CURRENT_TIMESTAMP(), 'VALOR_INVALIDO', IFF(valor_invalido = 0, 'PASS', 'FAIL'),
       valor_invalido::STRING, 'valor_brl deve ser preenchido e maior que zero';

SELECT *
FROM PIX_FRAUD_DB.CONTROL.QUALITY_CHECK_RESULTS
ORDER BY check_run_at DESC, check_name;
