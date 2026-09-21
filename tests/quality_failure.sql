-- Teste seguro: provoca FAIL sem alterar as tabelas oficiais.
CREATE OR REPLACE TEMPORARY TABLE PIX_FRAUD_DB.SILVER.SILVER_TESTE_FALHA AS
SELECT *
FROM PIX_FRAUD_DB.SILVER.PIX_TRANSACOES
LIMIT 1000;

UPDATE PIX_FRAUD_DB.SILVER.SILVER_TESTE_FALHA
SET valor_brl = -10
WHERE transaction_id = (
    SELECT transaction_id
    FROM PIX_FRAUD_DB.SILVER.SILVER_TESTE_FALHA
    LIMIT 1
);

SELECT
    'VALOR_INVALIDO' AS check_name,
    IFF(COUNT_IF(valor_brl IS NULL OR valor_brl <= 0) = 0, 'PASS', 'FAIL') AS status,
    COUNT_IF(valor_brl IS NULL OR valor_brl <= 0) AS observed_value,
    'valor_brl deve ser preenchido e maior que zero' AS expected_rule
FROM PIX_FRAUD_DB.SILVER.SILVER_TESTE_FALHA;
