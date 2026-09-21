-- Pré-requisito: carregue o Parquet pela interface do Snowflake
-- na tabela PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES_RAW.
-- A carga realizada no projeto criou a coluna VARIANT_COL.

CREATE OR REPLACE TABLE PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES AS
SELECT
    SHA2(TO_JSON(VARIANT_COL), 256) AS transaction_id,
    VARIANT_COL:"id_pagador"::STRING AS id_pagador,
    VARIANT_COL:"id_recebedor"::STRING AS id_recebedor,
    VARIANT_COL:"tipo_transacao"::STRING AS tipo_transacao,
    VARIANT_COL:"valor_brl"::FLOAT AS valor_brl,
    VARIANT_COL:"saldo_anterior_pagador"::FLOAT AS saldo_anterior_pagador,
    VARIANT_COL:"saldo_posterior_pagador"::FLOAT AS saldo_posterior_pagador,
    VARIANT_COL:"saldo_anterior_recebedor"::FLOAT AS saldo_anterior_recebedor,
    VARIANT_COL:"saldo_posterior_recebedor"::FLOAT AS saldo_posterior_recebedor,
    VARIANT_COL:"datetime_brasilia"::TIMESTAMP_NTZ AS datetime_brasilia,
    VARIANT_COL:"hora_dia"::INT AS hora_dia,
    VARIANT_COL:"dia_semana"::STRING AS dia_semana,
    VARIANT_COL:"dia_util"::BOOLEAN AS dia_util,
    VARIANT_COL:"horario_noturno"::BOOLEAN AS horario_noturno,
    VARIANT_COL:"acima_limite_noturno"::BOOLEAN AS acima_limite_noturno,
    VARIANT_COL:"razao_saldo_residual"::FLOAT AS razao_saldo_residual,
    VARIANT_COL:"proporcao_valor_recebedor"::FLOAT AS proporcao_valor_recebedor,
    VARIANT_COL:"fraude"::INT AS fraude,
    'train-00000-of-00001.parquet' AS source_file,
    CURRENT_TIMESTAMP() AS ingestion_timestamp,
    CURRENT_DATE() AS ingestion_date
FROM PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES_RAW;

SELECT COUNT(*) AS total_bronze
FROM PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES;
