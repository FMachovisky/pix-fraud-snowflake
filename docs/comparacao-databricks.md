# Comparação técnica: Snowflake × Databricks

Comparação do [repositório Snowflake](https://github.com/FMachovisky/pix-fraud-snowflake) com o [repositório Databricks](https://github.com/GahRizzo/pix-fraud-databricks). A análise separa o pipeline principal Snowflake (2 milhões de linhas) do Task Graph demonstrativo (amostra de 10 mil). Não compara tempos ou custos como benchmark.

## Workflow

| Aspecto | Snowflake principal | Snowflake Task Graph demo | Databricks |
| --- | --- | --- | --- |
| Fonte | Parquet carregado manualmente em `BRONZE.PIX_TRANSACOES_RAW` | Lê amostra da RAW já carregada | Parquets em Unity Catalog Volume |
| Bronze | CTAS que extrai `VARIANT_COL` e tipa campos | CTAS em amostra; `SOURCE_FILE` literal | Descobre arquivos não processados, controle e `MERGE` por transação |
| Threshold | P95 inicial; procedure semanal separada | P95 recriado a cada run como `v_demo` | Bootstrap somente sem ativo; recalibração semanal separada |
| Silver | CTAS com regras de risco | CTAS com exceções bloqueantes | Batch incremental por watermark; quality gate antes do `MERGE` |
| Gold | Três tabelas: eficácia, operacional e alertas | Somente alertas `score >= 3` | Dois snapshots e alertas incrementais por `MERGE` |
| Orquestração | Duas tasks agendadas independentes, de escopo parcial | Quatro tasks encadeadas, raiz manual sem agenda | DAB com workflow diário encadeado e job semanal |
| Observabilidade | Histórico SQL e tabela de quality checks | Histórico por task, grafo de runs, retorno com métricas e falha controlada | Runs do Jobs & Pipelines, logs, controle e watermarks |

## Paridade de regras

O arquivo `sql/04_silver.sql` do Snowflake deve representar as cinco flags do protótipo: horário noturno, limite noturno acima do P95 de valor, valor acima do P95, razão de saldo acima do P95 e proporção do recebedor acima do P95. O módulo [risk_rules.py](https://github.com/GahRizzo/pix-fraud-databricks/blob/main/src/pix_fraud/domain/risk_rules.py) usa a mesma estrutura de score e cortes (0 NORMAL, 1 BAIXO, 2 MEDIO, 3 ALTO, 4–5 CRITICO). Isso não prova paridade linha a linha: tipo numérico, nulos, timestamp e amostra precisam ser alinhados e testados.

O dataset de origem contém features derivadas. A proporção do recebedor documentada na fonte pode não coincidir com a fórmula recalculada nos dois projetos; os percentis e o score precisam ser validados contra uma regra de negócio comum. O rótulo `fraude` é usado para avaliar o resultado, não para calcular o score.

## Garantias e lacunas

- **Incrementalidade:** o Databricks controla arquivos processados, usa `MERGE` e watermarks; o Snowflake demonstrado reconstrói tabelas. A contagem de `SOURCE_FILE` da demo não é descoberta incremental.
- **Qualidade:** no Snowflake principal, `QUALITY_CHECK_RESULTS` registra PASS/FAIL, mas não é um gate. Na demo, a exceção Silver impede nova execução da Gold, porém a tabela Silver já foi substituída antes da validação. No Databricks, o gate ocorre antes do `MERGE` da Silver e impede avanço do watermark.
- **Threshold:** a demo Snowflake recalcula sempre; o Databricks diferencia bootstrap condicional e calibração semanal. O Snowflake principal tem recalibração semanal independente, mas não dispara automaticamente o reprocessamento posterior.
- **Gold:** os 33 alertas da demo são de uma amostra de 10 mil; os 13.883 alertas do Snowflake principal vêm dos 2 milhões. Não compará-los como se fossem a mesma carga.
- **Custo/desempenho:** não extrapolar segundos de uma task ou do run demonstrativo para custo mensal. Medir a mesma entrada, a mesma versão das regras, compute, região, cache, armazenamento e preços vigentes.

## Próximos passos para uma comparação justa

1. Fixar o mesmo arquivo e checksum em ambas as plataformas.
2. Definir e versionar uma especificação única das features, regras de nulos e timezone.
3. Conciliar contagens, scores, níveis, alertas e agregados em uma amostra compartilhada.
4. Testar falha antes da publicação Silver no Snowflake, preservando a última versão válida.
5. Medir runtimes e consumo completos com múltiplos runs, distinguindo inicialização, cache e processamento.

Referências de implementação: [README Databricks](https://github.com/GahRizzo/pix-fraud-databricks/blob/main/README.md), [workflow DAB](https://github.com/GahRizzo/pix-fraud-databricks/blob/main/databricks/resources/pix_fraud_pipeline.yml), [quality gate Databricks](https://github.com/GahRizzo/pix-fraud-databricks/blob/main/src/pix_fraud/quality/quality_checks.py).
