# Evidências para a entrega

Este diretório é um checklist. Não publique prints com tokens, credenciais, dados reais de pessoas, e-mails pessoais ou o Parquet completo. Prefira capturas sem identificadores de transação/pagador/recebedor.

## Pipeline principal — 2 milhões de registros

- Contagens de `BRONZE.PIX_TRANSACOES_RAW`, `BRONZE.PIX_TRANSACOES` e `SILVER.PIX_TRANSACOES`.
- Uma versão ativa em `CONTROL.RISK_THRESHOLDS` e os P95.
- `CONTROL.QUALITY_CHECK_RESULTS` com PASS; explicar que esse registro não bloqueia a Gold principal.
- `GOLD.EFICACIA_RISCO_PIX`, `GOLD.OPERACIONAL` e contagem de `GOLD.ALERTAS_PIX`.
- Tasks independentes em `CONTROL` e, se possível, grants da role analítica.

## Task Graph demonstrativo — 10 mil registros

- Tela `Transformation → Tasks → Task Graphs → TASK_BRONZE` com quatro nós verdes e `RETURN_VALUE` por etapa.
- Histórico com um run `FAILED` na Silver e sem nova execução da Gold.
- Erro da Silver: `QUALITY_GATE: valor_brl deve ser maior que zero`.
- Recuperação: quatro novos `SUCCEEDED` e zero valores inválidos na Silver.
- Configuração `INJETAR_FALHA = FALSE` ao terminar a demonstração.

Documente no nome do arquivo o escopo (`principal` ou `demo`), a etapa e a data. As evidências numéricas são observações de um run, não garantias de execução futura. Consulte [o runbook do grafo](../task-graph-demo.md) e [as consultas de verificação](../../sql/09_verificacao.sql).
