# Task Graph demonstrativo no Snowflake

> Escopo: `PIX_FRAUD_DB.DEMO_GRAFO_PIX_20260921`. Este grafo **não** executa nem substitui o pipeline principal de 2 milhões de registros em `BRONZE`, `SILVER`, `GOLD` e `CONTROL`.

## Visão do fluxo

```text
BRONZE.PIX_TRANSACOES_RAW (origem já carregada)
  → TASK_BRONZE: amostra de 10.000 linhas → BRONZE_TRANSACOES
  → TASK_THRESHOLD_BOOTSTRAP: P95 → RISK_THRESHOLDS
  → TASK_SILVER: BUILD_SILVER() + quality gate → SILVER_TRANSACOES
  → TASK_GOLD: score_risco >= 3 → GOLD_ALERTAS
```

As quatro tasks pertencem ao mesmo schema e usam `COMPUTE_WH`. A raiz `TASK_BRONZE` não tem schedule e permanece `suspended`; isso impede runs automáticos, mas permite execução manual. As três dependentes devem estar `started`. `CONFIG_EXECUCAO.INJETAR_FALHA` controla somente o teste isolado.

## O que cada camada faz

| Etapa | Entrada e transformação | Saída | Return value |
| --- | --- | --- | --- |
| Bronze | Seleciona até 10.000 registros de `BRONZE.PIX_TRANSACOES_RAW`, extrai campos de `VARIANT_COL`, cria `TRANSACTION_ID` com SHA2 e hashes para IDs de pagador/recebedor | `BRONZE_TRANSACOES` | Quantidade de transações e quantidade de `SOURCE_FILE` distintos |
| Threshold | Calcula P95 de valor, razão de saldo residual e proporção do recebedor sobre a Bronze da demo | `RISK_THRESHOLDS` com versão `v_demo` | Versão e três P95 calculados |
| Silver | Calcula atributos de tempo, proporções, cinco flags, `SCORE_RISCO` e `NIVEL_RISCO`; roda cinco checks bloqueantes | `SILVER_TRANSACOES` | Contagens e checks em PASS, fraudes e críticos |
| Gold | Seleciona apenas `SCORE_RISCO >= 3` | `GOLD_ALERTAS` | Total, críticos, altos, valor e threshold |

`SOURCE_FILE` na demo é um literal, não uma contagem de arquivos novos descobertos. A amostra utiliza `LIMIT 10000` sem uma seleção reprodutível da origem inteira; seus indicadores não devem ser comparados numericamente com o pipeline principal. O threshold `v_demo` é recalculado em cada execução, diferente do bootstrap condicional do Databricks.

## Execução saudável

1. Verificar que `CONFIG_EXECUCAO.INJETAR_FALHA = FALSE` e que as três tasks dependentes estão `started`.
2. No Snowsight, abrir **Catalog → Explorer → PIX_FRAUD_DB → DEMO_GRAFO_PIX_20260921 → Tasks → TASK_BRONZE → Graph** e usar **Run Task Graph** uma vez. Equivalente SQL: `EXECUTE TASK PIX_FRAUD_DB.DEMO_GRAFO_PIX_20260921.TASK_BRONZE;`.
3. Aguardar a conclusão em **Transformation → Tasks → Task Graphs → TASK_BRONZE**. Abrir o run para visualizar os quatro estados e `RETURN_VALUE`.
4. Executar as consultas somente leitura de `sql/09_verificacao.sql` para confirmar contagens e erros.

Na demonstração de 21/09/2026, houve 10.000 linhas na Bronze e na Silver, 33 alertas na Gold (10 críticos e 23 altos) e quatro tasks `SUCCEEDED`. A Silver teve zero `VALOR_BRL <= 0`, zero fraudes nulas/fora do domínio e zero scores nulos. Esses valores são evidências do run, não invariantes do código.

## Falha controlada e recuperação

O teste foi feito apenas no schema da demo:

```sql
UPDATE PIX_FRAUD_DB.DEMO_GRAFO_PIX_20260921.CONFIG_EXECUCAO
SET INJETAR_FALHA = TRUE;

EXECUTE TASK PIX_FRAUD_DB.DEMO_GRAFO_PIX_20260921.TASK_BRONZE;
```

A Bronze substituiu um `VALOR_BRL` por zero. No run observado, Bronze e Threshold tiveram `SUCCEEDED`; Silver retornou `FAILED` com `QUALITY_GATE: valor_brl deve ser maior que zero`; Gold **não teve nova execução**. A tabela Gold da execução saudável anterior permaneceu com 33 alertas. Não dizer que ela foi apagada ou ficou vazia.

**Ressalva de atomicidade:** `BUILD_SILVER()` cria/substitui a tabela Silver antes de lançar a exceção. A Silver da demo ficou com uma linha inválida até o run de recuperação. Portanto, o gate protege a execução da Gold, mas ainda não preserva a última Silver válida. Uma evolução é construir uma tabela candidata, validá-la e publicá-la somente após PASS.

Para recuperar:

```sql
UPDATE PIX_FRAUD_DB.DEMO_GRAFO_PIX_20260921.CONFIG_EXECUCAO
SET INJETAR_FALHA = FALSE;

EXECUTE TASK PIX_FRAUD_DB.DEMO_GRAFO_PIX_20260921.TASK_BRONZE;
```

Confirmar quatro novos `SUCCEEDED` e zero valores inválidos na Silver. Não repetir o disparo enquanto um run estiver em andamento. Cada run usa o warehouse e pode gerar créditos.

## Onde obter evidências no Snowsight

- **Estrutura:** Catalog → Explorer → schema da demo → Tasks → TASK_BRONZE → Graph.
- **Detalhes por task:** task → Overview → passar o mouse em `Return value`.
- **Run completo:** Transformation → Tasks → Task Graphs → TASK_BRONZE; o run saudável exibe quatro nós verdes e métricas em tabela.
- **Run com falha:** `Open previous runs` no grafo; selecionar o run em que a Silver falhou. A task Silver também mostra a mensagem de erro no Overview.

## Reprodutibilidade e limites

O repositório contém scripts do pipeline principal e consultas de verificação da demo, mas **ainda não contém o DDL integral instalável do grafo**, especialmente a procedure `BUILD_SILVER()`. Para fechar essa lacuna, exportar via `GET_DDL('PROCEDURE', 'PIX_FRAUD_DB.DEMO_GRAFO_PIX_20260921.BUILD_SILVER()')` e `GET_DDL('TASK', ...)` das quatro tasks, revisar dependências e versionar um instalador separado. Não executar o DDL exportado diretamente em um ambiente com objetos existentes sem revisão.

Este grafo não implementa upload automático do Parquet, controle de arquivos novos, watermarks, MERGE incremental, recalibração semanal ou as três tabelas Gold do pipeline principal.
