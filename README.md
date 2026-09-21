# PIX Fraud — Snowflake

Pipeline de engenharia de dados para análise de risco em transações PIX, implementado em **SQL no Snowflake**, com arquitetura **Medallion: Bronze, Silver e Gold**. O projeto transforma uma base de 2 milhões de registros em tabelas analíticas, aplica regras de risco, registra verificações de qualidade e agenda atualizações com Snowflake Tasks.

Desenvolvido por **Felippe Machoski de Souza**, como a implementação Snowflake de um trabalho acadêmico comparativo. A implementação complementar em Databricks é de Gabriel: [pix-fraud-databricks](https://github.com/GahRizzo/pix-fraud-databricks).

> **Escopo atual:** protótipo batch executado no Snowflake. A carga inicial e as transformações Bronze → Silver → Gold foram acionadas manualmente. Existem duas tarefas agendadas: atualização da Gold de eficácia e recalibração dos thresholds. Isso não equivale, ainda, à orquestração automática de todo o pipeline.

## Objetivo de negócio

Disponibilizar dados organizados para que equipes de prevenção a fraudes e análise de risco possam:

- Identificar transações que atendem a regras de suspeita.
- Comparar a incidência de fraude entre níveis de risco.
- Investigar padrões por dia útil e horário.
- Rastrear os parâmetros utilizados na classificação.
- Priorizar investigações a partir de uma tabela de alertas.

O foco é **integração, transformação, qualidade e disponibilização de dados**. O projeto não implementa bloqueio de pagamentos em tempo real nem treina um modelo de machine learning. A coluna `fraude` é usada para avaliar as classificações, não como entrada do score.

## Base de dados

| Item | Descrição |
| --- | --- |
| Dataset | PIX Fraud BR |
| Publicador | Perfil `andremessina`, no Hugging Face |
| Fonte | [Página do dataset](https://huggingface.co/datasets/andremessina/pix-fraud-br) |
| Formato | Parquet |
| Arquivo utilizado | `train-00000-of-00001.parquet` |
| Volume validado no ambiente | 2.000.000 de registros |
| Natureza | Base sintética para experimentação; não é um extrato bancário oficial |

A base permite exercitar ingestão, tipos semiestruturados, enriquecimento e agregações em escala maior que exemplos didáticos pequenos. Sua documentação pública ajuda a entender os atributos, mas não torna os resultados representativos de uma operação financeira real.

O tamanho exato do arquivo utilizado deve ser registrado junto à evidência de download. Esse tamanho não representa o armazenamento total do Snowflake, que inclui as diferentes tabelas e seus históricos.

### Dicionário resumido da origem

| Coluna | Significado |
| --- | --- |
| `id_pagador` | Identificador mascarado do pagador |
| `id_recebedor` | Identificador mascarado do recebedor |
| `tipo_transacao` | Modalidade PIX |
| `valor_brl` | Valor em reais |
| `saldo_anterior_pagador` | Saldo inicial do pagador |
| `saldo_posterior_pagador` | Saldo final do pagador |
| `saldo_anterior_recebedor` | Saldo inicial do recebedor |
| `saldo_posterior_recebedor` | Saldo final do recebedor |
| `datetime_brasilia` | Data e hora |
| `hora_dia` | Hora |
| `dia_semana` | Dia da semana |
| `dia_util` | Indicador de segunda a sexta |
| `horario_noturno` | Indicador noturno |
| `acima_limite_noturno` | Indicador de limite noturno |
| `razao_saldo_residual` | Proporção restante do saldo |
| `proporcao_valor_recebedor` | Proporção associada ao recebedor |
| `fraude` | Rótulo: 0 ou 1 |

As definições completas estão na [documentação da fonte](https://huggingface.co/datasets/andremessina/pix-fraud-br). Os atributos derivados são recalculados na Silver; suas definições precisam ser reconciliadas com as da origem, conforme a seção de limitações.

## Arquitetura

![Arquitetura do pipeline Snowflake](diagrama_snowflake_pix.png)

Para exibir a imagem no GitHub, mantenha `diagrama_snowflake_pix.png` na mesma pasta deste README.

| Etapa | Implementação |
| --- | --- |
| Ingestão | Upload do Parquet pela interface do Snowflake |
| Bronze bruta | `PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES_RAW` |
| Bronze estruturada | `PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES` |
| Parâmetros | `PIX_FRAUD_DB.CONTROL.RISK_THRESHOLDS` |
| Silver | `PIX_FRAUD_DB.SILVER.PIX_TRANSACOES` |
| Qualidade | `PIX_FRAUD_DB.CONTROL.QUALITY_CHECK_RESULTS` |
| Gold | Tabelas de eficácia, operação e alertas |
| Agendamento | Snowflake Tasks e procedure SQL |
| Compute observado | `COMPUTE_WH`, tamanho X-Small |
| Consumo demonstrado | Consultas SQL nas tabelas Gold |

O stage `BRONZE.STG_PIX_FRAUD` foi criado durante a preparação. Seu uso efetivo na carga não foi comprovado pelas evidências disponíveis; por isso, não é apresentado como etapa obrigatória do fluxo executado. Kafka, streaming e dashboards externos não fazem parte desta implementação demonstrada.

## Bronze

### Dados brutos

O upload criou `PIX_TRANSACOES_RAW` com uma coluna `VARIANT_COL`, que armazena os atributos de cada registro como conteúdo semiestruturado.

Exemplo de inspeção, sem alterar os dados:

```sql
SELECT VARIANT_COL
FROM PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES_RAW
LIMIT 5;
```

O acesso a um atributo exige o caminho dentro do `VARIANT`:

```sql
SELECT
    VARIANT_COL:"fraude"::INTEGER AS fraude,
    VARIANT_COL:"datetime_brasilia"::TIMESTAMP_NTZ AS datetime_brasilia
FROM PIX_FRAUD_DB.BRONZE.PIX_TRANSACOES_RAW
LIMIT 5;
```

### Dados estruturados

`BRONZE.PIX_TRANSACOES` extrai os atributos, converte seus tipos e adiciona metadados:

| Campo técnico | Finalidade |
| --- | --- |
| `transaction_id` | Identificador derivado de `SHA2(TO_JSON(VARIANT_COL), 256)` |
| `source_file` | Nome do arquivo informado na carga |
| `ingestion_timestamp` | Momento da transformação |
| `ingestion_date` | Data da transformação |

O hash não substitui uma chave de negócio fornecida pela origem: registros com conteúdo idêntico recebem o mesmo identificador. Na implementação atual, `source_file` é informado no SQL, não capturado automaticamente como metadado do arquivo.

## Silver

A tabela `SILVER.PIX_TRANSACOES` recalcula atributos temporais e proporções, aplica thresholds e produz flags, `score_risco` e `nivel_risco`.

O score soma cinco indicadores de risco. A classificação utilizada no protótipo é:

| Score | Nível |
| --- | --- |
| 0 | NORMAL |
| 1 | BAIXO |
| 2 | MEDIO |
| 3 | ALTO |
| 4 ou 5 | CRITICO |

Cada registro mantém `threshold_version` para identificar os parâmetros usados. Essa rastreabilidade é importante porque novas calibrações podem alterar classificações em execuções futuras.

## Thresholds de risco

`CONTROL.RISK_THRESHOLDS` mantém os percentis P95 de valor da transação e das proporções de saldo utilizadas pelo projeto.

| Campo | Finalidade |
| --- | --- |
| `threshold_version` | Identificação da calibração |
| `valid_from` | Início da vigência |
| `valid_to` | Fim da vigência; nulo na versão ativa |
| `p95_valor_brl` | Limiar do valor |
| `p95_razao_saldo_residual` | Limiar do saldo residual |
| `p95_proporcao_valor_recebedor` | Limiar da proporção do recebedor |

A primeira versão foi criada manualmente. A procedure `CONTROL.RECALIBRAR_THRESHOLDS()` encerra a versão ativa e publica novos parâmetros calculados sobre a Bronze.

**Publicar thresholds não reclassifica automaticamente os registros existentes.** Para isso, a Silver precisa ser processada novamente, seguida das saídas Gold.

## Qualidade de dados

As verificações são registradas em `CONTROL.QUALITY_CHECK_RESULTS`, com horário, nome, status, valor observado e regra esperada.

| Check | Resultado registrado |
| --- | --- |
| `CONTAGEM_BRONZE_SILVER` | PASS — 2.000.000 em cada camada |
| `FRAUDE_FORA_DO_DOMINIO` | PASS — 0 ocorrências |
| `FRAUDE_NULA` | PASS — 0 ocorrências |
| `SCORE_RISCO_NULO` | PASS — 0 ocorrências |
| `VALOR_INVALIDO` | PASS — 0 valores menores ou iguais a zero |

```sql
SELECT *
FROM PIX_FRAUD_DB.CONTROL.QUALITY_CHECK_RESULTS
ORDER BY CHECK_RUN_AT DESC, CHECK_NAME;
```

### Comportamento diante de falhas

O roteiro de teste utiliza uma tabela temporária isolada, `SILVER.SILVER_TESTE_FALHA`, na qual um valor é alterado para negativo. O check deve retornar `FAIL`, preservando os dados principais. O print dessa execução deve acompanhar a entrega.

Na versão atual, registrar `FAIL` **não demonstra um bloqueio automático da Gold**. A evolução prevista é integrar os testes à orquestração, interromper a publicação, preservar a última versão válida e registrar a falha para investigação.

Também é necessário incluir um teste explícito para `valor_brl IS NULL`: o predicado `valor_brl <= 0`, isoladamente, não detecta valores nulos. Igualdade de contagens, por sua vez, não comprova igualdade de conteúdo ou ausência de duplicatas.

## Gold

| Tabela | Conteúdo |
| --- | --- |
| `GOLD.EFICACIA_RISCO_PIX` | Quantidade, fraudes, valores e taxa de fraude por nível de risco |
| `GOLD.OPERACIONAL` | Agregações por dia útil e horário noturno |
| `GOLD.ALERTAS_PIX` | Transações selecionadas com `score_risco >= 3` |

A tabela de alertas é uma saída analítica. Sua existência não significa que notificações externas ou bloqueios de pagamentos tenham sido implementados.

### Consulta de negócio

```sql
SELECT
    NIVEL_RISCO,
    TRANSACOES,
    FRAUDES,
    TAXA_FRAUDE_PCT,
    VALOR_TOTAL_BRL
FROM PIX_FRAUD_DB.GOLD.EFICACIA_RISCO_PIX
ORDER BY TAXA_FRAUDE_PCT DESC;
```

### Resultados observados

| Nível | Transações | Fraudes | Taxa de fraude |
| --- | ---: | ---: | ---: |
| CRITICO | 7.274 | 842 | 11,58% |
| MEDIO | 333.683 | 6.118 | 1,83% |
| ALTO | 6.609 | 58 | 0,88% |
| BAIXO | 1.594.330 | 8.164 | 0,51% |
| NORMAL | 58.104 | 194 | 0,33% |
| **Total** | **2.000.000** | **15.376** | **0,7688%** |

O grupo CRITICO concentra uma proporção maior de fraudes e pode ajudar a priorizar investigações. Entretanto, o score não produz uma ordenação perfeitamente crescente: MEDIO apresenta taxa maior que ALTO.

Com alertas definidos por score ≥ 3, ALTO e CRITICO somam 13.883 transações e 900 fraudes. Isso corresponde a aproximadamente **6,48% de precisão** e **5,85% de recuperação das fraudes** neste conjunto. Portanto, o resultado demonstra o pipeline, mas não sustenta a adoção das regras como um detector eficaz de produção.

Esses indicadores foram calculados sobre os resultados apresentados; não representam uma avaliação independente em dados novos.

## Workflows e frequência

| Task | Frequência | Ação |
| --- | --- | --- |
| `CONTROL.TASK_REFRESH_GOLD_EFICACIA` | Diariamente às 02:00 | Recria a Gold de eficácia a partir da Silver existente |
| `CONTROL.TASK_RECALCULAR_THRESHOLDS` | Domingos às 03:00 | Executa a recalibração dos thresholds |

Fuso configurado: `America/Sao_Paulo`. Warehouse utilizado: `COMPUTE_WH`.

Uma execução da task de eficácia apresentou estado `SUCCEEDED`, com duração exibida de aproximadamente 1 segundo. Essa medida pertence **somente àquela task**; não é o tempo de execução do pipeline completo.

A frequência diária é uma proposta para consumo analítico em batch, não para autorização instantânea de pagamentos. A base estática não recebe novos dados automaticamente. A periodicidade semanal de calibração separa mudanças de parâmetros das atualizações de consumo e deve ser reavaliada conforme a disponibilidade de novos dados.

## Preparação e ordem de execução

Pré-requisitos: conta Snowflake, warehouse disponível, acesso ao arquivo Parquet e permissões para os objetos do projeto. As evidências foram produzidas em ambiente de estudo; uma implantação de produção deve usar funções de menor privilégio.

1. Criar o banco `PIX_FRAUD_DB` e os schemas `BRONZE`, `SILVER`, `GOLD` e `CONTROL`.
2. Carregar o Parquet em `BRONZE.PIX_TRANSACOES_RAW`.
3. Inspecionar `VARIANT_COL` e estruturar `BRONZE.PIX_TRANSACOES`.
4. Criar a primeira versão em `CONTROL.RISK_THRESHOLDS`.
5. Executar a transformação de `SILVER.PIX_TRANSACOES`.
6. Executar os quality checks e analisar seus resultados.
7. Criar as três tabelas Gold e executar a consulta de negócio.
8. Testar a falha em cópia temporária, sem modificar as tabelas principais.
9. Criar a procedure e as duas tasks; verificar agendamento e histórico.
10. Validar os acessos, coletar métricas e salvar evidências.

> Este README documenta o ambiente construído. Os scripts completos precisam ser exportados do Snowflake e adicionados ao repositório para permitir reprodução de ponta a ponta. Os exemplos de consulta aqui não substituem os scripts de implantação.

Evite recriar bancos ou schemas com `CREATE OR REPLACE` em ambientes que já contenham dados. Execute os comandos na ordem indicada e confira qual instrução está selecionada no editor.

## Organização sugerida do repositório

Os caminhos abaixo são uma proposta de organização para os arquivos exportados, não uma afirmação de que todos já estejam publicados.

| Caminho | Conteúdo esperado |
| --- | --- |
| `README.md` | Documentação do projeto |
| `diagrama_snowflake_pix.png` | Diagrama de arquitetura |
| `sql/01_setup.sql` | Banco, schemas e configuração |
| `sql/02_bronze.sql` | Ingestão e estruturação |
| `sql/03_thresholds.sql` | Calibração inicial |
| `sql/04_silver.sql` | Features e regras de risco |
| `sql/05_quality_checks.sql` | Checks e histórico |
| `sql/06_gold.sql` | Tabelas de consumo |
| `sql/07_tasks.sql` | Procedure e agendamentos |
| `sql/08_access.sql` | Roles e privilégios |
| `tests/quality_failure.sql` | Teste isolado de falha |
| `docs/evidencias/` | Prints das execuções e resultados |

Não versionar credenciais nem incluir a base completa por padrão. Preferir o link da fonte, acompanhado de versão ou checksum do arquivo utilizado.

## Governança e sustentabilidade

### Responsabilidade e acesso

Felippe é o responsável pela implementação acadêmica Snowflake; Gabriel, pela implementação Databricks. Para produção, a proposta é separar o dono dos dados na área de risco, o responsável técnico de engenharia e o responsável operacional pelo acompanhamento das execuções.

A role `ROLE_PIX_ANALYST` foi preparada para leitura da Gold. A comprovação final deve incluir os grants e um teste efetivo usando essa role. Também é necessário verificar `USAGE` no warehouse e a ausência de acesso indevido às outras camadas. Criar uma role, por si só, não comprova isolamento.

### Dados sensíveis

Mesmo com dados sintéticos e identificadores mascarados, o desenho considera segregação de acesso. Para dados reais, a proposta é minimizar a coleta, proteger identificadores antes da disponibilização aos analistas e restringir saídas detalhadas. Mascaramento visual, sozinho, não deve ser tratado como garantia de anonimização.

### Retenção proposta — ainda não automatizada

| Conteúdo | Política técnica inicial sugerida |
| --- | --- |
| Bronze | Até 12 meses ativos, com arquivo histórico conforme necessidade de reprocessamento |
| Silver | Janela ativa de 12 meses, ajustável à análise |
| Gold | Agregados históricos conforme utilidade; alertas detalhados com acesso restrito |
| Logs e thresholds | Retenção compatível com os dados que precisam ser auditados |

Esses prazos são hipóteses de projeto, não obrigações legais definidas. A política final depende de aprovação dos responsáveis por dados e conformidade. Como a base é histórica e estática, não aplicar expurgo baseado na data atual sem preservar o conjunto de demonstração.

### Manutenção

Versionar os scripts, registrar dependências, documentar a recuperação de falhas e manter procedimentos de passagem de conhecimento. O runbook deve indicar como verificar carga, qualidade, thresholds ativos, resultados Gold e consumo de compute.

## Comparação com Databricks

A arquitetura e o caso de uso são compartilhados, mas o grau de automação ainda é diferente. As características Databricks abaixo são documentadas no [README de Gabriel](https://github.com/GahRizzo/pix-fraud-databricks/blob/main/README.md).

| Dimensão | Snowflake deste projeto | Databricks de referência |
| --- | --- | --- |
| Desenvolvimento | SQL na interface web | PySpark, Delta Lake e pacote Python |
| Facilidade de uso | Adequado à exploração SQL; houve ajustes de carga e acesso a VARIANT | Estrutura de código, dependências e deploy por DAB; tempo de aprendizado a medir |
| Ingestão | Upload inicial e transformação em tabelas | Parquet em Volume, controle de arquivos e MERGE |
| Processamento | Reconstrução de tabelas no protótipo | Batch incremental com watermarks |
| Qualidade | Checks persistidos; bloqueio automático pendente | Quality gate interrompe a persistência da Silver |
| Orquestração | Duas tasks independentes, com escopo parcial | Workflow diário encadeado e job semanal |
| Governança | Schemas, role analítica, histórico de checks e thresholds | Unity Catalog, controles de ingestão e versionamento de parâmetros |
| Versionamento do código | Exportação dos scripts para Git ainda necessária | Código e configuração DAB no repositório |
| Performance | Evidência pontual da task Gold; benchmark completo pendente | Medição comparável pendente |
| Custo | Créditos de warehouse e armazenamento, entre componentes aplicáveis | Consumo serverless em DBUs e demais componentes aplicáveis |

Diferenças de implementação não são limitações inerentes das plataformas. A comparação precisa medir cargas e regras equivalentes antes de atribuir vantagens de custo ou desempenho.

## Custos

### Modelo e premissas

No Snowflake, o custo de compute depende do consumo de créditos. Como referência, um warehouse Standard **Gen1 X-Small** consome 1 crédito/hora, com cobrança por segundo e mínimo de 60 segundos por inicialização. Confirmar a geração e a configuração da conta antes de aplicar essa taxa. [Documentação oficial](https://docs.snowflake.com/en/user-guide/warehouses-overview).

O projeto Databricks de referência usa Serverless. Sua estimativa deve usar o SKU e os DBUs registrados para esse serviço; não somar automaticamente uma VM de cluster clássico. Os preços dependem da configuração comercial. [Preços Databricks](https://www.databricks.com/product/pricing).

### Cenário ilustrativo Snowflake

Hipótese de planejamento, não medição do ambiente: Gen1 X-Small, 30 janelas mensais de 10 minutos e 4 recalibrações de 5 minutos, com os tempos já incluindo a permanência ligada até a suspensão.

```text
Horas mensais = (30 × 10 + 4 × 5) / 60 = 5,33 horas
Compute estimado = 5,33 créditos/mês
Custo de compute = 5,33 × preço contratado por crédito
Custo total = compute + armazenamento + outros serviços aplicáveis
```

Exemplo puramente aritmético: **se** o crédito custasse US$ 3, o compute seria aproximadamente US$ 16/mês. US$ 3 não é uma cotação confirmada para esta conta. Upload inicial, desenvolvimento, consultas extras e armazenamento não estão incluídos.

### Estimativa Databricks a completar

```text
DBUs mensais = 30 × DBUs por execução diária
              + 4 × DBUs por recalibração semanal
Custo = soma dos DBUs de cada SKU × preço desse SKU
        + armazenamento e demais serviços aplicáveis
```

Faltam os consumos e preços efetivos das duas contas para concluir a comparação financeira. Créditos promocionais do trial não significam custo operacional zero. Antes da entrega final, registrar cloud, região, edição/SKU, tempo faturável, armazenamento e data dos preços.

## Limitações e próximos passos

- **Reconciliar as features:** na fonte, a proporção do recebedor usa `valor / (valor + saldo_anterior)`; na Silver construída, foi usada `valor / saldo_anterior`. Calibrar na Bronze com uma fórmula e comparar na Silver com outra compromete o significado do P95.
- **Unificar as regras temporais:** a fonte descreve outro intervalo noturno; a Silver utiliza horas ≤ 5 ou ≥ 22. Documentar a regra de negócio escolhida sem apresentá-la como regra regulatória validada.
- **Validar paridade:** utilizar o mesmo arquivo, fórmulas, thresholds e política para nulos nas duas plataformas; reconciliar contagens e agregações.
- **Automatizar o fluxo completo:** encadear ingestão, Silver, checks e todas as saídas Gold, com interrupção em caso de falha.
- **Implementar incrementalidade:** controle de arquivos, checksum, chave confiável, MERGE e checkpoints. A reconstrução atual não equivale à idempotência incremental do projeto Databricks.
- **Proteger a recalibração:** testar atomicidade, concorrência e recuperação da publicação de thresholds; garantir exatamente uma versão ativa.
- **Ampliar testes:** duplicatas, datas inválidas, nulos, divisões por zero e reconciliação dos valores agregados.
- **Medir performance:** repetir testes equivalentes, distinguir cache e inicialização e comparar tempo total e consumo, não apenas uma consulta.
- **Concluir evidências:** publicar os SQLs, prints, teste FAIL, acessos efetivos e custos medidos.

## Conclusão

O Snowflake demonstrou capacidade de transformar e agregar os 2 milhões de registros usando SQL, mantendo parâmetros de risco e um histórico de qualidade. Para um cenário predominantemente analítico e uma equipe orientada a SQL, é uma opção a considerar.

Entretanto, **a implementação Databricks de referência está mais completa em incrementalidade e orquestração**. Para operar o pipeline tal como documentado hoje, a recomendação técnica provisória é partir dela. A escolha definitiva entre plataformas permanece condicionada à correção das diferenças de regras e à comparação de custo e desempenho sob condições equivalentes.

O principal resultado deste projeto é a demonstração do ciclo de engenharia de dados e de seus controles — não uma validação de regras antifraude para produção.

## Créditos

- **Snowflake:** Felippe Machoski de Souza.
- **Databricks e referência de organização:** [Gabriel / GahRizzo](https://github.com/GahRizzo/pix-fraud-databricks).
- **Dataset:** [andremessina/pix-fraud-br](https://huggingface.co/datasets/andremessina/pix-fraud-br).

Projeto acadêmico. Antes de redistribuir dados ou código de terceiros, conferir as respectivas licenças; esta documentação não atribui uma licença ao dataset ou ao repositório de referência.
