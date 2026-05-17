# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Case Técnico – Analista de Dados (Databricks-First)
# MAGIC
# MAGIC Este notebook foi preparado **para execução direta no Databricks (Free Edition ou superior)** usando **Pyspark, Python ou SQL**. <br> 
# MAGIC Você vai trabalhar com dois CSVs:
# MAGIC - `dim_clientes.csv`
# MAGIC - `fato_receita.csv`
# MAGIC
# MAGIC > **Como preparar o ambiente (1 minuto):**
# MAGIC > 1. No Databricks, vá em **Workspace → Files** (ícone de pasta).
# MAGIC > 2. Crie uma pasta `banking_case` (opcional) e **faça upload** dos dois CSVs para dentro dela.
# MAGIC > 3. Anexe um **Compute** (serverless/cluster) ao notebook (canto superior direito).
# MAGIC > 4. Execute a seção **Setup** abaixo.
# MAGIC > 5. Inclua seus comentários/assunções no próprio notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 📤 Exportar e Enviar
# MAGIC
# MAGIC Quando terminar seu case:
# MAGIC - **File → Export**: baixe como **HTML** ou **Jupyter (.ipynb)**;
# MAGIC - Ou publique num repositório git e compartilhe o link.
# MAGIC
# MAGIC Nos retorne o notebook com suas respostas em anexo.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup (Databricks)
# MAGIC
# MAGIC O código abaixo:
# MAGIC - Define o seu usuário
# MAGIC - Recebe o input da pasta onde você subiu os arquivos
# MAGIC - Cria as tabelas no notebook para serem trabalhadas via **python**/sql/pyspark
# MAGIC

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import Window, SQLContext

# COMMAND ----------

pasta = "case" ##nome da pasta onde você subiu o notebook
usuario = spark.sql("SELECT current_user()").collect()[0][0]
print(usuario)
## --- ## Bases em Python utilizando Pandas
clientes = pd.read_csv("/Workspace/Users/{0}/{1}/dim_clientes.csv".format(usuario, pasta))
receita = pd.read_csv("/Workspace/Users/{0}/{1}/fato_receita.csv".format(usuario, pasta))



## --- ## Bases em Pyspark utilizando Pyspark
# spark_dim_clientes = spark.createDataFrame(clientes)
# spark_fato_receita = spark.createDataFrame(receita)

## --- ## Bases para SQL
# spark_dim_clientes.createOrReplaceTempView("clientes")
# spark_fato_receita.createOrReplaceTempView("receita")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ✅ Pergunta 1
# MAGIC
# MAGIC O banco está revisando sua estratégia comercial e precisa entender melhor a base de clientes e a performance de cada produto. 
# MAGIC
# MAGIC Você foi chamado para apoiar o time executivo na construção de um resumo que ajude a responder perguntas simples, mas fundamentais.
# MAGIC
# MAGIC O Head de Negócio perguntou: <br> 
# MAGIC <b>Se fossemos focar na melhoria de um produto apenas para o próximo ano, 
# MAGIC qual seria e que métricas você utilizaria para avaliar esta decisão?</b>
# MAGIC
# MAGIC - Qual a relevância de cada produto na nossa base? 
# MAGIC - Qual a receita média por cliente de cada produto?
# MAGIC - Algum produto piorou ou melhorou sua performance?

# COMMAND ----------

# MAGIC %md
# MAGIC #### ⏳ Desenvolvimento 01:

# COMMAND ----------

# a1 Qual produto para focar melhoria?
# a2 Quais as métricas utilizadas?
# a3 Qual a relevância de cada produto na nossa base? 
# a4 Qual a receita média por cliente de cada produto?
# a5 Algum produto piorou ou melhorou sua performance?

# 1o: entender as bases e variáveis
# dim_clientes contém as características dos clientes, idade, renda, perfil digital, regiao, tempo de relacionamento.
# fato_receita contém a receita mensal por produto por cliente

# Apaguei a ultima linha em branco manual ao verificar os csv's, mas poderia ter feito com dropna()

print('Dimensão clientes:', clientes.shape)
print('Clientes únicos na dimensão:', clientes['cliente_id'].nunique())

print('Fato receita:', receita.shape)
print('Clientes únicos na receita:', receita['cliente_id'].nunique())

# merge left entre as bases, trazendo as informacoes de receita e anomes para a clientes.
db1 = receita.merge(clientes, on='cliente_id', how='left')
print('db1 integrada:', db1.shape)
print('Clientes únicos na db1 :', db1['cliente_id'].nunique())

#verificacoes iniciais periodo, produtos, regioes e perfil digital
print('\nPeríodo da receita:', receita['ano_mes'].min(), 'a', receita['ano_mes'].max())

print('\n Produtos:', sorted(receita['produto_id'].unique()))
# verificar qual a distribuiçao de produto_id da base
print(db1['produto_id'].describe())

print('\n Perfil digital:', sorted(clientes['perfil_digital'].unique()))
# verificar qual a distribuiçao de perfil_digital da base
print(clientes['perfil_digital'].describe())

print('\n renda_mensal:', clientes['renda_mensal'].min(), 'a', clientes['renda_mensal'].max())
# verificar qual a faixa de renda da base
print(clientes['renda_mensal'].describe())

print('\n Regiões:', sorted(clientes['regiao'].unique()))
# verificar qual a distribuiçao de regiões da base
print(clientes['regiao'].describe())

# Com essas informações já é possível entender: 
# 1) no periodo apresentado o banco tem 1000 clientes
# 2) o banco tem 5 produtos: cartao consorcio, credito, invest e seguro
# 2) o maior produto do banco é o cartão, com 8928 lançamentos na base de receita ao longo do ano de 2024.
# 3) o perfil digital da base é alto com 667, 66,7% da base de mil clientes
# 4) a renda varia de 2 a 16,5k, com a média em 3.6k e a 75% concentrada na faixa de 0 a 4,4k.
# 5) o banco divide os clientes por 5 regiões, sendo o sudeste o principal com 449 clientes 44,9% da base.


# COMMAND ----------


 # display(db1)
# display no db1 para verificar tabela e permitir a visualizacao rapidas e entendimento dos dados macro

    # removi o display(db1) pois exporta a base inteira para o .ipynb, mas a logíca é gerar visualizações rápidas na plataforma sem onerar o sistema com diversas queries, para entender o caminho.

# na primeira visualizacao criei uma pivot de anomes por produto somando os clientes unicos, para entender a variacao da receita ao longo do tempo. Aqui é possivel perceber que temos poucos dados em jan, entao pode ser que os outros produtos ainda nao haviam começado nessa data, tendo apenas o produto cartao com os 214 clientes.
# mesmo o cartao sendo o melhor produto na qtd de clientes, é perceptivel o crescimento do credito, invest e seguros.

# na segunda visualizacao, criei outra pivot olhando para receita, cartoes mantem uma receita estavel, afinal está com clientes estaveis, porém credito e invest tem ganhos consideraveis. Em seguida alterei para receita média, assim é possivel entender o comportamento da receita por cliente, consorcio tem a maior média em dez24, seguido por invest e credito.

# na terceira verifiquei as outras variaveis alternando a coluna para regiao, renda, tempo de relacionamento e idade, para entender melhor o posicionamento da carteira, por fim deixei na visão anomes, produto e renda média.

# COMMAND ----------

# verificacao se temos churn na base:
# não tem churn em nenhum produto, isto é, todos os clientes que contratam um produto nessa base continuam gerando receita nesse produto, sem cancelamento.

receita["ano_mes"] = pd.to_datetime(receita["ano_mes"])

ultimo_mes_base = receita["ano_mes"].max()

print("Último mês da base:", ultimo_mes_base)

churn_produto = (
    receita
    .groupby(["cliente_id", "produto_id"])
    .agg(
        primeiro_mes=("ano_mes", "min"),
        ultimo_mes=("ano_mes", "max"),
        meses_ativos=("ano_mes", "nunique"),
        receita_total=("receita", "sum")
    )
    .reset_index()
)

churn_produto["churn_produto"] = churn_produto["ultimo_mes"] < ultimo_mes_base

resumo_churn_produto = (
    churn_produto
    .groupby("produto_id")
    .agg(
        clientes_produto=("cliente_id", "count"),
        clientes_com_churn=("churn_produto", "sum")
    )
    .reset_index()
)

resumo_churn_produto["taxa_churn_produto"] = (
    resumo_churn_produto["clientes_com_churn"] 
    / resumo_churn_produto["clientes_produto"]
)

resumo_churn_produto


# COMMAND ----------

# após entender o comportamento de cada produto ao longo do tempo de forma geral, criei uma tabela com os dados agregados por produto para responder a primeira questao.

#groupby por produto da db1 com a soma da receita, clientes unicos, media de receita por mês, 
db2 = (
    db1.groupby('produto_id')
        .agg(
            receita_total=('receita', 'sum'),
            clientes=('cliente_id', 'nunique'),
            receita_media_cliente_mes=('receita', 'mean')
    )
    .reset_index()
)

# criando as variaveis de receita media por ano, %receita e %pn
db2['receita_media_cliente_ano'] = db2['receita_total'] / db2['clientes']
db2['% Receita'] = 100 * db2['receita_total'] / db2['receita_total'].sum()
clientes_totais = clientes['cliente_id'].nunique()
db2['Penetração (%)'] = 100 * db2['clientes'] / clientes_totais

#renomeando para melhorar a visualizacao da tabela
db2 = db2.rename(columns={
    'produto_id': 'Produto',
    'clientes': 'Clientes Unicos',
    'receita_total': 'Receita Total',
    'receita_media_cliente_ano': 'Receita Média por cliente',
    'receita_media_cliente_mes': 'Receita Média por Cliente por mês'
})

#display da tabela final, ordenando pela receita.
display(db2[[
    'Produto',
    'Clientes Unicos',
    'Penetração (%)',
    'Receita Total',
    '% Receita',
    'Receita Média por cliente',
    'Receita Média por Cliente por mês'
]].sort_values('Receita Total', ascending=False))

# Com esses dados é possivel respondermos algumas questoes, 
# 1o o produto com maior receita e rentabilidade é o cartao, com 860 clientes, 5,17 mi de receita, 6k de receita por cliente anual e 579 mensal, contando com 86% de penetração, isto é, a cada 100 clientes 86 tem o produto cartao nessa base.

#  apenas olhando para esses dados, a resposta seria que cartoes é o melhor produto para investir, porém, acredito que os produtos de invest, seg e consorcio podem apresentar maior potencial de crescimento de receita visto que a receita media por cliente é bem maior. 
# Consorcio é o produto mais rentavel da base, com 1144 de receita media por cliente, ainda é um produto em desenvolvimento com base de clientes menor.

# a resposta quanto a qual produto focar melhorias fica será o cartao por ser o maior produto do banco, que gera a principal receita atual e tem maior número de clientes. Porém, tem o ponto de saturação, a base está estavel durante alguns meses, precisaria de mais variaveis de negócios/produtos para entender qual vale mais a pena investir.
# Investimentos e credito podem ser considerados como produtos para melhoria em formato de cross-sell com o cartão, dependendo de outras questoes como as complexidades de melhorias e custos agregados (operacoes, infra, risco, etc.)

# COMMAND ----------

# Receita e clientes únicos por produto e mês
receita_clientes_mes = (
db1.groupby(['produto_id', 'ano_mes'])
    .agg(
    receita_total=('receita', 'sum'),   
           receita_media=('receita', 'mean'),
        clientes_unicos=('cliente_id', 'nunique')
    )
    .reset_index()
    .sort_values(['produto_id', 'ano_mes'])
)

# Mudanças mês a mês por produto_id
receita_clientes_mes['delta_receita'] = receita_clientes_mes.groupby('produto_id')['receita_total'].diff()
receita_clientes_mes['delta_clientes'] = receita_clientes_mes.groupby('produto_id')['clientes_unicos'].diff()

display(receita_clientes_mes)

# Aqui, criei um gráfico de Bubble para visualizar a evolução da receita e clientes por produto e mês, visualmente facilita a percepcao do crescimento do invest e credito, e a estabilização do cartao.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### ⚪ Resposta 01: 
# MAGIC Após verificar os dados e fazer algumas análises, a resposta é que temos 3 caminhos para seguir:
# MAGIC
# MAGIC 1) Seguir com melhorias no maior produto da carteira, o cartão.
# MAGIC
# MAGIC As métricas utilizadas para essa decisão, com base nos dados disponíveis são: 
# MAGIC - Maior base de clientes 860, representando 86% da base
# MAGIC - Maior receita anual 5.1 milhões
# MAGIC - Quantidade de clientes e receita está estabilizada nos ultimos 5 meses, necessário verificar se a aquisição diminuiu, se manteve mas aumentou o churn ou se os clientes estão com limite total tomado.
# MAGIC - A ideia de seguir com o cartão é manter a receita alta do produto entrando, para poder continuar garantindo o fluxo de receita e aumentar cross-sell para os produtos menores.
# MAGIC
# MAGIC
# MAGIC
# MAGIC 2) Escalar investimentos ou crédito
# MAGIC Estão em crescimento acelerado e processo de maturação, podem alcançar maior número de clientes e resultados do que cartões no futuro, caso recebam melhorias e campanhas específicas.
# MAGIC
# MAGIC - São os maiores produtos além do cartão, tem melhor receita média por cliente por mês e podem agregar valor ao uso do principal produto com campanhas abrangendo ambos produtos.
# MAGIC
# MAGIC
# MAGIC 3) Melhorias em consórcio, é o produto que apresenta a maior receita média por cliente por mês, ainda está em processo de crescimento e ao chegar no tamanho da carteira de cartão, gerará muito mais receita, porém, é necessário entender perfil da base e interesse de contratação do produto.
# MAGIC
# MAGIC > A resposta final é que o banco deve aprofundar o estudo entre cartão, investimentos ou crédito, dependendo de outras variaveis (infra, produtos, marketing, budget, crédito, risco etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ✅ Pergunta 2
# MAGIC
# MAGIC Além do perfil dos nossos produtos, nosso Head de negócios gostaria de um breve diagnóstico dos nossos clientes:
# MAGIC 1. Existe algum perfil de renda que atendemos melhor?
# MAGIC 2. O tempo de relacionamento implica em resultados melhores? Estamos rentabilizando nossos clientes à medida que eles permanecem na base?
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### ⏳ Desenvolvimento 02:

# COMMAND ----------

# verificar qual a faixa de renda da base
display(clientes['renda_mensal'].describe())
# a média de renda é 3.6k, o minimo 2k e até 75% da base em 4.5k, assim podemos inferir as faixas 0 a 3, 3 a 5, 5 a 8 e 8+

# Verificar a quantidade total de receita, clientes e share por região usando groupby, em seguida colocar a faixa de renda.
cliente_metricas = (
    db1.groupby('cliente_id')
    .agg(
        receita_total=('receita', 'sum'),
        produtos=('produto_id', 'nunique'),
        meses_com_receita=('ano_mes', 'nunique')
    )
    .merge(clientes, on='cliente_id', how='left')
)

# criar as faixas, pd.cut ou com if/elif.
cliente_metricas['faixa_renda'] = pd.cut(
    cliente_metricas['renda_mensal'],
    bins=[0, 3000, 5000, 8000, np.inf],
    labels=['Até R$3k', 'R$3k–5k', 'R$5k–8k', 'R$8k+'],
)


# agrupar por faixa de renda
renda_resumo = (
    cliente_metricas.groupby('faixa_renda', observed=True)
    .agg(
        clientes=('cliente_id', 'nunique'),
        renda_media=('renda_mensal', 'mean'),
        receita_total=('receita_total', 'sum'),
        receita_media_cliente=('receita_total', 'mean'),
        produtos_medios_cliente=('produtos', 'mean')
    )
    .reset_index()
)

clientes_total = clientes['cliente_id'].nunique()
renda_resumo['share_clientes'] = renda_resumo['clientes'] / clientes_total

receita_total = db1['receita'].sum()
renda_resumo['share_receita'] = renda_resumo['receita_total'] / receita_total

display(renda_resumo)



# COMMAND ----------

# como a maior parte dos clientes, 463 e 353 estão nas faixas até 5k,
#  talvez, olhar mais aprofundado essas faixas mais baixas: 0-2, 2-3, 3-4, 4-5, 5-8, 8+
cliente_metricas['faixa_renda2'] = pd.cut(
    cliente_metricas['renda_mensal'],
    bins=[0, 2000, 3000,4000,5000, 8000, np.inf],
    labels=['Até R$2k', 'R$2k–3k','R$3k–4k', 'R$4k–5k','R$5k–8k', 'R$8k+'],
)

# agrupar por faixa de renda
renda_resumo2 = (
    cliente_metricas.groupby('faixa_renda2', observed=True)
    .agg(
        clientes=('cliente_id', 'nunique'),
        renda_media=('renda_mensal', 'mean'),
        receita_total=('receita_total', 'sum'),
        receita_media_cliente=('receita_total', 'mean'),
        produtos_medios_cliente=('produtos', 'mean')
    )
    .reset_index()
)

clientes_total = clientes['cliente_id'].nunique()
renda_resumo2['share_clientes'] = renda_resumo2['clientes'] / clientes_total

receita_total = db1['receita'].sum()
renda_resumo2['share_receita'] = renda_resumo2['receita_total'] / receita_total

display(renda_resumo2)


# COMMAND ----------


# verificar qual a faixa de tempo de relacionamento da base
display(clientes['tempo_relacionamento_anos'].describe())

# a média de tempo de relacionamento é 10 anos, 25% em 5 anos e 50% em 10, possivel fazer os cortes em 0-2 anos novos clientes, 2-5 clientes recentes, 5-10 clientes fidelizados, 10-15 clientes longo prazo e 15+ clientes fieis

# criar as faixas de tempo e agrupar para verificacao 
cliente_metricas['faixa_tempo'] = pd.cut(
    cliente_metricas['tempo_relacionamento_anos'],
    bins=[0, 2, 5, 10, 15, np.inf],
    labels=['0–2 anos', '2–5 anos', '5–10 anos', '10–15 anos', '15+ anos'],
    right=False
)

tempo_resumo = (
    cliente_metricas.groupby('faixa_tempo', observed=True)
    .agg(
        clientes=('cliente_id', 'nunique'),
        tempo_medio=('tempo_relacionamento_anos', 'mean'),
        receita_total=('receita_total', 'sum'),
        receita_media_cliente=('receita_total', 'mean'),
        produtos_medios_cliente=('produtos', 'mean'),
        meses_com_receita_medios=('meses_com_receita', 'mean')
    )
    .reset_index()
)
tempo_resumo['share_receita'] = tempo_resumo['receita_total'] / receita_total
display(tempo_resumo)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### ⚪ Resposta 02:
# MAGIC
# MAGIC #1. Existe algum perfil de renda que atendemos melhor?
# MAGIC O banco atende melhor o perfil de faixa de renda até 5 mil, que representa 81% da carteira.
# MAGIC
# MAGIC - 46,3% dos clientes estão nas faixa até R$ 3 mil e 35,3% entre 3 e 5 mil, 
# MAGIC - A receita por cliente é parecida entre as faixas, apresentando maior receita média entre >2 e <5k. 
# MAGIC - Os clientes que entram com a renda mais baixa de exatamente 2000 tem menor receita média.
# MAGIC - Em média os clientes contratam 2 produtos, independente da faixa, com leve aumento nas faixas entre 2 a 5 mil com maior volume de clientes.
# MAGIC
# MAGIC #2. O tempo de relacionamento implica em resultados melhores? 
# MAGIC Sim para clientes na faixa de tempo de relacionamento entre 5 a 10 anos a receita média é 6% maior e na faixa de 15+ anos é 4% maior.
# MAGIC - Após os primeiros dois anos, a quantidade média de produtos contratados permanece próxima em 2,2 produtos.
# MAGIC
# MAGIC - Porém, os clientes de 10 a 15 anos apresentam queda na receita e em quantidade de produtos, seria necessário entender se essa faixa de clientes está recebendo a mesma atenção e comunicação em relação aos outros clientes.
# MAGIC
# MAGIC #3. Estamos rentabilizando nossos clientes à medida que eles permanecem na base?
# MAGIC Levemente sim, clientes com maior tempo de relacionamento tem maior receita e tem leve aumento de produtos contratados, de 2 para 2,15, 8% maior em relação a novos clientes.
# MAGIC
# MAGIC - Considerando o potencial de rentabilização e cross sell ao longo do ciclo de vida dos clientes em relação aos novos clientes, os clientes de longa data poderiam ser melhor rentabilizados.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ✅ Pergunta 3
# MAGIC
# MAGIC Entendido o comportamento dos nossos produtos, nosso Head de Negócios gostaria de focar em uma estratégia de abertura de conta através da oferta de produtos mais bem direcionados. 
# MAGIC
# MAGIC - Quais os 3 principais produtos mais contratados pós abertura de conta? 
# MAGIC - O perfil digital do cliente ou a região do cliente podem mudar nossa estratégia?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### ⏳ Desenvolvimento 03:

# COMMAND ----------

#Pergunta 3
# Quais os 3 principais produtos mais contratados pós abertura de conta?
# Cartão 86%, crédito 7% e 5% invest

#groupby por cliente e prod id verificando menor anomes
produto_primeiro_mes = (
    db1.groupby(['cliente_id', 'produto_id'], as_index=False)
    .agg(primeiro_mes_produto=('ano_mes', 'min'))
)

#groupby por cliente id agregando o primeiro produto contratado
primeiro_mes_cliente = (
    produto_primeiro_mes.groupby('cliente_id', as_index=False)
    .agg(primeiro_mes_cliente=('primeiro_mes_produto', 'min'))
)

# criando a dtb com os dados de primeiro produto por cliente
jornada_produto = (
    produto_primeiro_mes
    .merge(primeiro_mes_cliente, on='cliente_id', how='left')
    .merge(clientes, on='cliente_id', how='left')
)

# marcar quais produtos foram contratados no primeiro mês do cliente
jornada_produto['produto_primeira'] = jornada_produto['primeiro_mes_produto'].eq(jornada_produto['primeiro_mes_cliente'])

#filtrar somente os produtos de primeira compra
db_primeira_compra = (
    jornada_produto[jornada_produto['produto_primeira']]
    .groupby('produto_id')
    .agg(clientes=('cliente_id', 'nunique'))
    .reset_index()
    .sort_values('clientes', ascending=False)
)
#calcular % sobre a base total
db_primeira_compra['share_base'] = db_primeira_compra['clientes'] / clientes_total

display(db_primeira_compra)

# COMMAND ----------

# O perfil digital do cliente ou a região do cliente podem mudar nossa estratégia?
# Não, o cartão continua dominando mesmo em outras faixas de perfil digital.

display(clientes['perfil_digital'].value_counts())
display(clientes['regiao'].value_counts())
    # principalmente o perfil de clientes é digital alto e do sudeste.


# produtos por perfil digital buscando a base de primeira compra
perfil_digital = jornada_produto[jornada_produto['produto_primeira']].copy()

# agrupando a base por perfil digital e produto id, contando por cliente id
perfil_digital = (
    perfil_digital.groupby(['perfil_digital', 'produto_id'])
    .agg(clientes=('cliente_id', 'nunique'))
    .reset_index()
) 
# groupby na clientes por perfil digital para ter o total por cliente
base_digital = clientes.groupby('perfil_digital').agg(clientes_segmento=('cliente_id', 'nunique')).reset_index()

# merge entre a base de primeira compra e perfil digital
perfil_digital = perfil_digital.merge(base_digital, on='perfil_digital')

# criacao de uma coluna com a porcentagem de clientes por perfil
perfil_digital['penetracao_segmento'] = perfil_digital['clientes'] / perfil_digital['clientes_segmento']

# sort
perfil_digital = perfil_digital.sort_values(['perfil_digital', 'clientes'], ascending=[True, False])

display(perfil_digital)


# COMMAND ----------

# produtos por região, buscando a base de primeira compra
perfil_regiao = jornada_produto[jornada_produto['produto_primeira']].copy()

# agrupando a base por regiao e produto id, contando por cliente id
perfil_regiao = (
    perfil_regiao.groupby(['regiao', 'produto_id'])
    .agg(clientes=('cliente_id', 'nunique'))
    .reset_index()
)

# groupby na clientes por regiao para ter o total por cliente
base_regiao = clientes.groupby('regiao').agg(clientes_segmento=('cliente_id', 'nunique')).reset_index()

# merge entre a base de primeira compra e regiao
perfil_regiao = perfil_regiao.merge(base_regiao, on='regiao')

# criacao de uma coluna com a porcentagem de clientes por regiao
perfil_regiao['penetracao_segmento'] = perfil_regiao['clientes'] / perfil_regiao['clientes_segmento']

# sort
perfil_regiao = perfil_regiao.sort_values(['regiao', 'clientes'], ascending=[True, False])

display(perfil_regiao)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### ⚪ Resposta 03:
# MAGIC Quais os 3 principais produtos mais contratados pós abertura de conta?
# MAGIC - Os 3 principais produtos são:
# MAGIC > - Cartão com 852 clientes, 85,2% da base
# MAGIC > - Crédito com 75 clientes, 7,5% da base
# MAGIC > - Investimentos com 51 clientes, 5,1% da base
# MAGIC
# MAGIC     > Cartão é o produto de entrada principal, crédito e investimentos aparecem como próximos produtos após abertura e ativação do cartão.
# MAGIC     > Seguro e consórcio tem menor penetração, mas são produtos menores e mais recentes, talvez com menor força de aquisição em relação ao investimento e crédito.
# MAGIC
# MAGIC O perfil digital do cliente ou a região do cliente podem mudar nossa estratégia?
# MAGIC - Sim, para o público de perfil digital baixo a estratégia pode ser oferecer o produto cartão com o crédito como cross-sell, com abordagem mais assistida e simplificada no digital.
# MAGIC - Para o perfil médio, o seguro ganha relevância e o grupo pode ser tratado como grupo de transição, também seguindo a linha de abordagem assistida e simplificada para melhorar o uso digital.
# MAGIC - Para o perfil digital alto a estrategia pode ser oferecer o produto cartão aliado com investimentos e crédito pelo canal digital.
# MAGIC
# MAGIC A análise por região mostra que sim, a região pode mudar a estratégia de produtos auxiliares, porém, cartão continua seguindo como produto principal, é possível ajustar a oferta conforme perfil digital e região.
# MAGIC
# MAGIC - Sudeste tem a maior concentração dos clientes, relevante para escalar os produtos auxiliares, porém com menor volume de consorcio.
# MAGIC - Centro Oeste apresenta maior volume de consorcio entre todas as regiões, a estratégia pode ser oferecer cartão + crédito e consorcio.
# MAGIC - Nordeste cartão + crédito e investimentos
# MAGIC - Sul apresenta a menor concentração de cartoes e maior concentração de seguros, investimentos e crédito.
# MAGIC - Norte tem maior volume de cartões seguido por crédito.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🚀 Pergunta 4
# MAGIC O banco está desenvolvendo uma estratégia de cross-sell: aumentar a quantidade média de produtos contratados por cliente.
# MAGIC Hoje, sabemos quais produtos cada cliente já possui, mas queremos antecipar qual seria o próximo produto mais provável de ser adquirido, para poder direcionar campanhas de marketing e ofertas personalizadas.
# MAGIC
# MAGIC Questão de negócio
# MAGIC > O Diretor de Marketing nos perguntou: Dado o histórico de contratação dos clientes, quais produtos geralmente são contratados depois de outro?
# MAGIC > Com base nesse padrão, se um cliente possui atualmente apenas alguns produtos, qual produto deveríamos recomendar como próximo passo natural da jornada dele?
# MAGIC > 

# COMMAND ----------

# MAGIC %md
# MAGIC #### ⏳ Desenvolvimento 04:

# COMMAND ----------

# DBTITLE 1,Untitled
# para cada cliente, identificar o primeiro mes que o produto apareceu, como já feito na parte de primeiro produto, e em seguida encontrar os proximos produtos que aparecem em seguida.

# ajustar o ano mes

if not pd.api.types.is_datetime64_any_dtype(db1['ano_mes']):
    db1['ano_mes_dt'] = pd.to_datetime(db1['ano_mes'] + '-01')
else:
    db1['ano_mes_dt'] = db1['ano_mes']


# Garantir ordenação correta
produto_primeiro_mes = produto_primeiro_mes.sort_values(
    ['cliente_id', 'primeiro_mes_produto']
)

# Criar pares origem - destino
pares = []
for cliente_id, grupo in produto_primeiro_mes.groupby('cliente_id'):
    grupo = grupo.sort_values('primeiro_mes_produto')
    
    for i, origem in grupo.iterrows():
        for j, destino in grupo.iterrows():
            
            # Só considera destino se veio depois da origem
            if destino['primeiro_mes_produto'] > origem['primeiro_mes_produto']:
                pares.append({
                    'cliente_id': cliente_id,
                    'produto_origem': origem['produto_id'],
                    'produto_destino': destino['produto_id'],
                    'mes_origem': origem['primeiro_mes_produto'],
                    'mes_destino': destino['primeiro_mes_produto']
                })

pares_produtos = pd.DataFrame(pares)

display(pares_produtos.head(5))

# COMMAND ----------

# verificar os pares ex cartao/seguros, seguros/cartao, cartao/invest etc.
# groupby na pares_produtos trazendo clientes unicos por prod origem e prod destino, com sort asc 
resumo_pares = (
    pares_produtos
    .groupby(['produto_origem', 'produto_destino'], as_index=False)
    .agg(clientes=('cliente_id', 'nunique'))
    .sort_values('clientes', ascending=False)
)

display(resumo_pares)

# Quais produtos geralmente são contratados depois de outro?
# O produto mais contratado é o cartão, os produtos mais contratados com cartão como origem -> são cartão -> credito 389 clientes, 25% do total (389/1576), cartão -> investimento 314 clientes, 20% e cartão -> seguro 250 clientes, 16%.
# O interessante dessa visualização é perceber que crédito -> investimentos 96 clientes 6% e investimentos 92 clientes 6% -> crédito possuem boa relação quando sao produtos de origem, fortalecendo a ideia do cross-sell entre eles, além do cartão.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### ⚪ Resposta 04:
# MAGIC > Quais produtos geralmente são contratados depois de outro?
# MAGIC
# MAGIC O cartão aparece como principal produto de origem na jornada de contratação, os produtos mais contratados com cartão como origem são:
# MAGIC - cartão -> credito 389 clientes, 37% de cartão (389/1040) e 25% do total (389/1.576), 
# MAGIC - cartão -> investimento 314 clientes, 30% de cartão e 20% do total,
# MAGIC - cartão -> seguro 250 clientes, 24% de cartão e 16% do total.
# MAGIC
# MAGIC Após os 3 principais, 
# MAGIC - crédito -> investimentos 96 clientes, 53% de crédito e 6% do total
# MAGIC - investimentos -> crédito 92 clientes, 53% de investimentos e 6% do total
# MAGIC
# MAGIC Crédito e investimentos possuem boa relação quando sao produtos de origem, fortalecendo a ideia do cross-sell entre eles, além do cartão.
# MAGIC
# MAGIC > Com base nesse padrão, se um cliente possui atualmente apenas alguns produtos, qual produto deveríamos recomendar como próximo passo natural da jornada dele?
# MAGIC
# MAGIC Com base apenas nessa tabela, deveria ser recomendado na ordem: 
# MAGIC Cartão: Crédito > investimento > seguro > consorcio
# MAGIC Crédito: investimentos > seguros > consorcio > cartão
# MAGIC Investimento: Crédito > seguro > consorcio > cartão
# MAGIC Consórcio: Crédito > investimento > seguro > cartão
# MAGIC Seguros: Crédito > investimento > consorcio > cartão
# MAGIC
# MAGIC Cartão aparece por ultimo por ser o produto de entrada, já que não foi o produto pelo qual o cliente buscou o banco, seguindo essa lógica é possível que não seja o melhor produto para cross-sell.
# MAGIC
# MAGIC Com mais tempo de estudo e análise, seria possível aprofundar mais usando recursos de estatistica, segmentacoes por perfil de cliente, renda, tempo de relacionamento e receita gerada para definir com mais certeza as possibilidades de cross-sell.

# COMMAND ----------

# Olá, formulei as ideias do que queria seguir e usei ia (gpt5.5 e o genie do databricks) para a escrita dos codigos, ultimamente trabalhei mais com power query do que sql/python, acredito que o principal seja a ideia para chegar nos resultados.
# Priorizei usar a ia como ferramenta, como as ferramentas de point and click, ao inves de montar um groupby/merge na mão. 

# Obrigado pelo tempo!

# COMMAND ----------

# MAGIC %md
# MAGIC ---