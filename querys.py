# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[4]') \
    .appName("query_receitas_municipais") \
    .config('spark.ui.port', '4050') \
    .getOrCreate()


receita_tipo = spark.read.load('./data/receita_tipo')
receita_fonte = spark.read.load('./data/receita_fonte')
data = spark.read.load('./data/data')
municipio = spark.read.load('./data/municipio')
tipo_bem = spark.read.load('./data/tipo_bem')
pais = spark.read.load('./data/pais')

receita = spark.read.load('./data/receita_fato')
importacao = spark.read.load('./data/importacao_mun')
exportacao = spark.read.load('./data/exportacao_mun')

receita_fonte.createOrReplaceTempView("fonte")
receita_tipo.createOrReplaceTempView("tipo")
municipio.createOrReplaceTempView("municipio")
data.createOrReplaceTempView("data")
tipo_bem.createOrReplaceTempView("tipo_bem")
pais.createOrReplaceTempView("pais")

receita.createOrReplaceTempView("receita")
importacao.createOrReplaceTempView("importacao")
exportacao.createOrReplaceTempView("exportacao")


# %%
# ? checar nulos na tabela de fatos receitas
spark.sql("""
    select count(*)
    from receita
    where
        municipio_pk is null or 
        data_pk is null or
        receita_tipo_pk is null or
        receita_fonte_pk is null or
        valor_arrecadacao is null
""").toPandas()

# %%
# ? checar nulos na tabela de fatos comex
spark.sql("""
    select count(*)
    from importacao 
    where
        data_pk is null or
        pais_pk is null or
        tipo_bem_pk is null or
        kg_liquido is null or
        vl_fob is null
""").toPandas()
# %%
# ? Roll Up: Qual e o valor total arrecadado por Sao Carlos ao longo dos anos?
rollup1 = spark.sql("""
    select d.ano, cast(sum(valor_arrecadacao) as INT) as Arrecadado
    from receita r 
    join municipio m
        on r.municipio_pk = m.municipio_pk
    join data d
        on r.data_pk =  d.data_pk

    where m.nome_municipio = 'SAO CARLOS' and
        m.sigla_uf = "SP"
    group by d.ano 
    order by d.ano
""").toPandas()
rollup1.to_parquet("./query_output/rollup1.panda_parquet")
rollup1
# %%
# ? Roll Up: Qual o peso bruto exportado pelo município de são carlos, para cada tipo de bem na categoria secção ao longo dos anos?
# *  Nas seguintes categorias:
# * I Animais vivos e produtos do reino animal
# * II Produtos do reino vegetal
# * XVI Máquinas e aparelhos, material elétrico

rollup2 = spark.sql("""
    select 
        ano as Ano,
        cd_seccao as `Código Secção`,
        ds_seccao as `Secção SH` ,
        SUM(vl_fob) as `Valor FOB (R$) Exportado`
    from exportacao e
    join municipio m
        on m.municipio_pk = e.municipio_pk
    join tipo_bem t
        on t.tipo_bem_pk = e.tipo_bem_pk
    join data d
        on d.data_pk = e.data_pk
    where 
        nome_municipio = "SAO CARLOS" and sigla_uf = "SP" and
        cd_seccao = "XVI"

    -- group by rollup (ano,(cd_seccao,ds_seccao))
    group by ano,cd_seccao,ds_seccao
    order by ano
""").toPandas()
rollup2.to_parquet("./query_output/rollup2.panda_parquet")
rollup2
# %%
# ? Exploração Slice and Dice:
# municipio de sao carlos?
spark.sql(" select * from municipio where nome_municipio = 'SAO CARLOS'").toPandas()
# %%
# tipos diferentes secoes de bens
spark.sql(
    " select distinct(ds_seccao),cd_seccao,tipo_bem_pk from tipo_bem").toPandas()
# %%
# podemos fazer a consulta sem fazer join se soubermos a chave artificial que queremos (nao recomendado, mas é justamente isso que o join faz para nós)
# spark.sql(" select * from importacao where tipo_bem_pk = 184 and municipio_pk = '3146'").toPandas()
# spark.sql(" select * importacao where municipio_pk = '3146'").toPandas()

# %%
# * encontrar produto adequado para pesquisa .
# ? Slice and Dice: Para um dado produto contido na dimensão "Tipo Bem" e para um dado município (sao carlos), qual é o valor importado por ano?
spark.sql("""
    select 
        cd_seccao,ds_seccao,sum(vl_fob)
    from importacao i
        join tipo_bem t on t.tipo_bem_pk = i.tipo_bem_pk
        join municipio m on m.municipio_pk = i.municipio_pk
    where 
        m.nome_municipio = 'SAO CARLOS' and m.sigla_uf = 'SP'
    group by cd_seccao,ds_seccao
    order by sum(vl_fob) desc
""").toPandas()
# .to_csv("./trash/produtos.csv")
# %%
# ? Slice and Dice: Importação de Aparelhos e dispositivos para lançamento de veı́culos aéreos para São Carlos
slice_dice = spark.sql("""
    select 
        ano as Ano, 
        sum(vl_fob) as `Valor Importado por Ano R$ Aparelhos e dispositivos para lançamento de veículos aéreos`

    from importacao i
        join tipo_bem t on t.tipo_bem_pk = i.tipo_bem_pk
        join data d on d.data_pk = i.data_pk
        join municipio m on m.municipio_pk = i.municipio_pk
    where 
        m.nome_municipio = 'SAO CARLOS' and m.sigla_uf = 'SP' and
        cd_sh4 = 8805
        
    group by ano, cd_sh4,ds_sh4
    order by ano
""").toPandas()
slice_dice
# slice_dice.to_parquet("./query_output/slice_dice.panda_parquet")
# %%
# ? Pivot: Qual e o valor arrecado por tipo de receita fonte, para o ano de 2021, para a cidade de Sao Carlos?

pivot = spark.sql("""
    select  t.cd_fonte,t.ds_fonte, cast(sum(valor_arrecadacao) as DECIMAL(20,2))
        as `Arrecadado em São Carlos em 2021`
    from receita r 

    join municipio m
        on r.municipio_pk = m.municipio_pk
    join data d
        on r.data_pk =  d.data_pk
    join tipo t
        on r.receita_tipo_pk = t.receita_tipo_pk

    where m.nome_municipio = 'SAO CARLOS' and
        m.sigla_uf = "SP" and
        d.ano = 2021

    group by t.cd_fonte,t.ds_fonte
""").toPandas()
pivot
# pivot.to_parquet('./query_output/pivot.panda_parquet')
# %%
# ? Drill Across: qual é o saldo da balança comercial por ano para o município de são carlos?

drill_across = spark.sql("""
    select imp_ano as Ano , 
    exportado as `Exportado R$` ,
    importado as `Importado R$`, 
    exportado - importado as `Balança Comercial R$`

    from (
        select ano,SUM(vl_fob)
        from importacao i
        join municipio m
            on m.municipio_pk = i.municipio_pk
        join data d
            on d.data_pk = i.data_pk
        where nome_municipio = 'SAO CARLOS' and sigla_uf = 'SP'
        group by ano
    ) as imp(imp_ano, importado)
    join 
    (
        select ano, SUM(vl_fob)
        from exportacao e
        join municipio m
            on m.municipio_pk = e.municipio_pk
        join data d
            on d.data_pk = e.data_pk
        where nome_municipio = 'SAO CARLOS' and sigla_uf = 'SP'
        group by ano
    ) as exp(exp_ano,exportado)

    on imp_ano = exp_ano
    order by imp_ano
""").toPandas()
drill_across.to_parquet("./query_output/drill_across.panda_parquet")
# %%

# ? Pergunta extra: Quais sao os tipos de produto que Sao Carlos mais importa?
# spark.sql()
