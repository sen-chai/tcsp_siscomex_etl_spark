import pyspark.sql.functions as F


def generate_receita_fatos(spark):
    # * carregar dimensoes
    receita_tipo = spark.read.load('./data/receita_tipo')
    receita_fonte = spark.read.load('./data/receita_fonte')
    municipio = spark.read.load('./data/municipio')
    data = spark.read.load('./data/data')

    # * produzir tabela de fatos

    _receita = spark.read.options(
        sep=";", inferSchema=True, header=True,
        encoding="iso-8859-1", locale="pt").csv('./datalake/receitas')

    receita = (
        _receita.select(
            'ano_exercicio',
            'mes_referencia',
            'ds_municipio',
            'vl_arrecadacao',
            F.regexp_replace('vl_arrecadacao', ",", ".").cast(
                'double').alias("valor_arrecadacao"),
            F.split(F.col('ds_alinea'), ' - ').getItem(0).alias('alinea'),
            F.split(F.col('ds_subalinea'),
                    ' - ').getItem(0).alias('subalinea'),
            F.split(F.col('ds_fonte_recurso'),
                    ' - ').getItem(0).alias('fonte_recurso'),
            F.split(F.col('ds_cd_aplicacao_fixo'),
                    ' - ').getItem(0).alias('aplicacao_fixo'),
        )
    )

    receita.createOrReplaceTempView("receita")
    receita_fonte.createOrReplaceTempView("fonte")
    receita_tipo.createOrReplaceTempView("tipo")
    municipio.createOrReplaceTempView("municipio")
    data.createOrReplaceTempView("data")

    fatos = spark.sql("""
            select  municipio_pk, data_pk, receita_fonte_pk, receita_tipo_pk, valor_arrecadacao

            from receita r 

            left join municipio m
                on r.ds_municipio <=> m.nome_municipio_minusculo

            left join data d
            on r.ano_exercicio <=> d.ano and
                r.mes_referencia <=> d.mes_numero

            left join tipo t
            on  r.alinea <=> t.cd_alinea and 
                r.subalinea <=> t.cd_subalinea

            left join fonte f
            on r.fonte_recurso <=> f.cd_fonte_recurso and 
                r.aplicacao_fixo <=> f.cd_aplicacao_fixo
            
        """)

    fatos = fatos.na.drop(
        subset=["municipio_pk", "data_pk",
                "receita_fonte_pk", "receita_tipo_pk"])

    fatos.write.mode('overwrite').save('./data/receita_fato')

    return fatos
