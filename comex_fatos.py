def generate_importacao_exportacao(spark):
    tipo_bem = spark.read.load('./data/tipo_bem')
    pais = spark.read.load('./data/pais')
    municipio = spark.read.load('./data/municipio')
    data = spark.read.load('./data/data')

    tipo_bem.createOrReplaceTempView("tipo_bem")
    pais.createOrReplaceTempView("pais")
    municipio.createOrReplaceTempView("municipio")
    data.createOrReplaceTempView("data")

    comex_schema = "CO_ANO int, CO_MES int, SH4 string, CO_PAIS string, SG_UF_MUN string, CO_MUN string, KG_LIQUIDO int , VL_FOB int"

    for imp_exp in ['importacao_mun', 'exportacao_mun']:
        comex = spark.read.schema(comex_schema).options(
            sep=";", header=True,
            encoding="iso-8859-1", locale="pt").csv(f'./datalake/{imp_exp}')

        comex = comex.selectExpr(
            "CO_ANO as ano",
            "CO_MES as mes_numero",
            "SH4 as cd_sh4",
            "CO_PAIS as cd_pais",
            "SG_UF_MUN as sigla_uf",
            "CO_MUN as cd_municipio",
            "KG_LIQUIDO as kg_liquido",
            "VL_FOB as vl_fob",
        )

        comex.createOrReplaceTempView("comex")

        fatos = spark.sql("""
            select tipo_bem_pk , pais_pk , data_pk, municipio_pk, kg_liquido, vl_fob
            from comex c
            left join tipo_bem t
                on t.cd_sh4 <=> c.cd_sh4

            left join pais p
                on p.cd_pais <=> c.cd_pais

            left join data d
                on d.ano <=> c.ano and
                    d.mes_numero <=> c.mes_numero

            left join municipio m
                on m.cd_municipio <=> c.cd_municipio
        """)
        fatos.printSchema()
        fatos.count()
        fatos.write.mode('overwrite').save(f'./data/{imp_exp}')

        if imp_exp == 'importacao_mun':
            importacao = fatos

        elif imp_exp == 'exportacao_mun':
            exportacao = fatos

    return importacao, exportacao
