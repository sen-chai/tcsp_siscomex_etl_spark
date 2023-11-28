import pyspark.sql.functions as F


def generate_data(spark):
    meses_nome = ['Janeiro', 'Fevereiro', 'Marco', 'Abril', 'Maio', 'Junho',
                  'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    meses_numero = range(1, 13)

    bimestre = [f"{i} bimestre" for i in range(1, 5) for j in range(3)]
    trimestre = [f"{i} trimestre " for i in range(1, 4) for j in range(4)]
    semestre = [f"{i} semestre" for i in range(1, 3) for j in range(6)]

    mes_periodo_schema = "semestre STRING, bimestre STRING,trimestre STRING,mes_nome STRING, mes_numero INT"

    meses_periodos = spark.createDataFrame(
        data=zip(semestre, bimestre, trimestre, meses_nome, meses_numero,), schema=mes_periodo_schema)

    meses_periodos.toPandas()
    anos = spark.range(1900, 2101).withColumnRenamed("id", "ano")

    # cross join para gerar todos os meses do escopo do ano
    data = anos.crossJoin(meses_periodos).orderBy("ano")

    data = data.select(F.monotonically_increasing_id().alias("data_pk"), "*")
    data.write.mode("overwrite").save("./data/data")

    data.printSchema()
    return data
