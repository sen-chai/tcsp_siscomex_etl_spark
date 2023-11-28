import pyspark.sql.functions as F


def generate_pais(spark):
    pais = spark.read.options(
        sep=";", inferSchema=True, header=True,
        encoding="iso-8859-1", locale="pt").csv('./datalake/PAIS.csv')

    pais = pais.selectExpr(
        "CO_PAIS as cd_pais",
        "CO_PAIS_ISOA3 as cd_pais_isoa3",
        "NO_PAIS as nome_pais_portugues",
        "NO_PAIS_ING as nome_pais_ingles",
    )
    pais = pais.select(
        F.monotonically_increasing_id().alias("pais_pk"), "*")

    pais.write.mode('overwrite').save('./data/pais')
