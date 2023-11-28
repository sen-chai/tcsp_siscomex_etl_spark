import pyspark.sql.functions as F

def generate_municipio(spark):

    uf_mun = spark.read.csv(
        "./datalake/UF_MUN.csv",
        inferSchema=True, sep=';', header=True, encoding='iso-8859-1', locale='pt')
    uf = spark.read.csv(
        "./datalake/UF.csv",
        inferSchema=True, sep=';', header=True, encoding='iso-8859-1', locale='pt')

    municipio = uf.join(uf_mun, on='SG_UF', how='inner')

    municipio = municipio.selectExpr(
        "CO_MUN_GEO as cd_municipio",
        "NO_REGIAO as nome_regiao",
        "SG_UF as sigla_uf",
        "NO_UF as nome_uf",
        "NO_MUN as nome_municipio",
        "NO_MUN_MIN nome_municipio_minusculo")

    municipio = municipio.select(
        F.monotonically_increasing_id().alias("municipio_pk"), "*")

    municipio.write.mode('overwrite').save("./data/municipio")

    print(municipio.columns)
    return municipio

