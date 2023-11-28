
def generate_tipo_bem(spark):
    # * esquema necessáio, caso contrário sh4 com valor zero ficará armazenado como inteiro,
    # * definição de esquema é útil para detectar inconsistencias
    tipo_bem_schema = """
        CO_SH6 string,
        NO_SH6_POR string,
        NO_SH6_ESP string,
        NO_SH6_ING string,
        CO_SH4 string,
        NO_SH4_POR string,
        NO_SH4_ESP string,
        NO_SH4_ING string,
        CO_SH2 string,
        NO_SH2_POR string,
        NO_SH2_ESP string,
        NO_SH2_ING string,
        CO_NCM_SECROM string,
        NO_SEC_POR string,
        NO_SEC_ESP string,
        NO_SEC_ING string
    """

    tipo_bem = spark.read.schema(tipo_bem_schema).options(
        sep=";", header=True,
        encoding="iso-8859-1", locale="pt", escape='"').csv('./datalake/NCM_SH.csv')

    tipo_bem = tipo_bem.drop_duplicates(["CO_SH4"])

    tipo_bem = tipo_bem.selectExpr(
        "monotonically_increasing_id() as tipo_bem_pk",
        "CO_SH4 as cd_sh4",
        "NO_SH4_POR as ds_sh4",
        "NO_SH4_ING as ds_sh4_ingles",
        "CO_SH2 as cd_sh2",
        "NO_SH2_POR as ds_sh2",
        "NO_SH2_ING as ds_sh2_ingles",
        "CO_NCM_SECROM as cd_seccao",
        "NO_SEC_POR as ds_seccao",
        "NO_SEC_ING as ds_seccao_ingles",
    )

    tipo_bem.write.mode('overwrite').save('./data/tipo_bem')

    return tipo_bem
