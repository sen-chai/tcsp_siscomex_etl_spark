import pyspark.sql.functions as F


def generate_receita_tipo_fonte(spark):
    dirname = './datalake/receitas'

    receitas = spark.read.options(
        sep=";", inferSchema=True, header=True,
        encoding="iso-8859-1", locale="pt").csv(dirname)

    receita_fonte = _generate_receita_fonte(receitas)
    receita_tipo = _generate_receita_tipo(receitas)
    print(receita_fonte.columns)
    print(receita_tipo.columns)

    return receita_tipo, receita_fonte


def _generate_receita_tipo(receitas):
    targets = (
        'ds_categoria',
        'ds_subcategoria',
        'ds_fonte',
        'ds_rubrica',
        'ds_alinea',
        'ds_subalinea',
    )

    expr = split_targets(targets)
    receita_tipo = eval(expr)
    receita_tipo = receita_tipo.distinct()
    receita_tipo = receita_tipo.select(
        F.monotonically_increasing_id().alias('receita_tipo_pk'), "*")

    receita_tipo.write.mode('overwrite').save('./data/receita_tipo')

    return receita_tipo


def _generate_receita_fonte(receitas):
    targets = (
        'ds_fonte_recurso',
        'ds_cd_aplicacao_fixo',
    )
    receita_fonte = (
        receitas.select(
            F.split(F.col('ds_fonte_recurso'),
                    ' - ').getItem(0).alias('cd_fonte_recurso'),
            F.split(F.col('ds_fonte_recurso'),
                    ' - ').getItem(1).alias('ds_fonte_recurso'),
            F.split(F.col('ds_cd_aplicacao_fixo'),
                    ' - ').getItem(0).alias('cd_aplicacao_fixo'),
            F.split(F.col('ds_cd_aplicacao_fixo'),
                    ' - ').getItem(1).alias('ds_aplicacao_fixo'),
        )
    )
    receita_fonte = receita_fonte.distinct()
    receita_fonte = receita_fonte.select(
        F.monotonically_increasing_id().alias('receita_fonte_pk'), "*")

    receita_fonte.write.mode('overwrite').save('./data/receita_fonte')
    return receita_fonte


def split_targets(targets):
    _extracts = []
    for target in targets:
        for num in range(0, 2):
            if num == 0:
                if 'cd_' in target:
                    alias = target.replace('ds_', '')
                alias = target.replace('ds_', 'cd_')
            else:
                alias = target
            insert = f"F.split(F.col('{target}'), ' - ').getItem({num}).alias('{alias}')"
            _extracts.append(insert)

    expr = f"(receitas.select({','.join(_extracts)},))"
    return expr
