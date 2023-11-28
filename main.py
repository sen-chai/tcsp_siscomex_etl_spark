from pyspark.sql import SparkSession

from comex_fatos import generate_importacao_exportacao
from data import generate_data
from municipio import generate_municipio
from pais import generate_pais
from receita_fatos import generate_receita_fatos
from receita_fonte_tipo import generate_receita_tipo_fonte
from tipo_bem import generate_tipo_bem

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Processamento Analitico Main") \
    .getOrCreate()

# %% chamada para geração das dimensões
data = generate_data(spark)
municipio = generate_municipio(spark)
pais = generate_pais(spark)
tipo_bem = generate_tipo_bem(spark)

# %% receitas municipais

print("iniciando receita tipo e fonte")
receita_tipo, receita_fonte = generate_receita_tipo_fonte(spark)
print("iniciando receita municipais fatos")
receita = generate_receita_fatos(spark)

print("Fatos Receitas Municipais geradas")


# %% comercio exterior
print("iniciando importacao exportacao fatos")
importacao, exportacao = generate_importacao_exportacao(spark)

print("Fatos Comex gerado com sucesso")
