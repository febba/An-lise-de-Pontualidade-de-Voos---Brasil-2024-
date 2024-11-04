# Databricks notebook source
df_modelo_gold = spark.read.parquet("dbfs:/FileStore/tables/AVProj/gold/df_modelo_gold")
df_data_gold = spark.read.parquet("dbfs:/FileStore/tables/AVProj/gold/df_data_gold")
df_hora_gold = spark.read.parquet("dbfs:/FileStore/tables/AVProj/gold/df_hora_gold")
top_5_atrasos = spark.read.parquet("dbfs:/FileStore/tables/AVProj/gold/top_5_atrasos")
top_5_atrasos_aeroporto = spark.read.parquet("dbfs:/FileStore/tables/AVProj/gold/top_5_atrasos_aeroporto")
top_5_pontuais = spark.read.parquet("dbfs:/FileStore/tables/AVProj/gold/top_5_pontuais")
top_5_pontuais_aeroporto = spark.read.parquet("dbfs:/FileStore/tables/AVProj/gold/top_5_pontuais_aeroporto")

# COMMAND ----------

displayHTML("""<h1 style="text-align: center; font-size: 32px; color: #2A3F54;">Análise de Pontualidade de Voos - Brasil (2024)</h1>""")


# COMMAND ----------

displayHTML("""<h3 style="text-align: center; font-size: 25px; color: #2A3F54;">Companhias Aéreas</h3>""")

# COMMAND ----------

display(top_5_atrasos)

# COMMAND ----------

display(top_5_pontuais)

# COMMAND ----------

displayHTML("""<h3 style="text-align: center; font-size: 25px; color: #2A3F54;">Aeródromos</h3>""")

# COMMAND ----------

displayHTML("""<h3 style="text-align: center; font-size: 20px; color: #2A3F54;">TOP 5 AERÓDROMOS COM MAIOR PONTUALIDADE</h3>""")

# COMMAND ----------

from pyspark.sql.functions import round, col

display(
    top_5_pontuais_aeroporto.select(
        'Sigla ICAO Aeroporto Origem',
        'Descrição Aeroporto Origem',
        'Total de Voos',
        round(col('Percentual Pontual') / 100, 4).alias('Percentual Pontual'),
        'Latitude_decimal',
        'Longitude_decimal'
    )
)

# COMMAND ----------

display(top_5_pontuais_aeroporto)

# COMMAND ----------

displayHTML("""<h3 style="text-align: center; font-size: 20px; color: #2A3F54;">TOP 5 AERÓDROMOS COM MAIOR ATRASO</h3>""")

# COMMAND ----------

display(top_5_atrasos_aeroporto)

# COMMAND ----------

from pyspark.sql.functions import round, col

display(
    top_5_atrasos_aeroporto.select(
        'Sigla ICAO Aeroporto Origem',
        'Descrição Aeroporto Origem',
        'Total de Voos',
        round(col('Percentual Atraso') / 100, 4).alias('Percentual Atraso'),
        'Latitude_decimal',
        'Longitude_decimal'
    )
)

# COMMAND ----------

displayHTML("""<h3 style="text-align: center; font-size: 20px; color: #2A3F54;">PONTUALIDADE POR AERONAVES</h3>""")

# COMMAND ----------

display(df_modelo_gold)

# COMMAND ----------

displayHTML("""<h3 style="text-align: center; font-size: 20px; color: #2A3F54;">MELHORES MOMENTOS PARA VIAJAR</h3>""")

# COMMAND ----------

display(df_data_gold)
df_data_gold.dtypes

# COMMAND ----------


display(df_data_gold)

# COMMAND ----------

from pyspark.sql import functions as F


df_dia_da_semana = df_data_gold.select(
    date_format(col('Referência'), 'EEEE').alias('Dia da Semana'),  
    'Total de Voos',
    'Total Atrasos',
    round(col('Percentual de Atraso'), 4).alias("Percentual Atraso")
).withColumn('Dia da Semana Ordinal', 
    F.expr(f"CASE WHEN `Dia da Semana` = 'Sunday' THEN 0 " +
           "WHEN `Dia da Semana` = 'Monday' THEN 1 " +
           "WHEN `Dia da Semana` = 'Tuesday' THEN 2 " +
           "WHEN `Dia da Semana` = 'Wednesday' THEN 3 " +
           "WHEN `Dia da Semana` = 'Thursday' THEN 4 " +
           "WHEN `Dia da Semana` = 'Friday' THEN 5 " +
           "WHEN `Dia da Semana` = 'Saturday' THEN 6 END")
).orderBy('Dia da Semana Ordinal')  # Ordenar pelo valor numérico


display(df_dia_da_semana.select('Dia da Semana', 'Total de Voos', 'Total Atrasos', 'Percentual Atraso'))


# COMMAND ----------

from pyspark.sql.functions import *

df_hora_gold_ord = df_hora_gold.select(
    'Hora Partida',
    'Total de Voos',
    'Total Atrasos',
    round(col('Percentual de Atraso') / 1, 4).alias("Percentual Atraso")
).orderBy(1)


display(df_hora_gold_ord.select(
    concat(col('Hora Partida').cast("string"), lit(':00')).alias('Hora Partida'),
    'Total de Voos',
    'Total Atrasos',
    'Percentual Atraso'
))

# COMMAND ----------


