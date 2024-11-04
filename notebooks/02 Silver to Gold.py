# Databricks notebook source
df_voos = spark.read.parquet("dbfs:/FileStore/tables/AVProj/silver/VRA")
df_aerodromos = spark.read.parquet("dbfs:/FileStore/tables/AVProj/silver/aerodromos")

# COMMAND ----------

from pyspark.sql.functions import *

df_aerodromos.dtypes


# COMMAND ----------

df_voos.display()
df_aerodromos.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Filtra apenas as empresas com mais de 100 voos registrados
df_voos_filtrado = (
    df_voos
    .groupBy("Sigla ICAO Empresa Aérea", "Empresa Aérea")
    .agg(
        F.count("*").alias("Total de Voos"),
        F.sum(F.when(F.col("Situação Partida") == "Pontual", 1).otherwise(0)).alias("Total Pontual"),
        F.sum(F.when(F.col("Situação Partida") == "Atraso", 1).otherwise(0)).alias("Total Atraso"),
        F.sum(F.when(F.col("Situação Partida") == "Antecipado", 1).otherwise(0)).alias("Total Antecipado")
    )
    .filter(F.col("Total de Voos") > 100)
)

# Calcula os percentuais
df_voos_gold = (
    df_voos_filtrado
    .withColumn("Percentual Pontual", F.col("Total Pontual") / F.col("Total de Voos") * 100)
    .withColumn("Percentual Atraso", F.col("Total Atraso") / F.col("Total de Voos") * 100)
    .withColumn("Percentual Antecipado", F.col("Total Antecipado") / F.col("Total de Voos") * 100)
)

# Seleciona o top 5 mais pontuais e top 5 com mais atrasos
top_5_pontuais = df_voos_gold.orderBy(F.desc("Percentual Pontual")).limit(5)
top_5_atrasos = df_voos_gold.orderBy(F.desc("Percentual Atraso")).limit(5)

# Exibe ou salva como tabela Gold
display(top_5_pontuais)
display(top_5_atrasos)


# COMMAND ----------

#Salvando os dados em parquet

top_5_atrasos.write.parquet("dbfs:/FileStore/tables/AVProj/gold/top_5_atrasos", mode="overwrite")
top_5_pontuais.write.parquet("dbfs:/FileStore/tables/AVProj/gold/top_5_pontuais", mode="overwrite")

# COMMAND ----------

# Agrupa os dados de voos por aeroporto e calcula pontualidade
df_aeroporto_agg = (
    df_voos
    .groupBy("Sigla ICAO Aeroporto Origem", "Descrição Aeroporto Origem")
    .agg(
        F.count("*").alias("Total de Voos"),
        F.sum(F.when(F.col("Situação Partida") == "Pontual", 1).otherwise(0)).alias("Total Pontual"),
        F.sum(F.when(F.col("Situação Partida") == "Atraso", 1).otherwise(0)).alias("Total Atraso"),
        F.sum(F.when(F.col("Situação Partida") == "Antecipado", 1).otherwise(0)).alias("Total Antecipado")
    )
    .filter(F.col("Total de Voos") > 100)
)

# Calcula os percentuais
df_aeroporto_gold = (
    df_aeroporto_agg
    .withColumn("Percentual Pontual", F.col("Total Pontual") / F.col("Total de Voos") * 100)
    .withColumn("Percentual Atraso", F.col("Total Atraso") / F.col("Total de Voos") * 100)
    .withColumn("Percentual Antecipado", F.col("Total Antecipado") / F.col("Total de Voos") * 100)
)

# Join com df_aerodromos para obter latitude e longitude
df_aeroporto_gold = (
    df_aeroporto_gold
    .join(df_aerodromos, df_aeroporto_gold["Sigla ICAO Aeroporto Origem"] == df_aerodromos["Código OACI"], "left")
    .select(
        "Sigla ICAO Aeroporto Origem",
        "Descrição Aeroporto Origem",
        "Total de Voos",
        "Percentual Pontual",
        "Percentual Atraso",
        "Percentual Antecipado",
        "Latitude_decimal",
        "Longitude_decimal"
    )
    .filter(F.col("País")=="Brasil")
)

# Seleciona o top 5 mais pontuais e top 5 com mais atrasos
top_5_pontuais_aeroporto = df_aeroporto_gold.orderBy(F.desc("Percentual Pontual")).limit(5)
top_5_atrasos_aeroporto = df_aeroporto_gold.orderBy(F.desc("Percentual Atraso")).limit(5)

# Exibe ou salva como tabela Gold
display(top_5_pontuais_aeroporto)
display(top_5_atrasos_aeroporto)


# COMMAND ----------

#Salvando os dados em parquet

top_5_pontuais_aeroporto.write.parquet("dbfs:/FileStore/tables/AVProj/gold/top_5_pontuais_aeroporto", mode="overwrite")
top_5_atrasos_aeroporto.write.parquet("dbfs:/FileStore/tables/AVProj/gold/top_5_atrasos_aeroporto", mode="overwrite")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Converte as colunas "Partida Real" e "Chegada Real" para timestamp e extrai a hora
df_voos_hora = (
    df_voos
    .withColumn("Partida Real Timestamp", F.to_timestamp("Partida Real", "dd/MM/yyyy HH:mm"))
    .withColumn("Chegada Real Timestamp", F.to_timestamp("Chegada Real", "dd/MM/yyyy HH:mm"))
    .withColumn("Hora Partida", F.hour("Partida Real Timestamp"))
    .withColumn("Hora Chegada", F.hour("Chegada Real Timestamp"))
)

# Agrupa por hora e calcula o total de voos e o total de atrasos
df_hora_agg = (
    df_voos_hora
    .groupBy("Hora Partida")
    .agg(
        F.count("*").alias("Total de Voos"),
        F.sum(F.when(F.col("Situação Partida") == "Atraso", 1).otherwise(0)).alias("Total Atrasos")
    )
)

# Calcula o percentual de atraso
df_hora_gold = (
    df_hora_agg
    .withColumn("Percentual de Atraso", F.col("Total Atrasos") / F.col("Total de Voos") * 100)
)

# Exibe ou salva como tabela Gold
df_hora_gold.orderBy(df_hora_gold['Hora Partida'].asc()).display()

df_hora_gold.write.parquet("dbfs:/FileStore/tables/AVProj/gold/df_hora_gold", mode="overwrite")


# COMMAND ----------

# Agrupa os dados de voos por modelo de equipamento e calcula pontualidade
df_modelo_agg = (
    df_voos
    .groupBy("Modelo Equipamento")
    .agg(
        F.count("*").alias("Total de Voos"),
        F.sum(F.when(F.col("Situação Partida") == "Pontual", 1).otherwise(0)).alias("Total Pontual"),
        F.sum(F.when(F.col("Situação Partida") == "Atraso", 1).otherwise(0)).alias("Total Atraso"),
        F.sum(F.when(F.col("Situação Partida") == "Antecipado", 1).otherwise(0)).alias("Total Antecipado")
    )
)

# Calcula os percentuais
df_modelo_gold = (
    df_modelo_agg
    .withColumn("Percentual Pontual", F.col("Total Pontual") / F.col("Total de Voos") * 100)
    .withColumn("Percentual Atraso", F.col("Total Atraso") / F.col("Total de Voos") * 100)
)

# Exibe ou salva como tabela Gold
display(df_modelo_gold)


# COMMAND ----------

df_modelo_gold.write.parquet("dbfs:/FileStore/tables/AVProj/gold/df_modelo_gold", mode="overwrite")

# COMMAND ----------

# Agrupa os dados por data e calcula o total de voos e atrasos
df_data_agg = (
    df_voos
    .groupBy("Referência")
    .agg(
        F.count("*").alias("Total de Voos"),
        F.sum(F.when(F.col("Situação Partida") == "Atraso", 1).otherwise(0)).alias("Total Atrasos")
    )
)

# Calcula o percentual de atraso
df_data_gold = (
    df_data_agg
    .withColumn("Percentual de Atraso", F.col("Total Atrasos") / F.col("Total de Voos") * 100)
)

# Exibe ou salva como tabela Gold
display(df_data_gold)


# COMMAND ----------

df_data_gold.write.parquet("dbfs:/FileStore/tables/AVProj/gold/df_data_gold", mode="overwrite")
