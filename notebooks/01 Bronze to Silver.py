# Databricks notebook source
#As informações sobre os vôos vem em formato csv em varios arquivos, aqui estou lendo todos os arquivos da pasta e fazendo o merge deles em um só

caminho_pasta = "dbfs:/FileStore/tables/AVProj/bronze/"

arquivos = dbutils.fs.ls(caminho_pasta)

df_resultado = None
for arquivo in arquivos:
    if arquivo.name.startswith("VRA") and arquivo.name.endswith(".csv"):
        df_temp = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").load(arquivo.path)
        df_resultado = df_temp if df_resultado is None else df_resultado.unionByName(df_temp)

# COMMAND ----------

df_resultado.filter("`Referência` like '2024-03-%'").display()

# COMMAND ----------

#Dropando colunas desnecessárias

df_resultado = df_resultado.drop('Código DI', 'Código Tipo Linha' )

# COMMAND ----------

#Removendo vôos onde não se tem informações sobre Partida e Chegada e voos cargueiros(assentos=0)

df_resultado = df_resultado.filter("`Partida Prevista` is not null and `Partida Real` is not null and `Chegada Prevista` is not null and `Chegada Real` is not null and `Número de Assentos` > 0")

df_resultado.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#Normalizando a coluna de Situação para as linhas que tem Atraso% fique somente com "Atraso"

df_resultado = df_resultado.withColumn('Situação Partida2', expr("case when `Situação Partida` like 'Atraso%' then 'Atraso'  when `Situação Partida` like 'Antecipado%' then 'Pontual' else `Situação Partida` end"))

df_resultado = df_resultado.withColumn('Situação Chegada2', expr("case when `Situação Chegada` like 'Atraso%' then 'Atraso' when `Situação Chegada` like 'Antecipado%' then 'Pontual' else `Situação Chegada` end"))

# COMMAND ----------

#Dropando as antigas colunas

df_resultado = df_resultado.drop('Situação Partida', 'Situação Chegada')

# COMMAND ----------

#Renomeando as novas colunas

df_resultado = df_resultado.withColumnRenamed('Situação Partida2', 'Situação Partida').withColumnRenamed('Situação Chegada2','Situação Chegada')

# COMMAND ----------

df_resultado.display()

# COMMAND ----------

#Salvando os dados em parquet

df_resultado.write.parquet("dbfs:/FileStore/tables/AVProj/silver/VRA", mode="overwrite")

# COMMAND ----------

#Lendo a lista de aeródromos brasileiros

df_aerodromos = spark.read.csv("dbfs:/FileStore/tables/AVProj/bronze/pda_aerodromos_publicos_caracteristicas_gerais.csv", header=True, sep=",", inferSchema=True)

# COMMAND ----------

#Selecionando colunas de interesse apenas
df_aerodromos.select('Id do Aeródromo','Código OACI','Nome','Município','UF','País','Latitude','Longitude').display()

# COMMAND ----------

#Função para converter as coordenadas do formato DMS para o formato DD (decimal) - formato que o Databricks entende.

from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType

# Função para converter DMS para decimal
def dms_to_decimal(dms):
    if dms is None:
        return None
    dms = dms.replace('°', ' ').replace("'", ' ').replace('"', ' ')
    parts = dms.split()
    degrees = float(parts[0])
    minutes = float(parts[1])
    seconds = float(parts[2].replace(',', '.'))
    direction = parts[3]
    
    # Calcular valor decimal
    decimal = degrees + minutes / 60 + seconds / 3600
    if 'S' in direction or 'W' in direction:
        decimal *= -1  # Negar para S e W
    return decimal

# Registrar UDF
dms_to_decimal_udf = udf(dms_to_decimal, FloatType())

# Aplicar a função de conversão
df_aerodromos_converted = df_aerodromos.withColumn("Latitude_decimal", dms_to_decimal_udf(col("Latitude"))) \
                                       .withColumn("Longitude_decimal", dms_to_decimal_udf(col("Longitude")))

# Mostrar o resultado
display(df_aerodromos_converted)

# COMMAND ----------

#Somente dados úteis, e filtrando para somente aeródromos com código ICAO

df_aerodromos_converted = df_aerodromos_converted.select('Id do Aeródromo','Código OACI','Nome','Município','UF','País','Latitude_decimal','Longitude_decimal').filter("`Código OACI` is not null")

# COMMAND ----------

#Salvando os dados em parquet

df_aerodromos_converted.write.parquet("dbfs:/FileStore/tables/AVProj/silver/aerodromos", mode="overwrite")
