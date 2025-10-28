# ==============================================================
#  Análisis en lote y tiempo real del tráfico vehicular en Bogotá
#  Autor: Tatiana Vanegas
# ==============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg, count, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# =======================================
# 1. Inicializar sesión Spark
# =======================================
spark = SparkSession.builder \
    .appName("AnalisisTraficoBogota") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =======================================
# 2. PROCESAMIENTO BATCH
# =======================================
ruta_csv = "data/trafico_bogota_historico.csv"

df_trafico = spark.read.option("header", True).option("inferSchema", True).csv(ruta_csv)

df_trafico = df_trafico.withColumn("fecha_hora", col("fecha_hora").cast(TimestampType()))
df_trafico = df_trafico.withColumn("velocidad_promedio", col("velocidad_promedio").cast(DoubleType()))
df_trafico = df_trafico.dropna(subset=["fecha_hora", "localidad", "velocidad_promedio"])

def clasificar_congestion(df, velocidad_col='velocidad_promedio'):
    """Clasifica el nivel de congestión según la velocidad promedio"""
    return df.withColumn(
        'nivel_congestion',
        when(df[velocidad_col] >= 30, 'Bajo')
        .when((df[velocidad_col] >= 15) & (df[velocidad_col] < 30), 'Medio')
        .otherwise('Alto')
    )

df_trafico = clasificar_congestion(df_trafico)

df_agrupado = df_trafico.groupBy("localidad", window("fecha_hora", "1 hour")) \
    .agg(
        avg("velocidad_promedio").alias("velocidad_media"),
        count("*").alias("total_registros")
    )

df_agrupado.write.mode("overwrite").parquet("output/trafico_bogota_batch.parquet")

print("Procesamiento batch completado. Datos guardados en carpeta 'output/'")

# =======================================
# 3. PROCESAMIENTO STREAMING (Kafka)
# =======================================
#schema_stream = StructType([
 #   StructField("fecha_hora", TimestampType(), True),
  #  StructField("localidad", StringType(), True),
   # StructField("velocidad_promedio", DoubleType(), True)
#])

#df_stream = spark.readStream \
  #  .format("kafka") \
   # .option("kafka.bootstrap.servers", "localhost:9092") \
    #.option("subscribe", "trafico_bogota") \
    #.option("startingOffsets", "latest") \
    #.load()

#df_stream_values = df_stream.selectExpr("CAST(value AS STRING) as json")

#df_datos = df_stream_values.select(from_json(col("json"), schema_stream).alias("data")).select("data.*")

#df_datos_clasificado = clasificar_congestion(df_datos)

#df_resultado = df_datos_clasificado.groupBy(
 #   window(col("fecha_hora"), "1 minute"),
  #  col("localidad")
#).agg(
 #   avg("velocidad_promedio").alias("velocidad_media"),
  #  count("*").alias("total_eventos")
#)

#consulta = df_resultado.writeStream \
 #   .outputMode("update") \
  #  .format("console") \
   # .option("truncate", False) \
    #.start()

#consulta.awaitTermination()
