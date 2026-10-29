from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
import logging

# Configurar la sesión de Spark
spark = SparkSession.builder \
    .appName("DesercionEstudiantilStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema ajustado a las variables sociodemográficas y laborales
schema = StructType([
    StructField("student_id", IntegerType()),
    StructField("estrato", IntegerType()),
    StructField("situacion_laboral", StringType()),
    StructField("horas_laborales", IntegerType()),
    StructField("carga_academica", IntegerType()),
    StructField("riesgo_desercion", FloatType()),
    StructField("timestamp", TimestampType())
])

# Configurar el lector de streaming para leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Parsear los datos JSON entrantes
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calcular estadísticas de riesgo y horas de trabajo promedio agrupadas por ventana de tiempo y estrato socioeconómico
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), col("estrato")) \
    .agg(
        avg("riesgo_desercion").alias("promedio_riesgo"),
        avg("horas_laborales").alias("promedio_horas_trabajo")
    )

# Escribir los resultados analíticos en la consola en tiempo real
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
