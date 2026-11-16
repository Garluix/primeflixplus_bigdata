from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder.appName("Primeflix_Preparacion_Datos").getOrCreate() # Inicializamos una sesión Spark

archivo_entrada = "eventos.jsonl"
df = spark.read.json(archivo_entrada)

# Mostrar esquema inicial para verificar los tipos de variables
print("\n--- Esquema del Dataframe ---")
df.printSchema()

# Conteo de los registros iniciales
conteo_inicial = df.count()

# Convertimos la columna 'timestamp' de String a tipo timestamp
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Limpieza para eliminar valores nulos, y minutos vistos válidos >=0 y <=200
df_limpio = df.filter(
    col("minutos_vistos").isNotNull() & 
    (col("minutos_vistos") >= 0) & 
    (col("minutos_vistos") <= 200)
)

# Conteo de los registros después de la limpieza
conteo_final = df_limpio.count()

# Mostrar el conteo antes y después del tratamiento
print("\n--- Resultados de la limpieza ---")
print(f"Registros iniciales (antes): {conteo_inicial}")
print(f"Registros finales (después): {conteo_final}")
print(f"Registros defectuosos eliminados: {conteo_inicial - conteo_final}")

spark.stop()