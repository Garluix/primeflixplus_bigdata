import os
import sys
import matplotlib
matplotlib.use("Agg")  # guarda los PNG sin abrir ventanas
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp

# En Windows: que Spark use este mismo Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Inicializamos una sesión Spark
spark = SparkSession.builder.appName("Primeflix_Preparacion_Datos").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

archivo_entrada = sys.argv[1] if len(sys.argv) > 1 else "eventos.jsonl"
df = spark.read.json(archivo_entrada)

# Mostrar esquema inicial para verificar los tipos de variables
print("\n--- Esquema del Dataframe ---")
df.printSchema()

# Conteo de los registros iniciales
conteo_inicial = df.count()

# Convertimos la columna 'timestamp' de String a tipo timestamp
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Identificamos el problema de calidad: nulos y valores fuera del rango 0 a 200
nulos = df.filter(col("minutos_vistos").isNull()).count()
fuera_rango = df.filter((col("minutos_vistos") < 0) | (col("minutos_vistos") > 200)).count()
print("\n--- Problemas de calidad detectados en minutos_vistos ---")
print(f"Valores nulos: {nulos}")
print(f"Valores fuera de rango (menores a 0 o mayores a 200): {fuera_rango}")

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

# Muestra del esquema con la variable timestamp transformada
print("\n--- Esquema del Dataframe tras la limpieza---")
df_limpio.printSchema()

# Muestra pequeña de los datos limpios
print("\n--- DataFrame después de la limpieza ---")
df_limpio.show(10)

# Pregunta 1: estadística descriptiva de minutos_vistos
print("\n--- P1: Estadística descriptiva de minutos_vistos ---")
stats = df_limpio.select(
    F.round(F.avg("minutos_vistos"), 2).alias("promedio"),
    F.min("minutos_vistos").alias("minimo"),
    F.max("minutos_vistos").alias("maximo"),
    F.round(F.stddev("minutos_vistos"), 2).alias("desviacion"),
)
stats.show()
s = stats.first()

# Gráfico 1: distribución de minutos vistos
minutos = [r[0] for r in df_limpio.select("minutos_vistos").collect()]
plt.figure(figsize=(8, 5))
plt.hist(minutos, bins=24, color="#7b1fa2", edgecolor="white")
plt.title("Distribución de minutos vistos por reproducción")
plt.xlabel("Minutos vistos")
plt.ylabel("Cantidad de reproducciones")
plt.tight_layout()
plt.savefig("grafico1_distribucion.png", dpi=150)
plt.close()

# Pregunta 2: promedio de minutos vistos por subgénero
print("--- P2: Promedio de minutos vistos por subgénero ---")
por_sub = (df_limpio.groupBy("subgenero")
           .agg(F.round(F.avg("minutos_vistos"), 1).alias("promedio_min"),
                F.count("*").alias("reproducciones"))
           .orderBy(F.desc("promedio_min")))
por_sub.show()
filas = por_sub.collect()

# Gráfico 2: promedio por subgénero
plt.figure(figsize=(8, 5))
plt.bar([r["subgenero"] for r in filas], [r["promedio_min"] for r in filas], color="#c62828")
plt.title("Promedio de minutos vistos por subgénero")
plt.xlabel("Subgénero")
plt.ylabel("Promedio de minutos vistos")
plt.tight_layout()
plt.savefig("grafico2_subgenero.png", dpi=150)
plt.close()

# Pregunta 3: reproducciones por mes
print("--- P3: Reproducciones por mes ---")
por_mes = (df_limpio.withColumn("mes", F.month("timestamp"))
           .groupBy("mes").count().orderBy("mes"))
por_mes.show()
meses = por_mes.collect()
nombres = {7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}
etiquetas = [nombres[r["mes"]] for r in meses]
valores = [r["count"] for r in meses]
colores = ["#ef6c00" if r["mes"] == 10 else "#78909c" for r in meses]

# Gráfico 3: reproducciones por mes, con octubre destacado
plt.figure(figsize=(8, 5))
plt.bar(etiquetas, valores, color=colores)
plt.title("Reproducciones de terror por mes (octubre destacado)")
plt.xlabel("Mes (2026)")
plt.ylabel("Cantidad de reproducciones")
plt.tight_layout()
plt.savefig("grafico3_mensual.png", dpi=150)
plt.close()

# Apoyo: reproducciones por hora del día
print("--- Apoyo: reproducciones por hora ---")
por_hora = (df_limpio.withColumn("hora", F.hour("timestamp"))
            .groupBy("hora").count().orderBy("hora"))
por_hora.show(24)
noche = (df_limpio.withColumn("hora", F.hour("timestamp"))
         .filter(col("hora").isin(21, 22, 23, 0, 1, 2)).count())

# Respuestas a las preguntas de análisis
oct_n = next(r["count"] for r in meses if r["mes"] == 10)
otros = [r["count"] for r in meses if r["mes"] != 10]
prom_otros = sum(otros) / len(otros)
print("\n--- RESPUESTAS ---")
print(f"P1: promedio {s['promedio']} min, mínimo {s['minimo']}, máximo {s['maximo']}, desviación {s['desviacion']}.")
print(f"P2: el subgénero con mayor promedio es '{filas[0]['subgenero']}' ({filas[0]['promedio_min']} min); "
      f"el menor es '{filas[-1]['subgenero']}' ({filas[-1]['promedio_min']} min).")
print(f"P3: octubre tiene {oct_n} reproducciones vs {prom_otros:.0f} de promedio en los otros meses "
      f"(x{oct_n / prom_otros:.1f}).")
print(f"Apoyo: el {noche / conteo_final:.0%} de las reproducciones ocurre entre las 21:00 y las 02:59.")
print("Nota: los datos son sintéticos; el pico de octubre y las diferencias por subgénero "
      "se deben a las reglas con que se generaron.")

spark.stop()