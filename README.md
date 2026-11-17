 # Integrantes
* Jesús Rodríguez
* David Bravo
* Matías Palma
* Nicolás Venegas
* Nicolás Olguín

Ítalo Osorio
# Primeflix+ — Análisis de Reproducciones de Terror en Streaming

 Se busco analizar el comportamiento de la audiencia en el género de terror dentro de la plataforma Primeflix+. El proyecto simula eventos de streaming, los ingesta mediante Apache Kafka y los procesa con PySpark para responder preguntas clave de negocio (duración promedio, subgéneros más consumidos y estacionalidad en octubre/Halloween).

## 🚀 Características principales
* Simulación y Producción de Eventos (productor.py): Generación de 5.000 eventos sintéticos en formato JSON (periodo julio–diciembre de 2026) con patrones intencionales de negocio (pico de x2,5 en octubre, horario peak nocturno de 21:00 a 02:59) e inyección controlada de ruido (5% de anomalías en minutos_vistos).
* Ingesta en Streaming con Apache Kafka (consumidor_2.py): Suscripción al tópico reproducciones_terror desde el primer offset (earliest), validación de esquema JSON y almacenamiento persistente línea por línea en eventos.jsonl.
* Limpieza y Transformación en PySpark: Conversión de tipos (timestamp de string a timestamp) y filtrado de los registros defectuosos (null, -5, 999), reduciendo los 5.000 eventos iniciales a 4.755 registros limpios.
* Analítica y Visualización: Cálculo de estadísticas descriptivas, agregaciones por subgénero, mes y hora, y generación de gráficos de distribución para la toma de decisiones de contenido.

## 🛠️ Tecnologías utilizadas
* Lenguaje: Python
* Librería de mensajería: confluent-kafka
* Procesamiento : Apache Spark / PySpark
* Visualización de Datos: Matplotlib / Pandas
* Formato de Almacenamiento: JSON / JSON Lines (.jsonl)
* Entorno de desarrollo: Visual Studio Code

## 📋 Requisitos previos
Lista de lo que el usuario necesita tener instalado antes de ejecutar el proyecto:
* Python 3.8+ y gestor de paquetes pip.
* Apache Kafka en ejecución local en localhost:9092 con el tópico reproducciones_terror creado.
* Java (JDK 8/11/17) configurado (requerido para ejecutar PySpark en local).
* Visual Studio Code con las extensiones de Python y Jupyter (si utilizas notebooks para PySpark).

## 🔧 Instalación y ejecución
Pasos exactos para levantar el proyecto en local:

1. Clona el repositorio:
   git clone [https://github.com/Garluix/primeflixplus_bigdata.git]
   
2. Creación y activación de un entorno virtual:
   python -m venv .venv
# En Windows:
.venv\Scripts\activate
# En Linux/macOS:
source .venv/bin/activate

3. Instalar las dependencias:
   pip install confluent-kafka pyspark matplotlib pandas
   
4. Ejecutar el Productor para enviar eventos a Kafka:
   python productor.py
   
5. Ejecuta el Consumidor para guardar los eventos en eventos.jsonl:
   python consumidor_2.py
   
6. Ejecuta el procesamiento y análisis en PySpark (preparacion.py):
   python preparacion.py
