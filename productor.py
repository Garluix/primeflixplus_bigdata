import json
import random
import sys
import calendar
from datetime import datetime
from confluent_kafka import Producer

# Parámetros:
if len(sys.argv) > 1: # cantidad de eventos
    N = int(sys.argv[1])
else:
    N = 5000

if len(sys.argv) > 2: # semilla (opcional)
    SEMILLA = int(sys.argv[2])
else:
    SEMILLA = 42

TOPICO = "reproducciones_terror"
SERVIDOR = "localhost:9092"
PROB_CALIDAD = 0.05          # 5% de eventos con minutos_vistos defectuoso
ANIO = 2026
random.seed(SEMILLA)         # resultados reproducibles

# Reglas del patrón
MESES = [7, 8, 9, 10, 11, 12]
PESO_MES = [1, 1, 1, 2.5, 1, 1]            # octubre 2.5 veces más

SUBGENEROS = ["slasher", "paranormal", "zombies", "psicológico", "gore"]
PESO_SUB = [1, 1, 1, 1, 1]
PESO_SUB_OCTUBRE = [2, 2, 1, 1, 1]         # en octubre suben slasher y paranormal

MEDIA_MIN = { #En minutos
            "paranormal": 95, 
            "psicológico": 85, 
            "zombies": 75,
            "slasher": 70, 
            "gore": 55
            }    # paranormal se ve más que gore
DESV_MIN = 20

PESO_HORA = [1] * 24
for h in (21, 22, 23, 0, 1, 2): # horario nocturno con mayor peso
    PESO_HORA[h] = 3 

DISPOSITIVOS = ["celular", "TV", "computador"]
VALORES_MALOS = [None, -5, 999]


def generar_evento(i):
    mes = random.choices(MESES, weights=PESO_MES)[0]
    dia = random.randint(1, calendar.monthrange(ANIO, mes)[1])
    hora = random.choices(range(24), weights=PESO_HORA)[0]
    ts = datetime(ANIO, mes, dia, hora, random.randint(0, 59), random.randint(0, 59))

    pesos = PESO_SUB_OCTUBRE if mes == 10 else PESO_SUB
    sub = random.choices(SUBGENEROS, weights=pesos)[0]

    minutos = int(random.gauss(MEDIA_MIN[sub], DESV_MIN))
    minutos = max(5, min(minutos, 240))

    if random.random() < PROB_CALIDAD:          # problema de calidad
        minutos = random.choice(VALORES_MALOS)

    return {
        "id_evento": f"ev_{i:06d}",
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "subgenero": sub,
        "minutos_vistos": minutos,
        "dispositivo": random.choice(DISPOSITIVOS),
    }


enviados = 0
errores = 0


def callback(err, msg):
    global enviados, errores
    if err is not None:
        errores += 1
    else:
        enviados += 1


producer = Producer({"bootstrap.servers": SERVIDOR})

for i in range(1, N + 1):
    evento = generar_evento(i)
    try:
        producer.produce(TOPICO, value=json.dumps(evento, ensure_ascii=False).encode("utf-8"),
                         callback=callback)
    except BufferError:
        producer.poll(1)
        errores += 1
    producer.poll(0)

producer.flush()
print(f"Eventos solicitados: {N}")
print(f"Eventos enviados:    {enviados}")
print(f"Errores:             {errores}")