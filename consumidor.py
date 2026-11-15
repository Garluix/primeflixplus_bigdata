import json
import sys
import time
from confluent_kafka import Consumer

# Parámetros
if len(sys.argv) > 1:
    ARCHIVO = sys.argv[1]
else:
    ARCHIVO = "eventos.jsonl"

if len(sys.argv) > 2:
    N = int(sys.argv[2])  # mensajes esperados
else:
    N = 5000

if len(sys.argv) > 3:
    GRUPO = sys.argv[3]
else:
    GRUPO = "grupo_primeflix"
TOPICO = "reproducciones_terror"
SERVIDOR = "localhost:9092"
TIMEOUT = 20                    # segundos sin mensajes nuevos para terminar
CAMPOS = {"id_evento", "timestamp", "subgenero", "minutos_vistos", "dispositivo"}

consumer = Consumer({
    "bootstrap.servers": SERVIDOR,
    "group.id": GRUPO,
    "auto.offset.reset": "earliest",   # si el grupo es nuevo, lee desde el offset 0
})
consumer.subscribe([TOPICO])

guardados = 0
invalidos = 0
muestra = []
ultimo_mensaje = time.time()

print(f"Escuchando '{TOPICO}'... (termina al llegar a {N} mensajes o tras {TIMEOUT}s sin datos)")

with open(ARCHIVO, "w", encoding="utf-8") as f:
    while guardados + invalidos < N:
        msg = consumer.poll(1.0)

        if msg is None:
            if time.time() - ultimo_mensaje > TIMEOUT:
                print("Timeout: no llegaron más mensajes.")
                break
            continue
        if msg.error():
            print("Error de Kafka:", msg.error())
            continue

        ultimo_mensaje = time.time()
        try:
            evento = json.loads(msg.value().decode("utf-8"))
            if not isinstance(evento, dict) or not CAMPOS.issubset(evento):
                raise ValueError("faltan campos")
        except (ValueError, UnicodeDecodeError) as e:
            invalidos += 1
            print(f"Mensaje inválido (offset {msg.offset()}): {e}")
            continue

        f.write(json.dumps(evento, ensure_ascii=False) + "\n")
        guardados += 1
        if len(muestra) < 5:
            muestra.append((msg.offset(), evento))

consumer.close()

print("\n--- Muestra de mensajes recibidos ---")
for offset, ev in muestra:
    print(f"offset {offset}: {ev}")
print(f"\nGuardados:  {guardados}  ->  {ARCHIVO}")
print(f"Inválidos:  {invalidos}")