import time
import json
import random
import paho.mqtt.client as mqtt
from datetime import datetime

broker = "mosquitto"  # nome do serviço dentro do docker-compose
port = 1883

# apenas um device simulado
devices = ["device01"]
publish_rate = 1  # 1 Hz por device


def connect_with_retry(client: mqtt.Client):
    """Tenta conectar ao broker em loop até conseguir."""
    while True:
        try:
            client.connect(broker, port, 60)
            return
        except Exception as exc:
            print(f"[WARN] Falha ao conectar no broker: {exc}, tentando em 2s...")
            time.sleep(2)


def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Conectado (rc={rc})")


def on_disconnect(client, userdata, rc):
    print(f"[WARN] MQTT desconectado (rc={rc}), tentando reconectar...")


client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.reconnect_delay_set(min_delay=1, max_delay=10)
client.enable_logger()

connect_with_retry(client)
client.loop_start()  # habilita auto-reconnect em background

print("Simulador de 1 device iniciado...")

next_send = {d: time.time() for d in devices}

while True:
    now = time.time()

    for device in devices:
        if now >= next_send[device]:
            payload = {
                "deviceId": device,
                "timestamp": datetime.utcnow().isoformat(),
                "temperature": round(20 + random.uniform(-5, 5), 2),
                "humidity": round(60 + random.uniform(-10, 10), 2),
                "pressure": round(1013 + random.uniform(-20, 20), 2),
            }

            topic = f"device/{device}/telemetry/env"
            if not client.is_connected():
                connect_with_retry(client)

            result = client.publish(topic, json.dumps(payload), qos=1)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"[WARN] publish falhou (rc={result.rc}) para {topic}")

            print(f"[{device}] enviado -> {payload}")

            next_send[device] += publish_rate

    time.sleep(0.05)
