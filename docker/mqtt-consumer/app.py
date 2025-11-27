import os
import json
import time
import re
import threading
from datetime import datetime, timezone
from queue import Queue, Empty
import psycopg2
from psycopg2.extras import execute_values
from paho.mqtt import client as mqtt

MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "device/+/telemetry/env")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt-consumer")

DB_HOST = os.getenv("PGHOST", "timescaledb")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "mqttdb")
DB_USER = os.getenv("PGUSER", "mqttuser")
DB_PASS = os.getenv("PGPASSWORD", "changeme")

BATCH_SIZE = 1000
FLUSH_INTERVAL = 0.05  # seconds

work_queue: "Queue[tuple[str, tuple]]" = Queue()
stop_event = threading.Event()


def connect_db():
    while True:
        try:
            print("Connecting to PostgreSQL/Timescale...")
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
            )
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit TO OFF;")
            print("Connected to database.")
            return conn
        except Exception as e:
            print("Error connecting to database, retrying in 2s:", e)
            time.sleep(2)


def parse_payload(payload_str: str):
    try:
        return json.loads(payload_str)
    except json.JSONDecodeError:
        pass

    s = payload_str.strip()
    if not (s.startswith("{") and s.endswith("}")):
        raise json.JSONDecodeError("Not a JSON-like object", payload_str, 0)

    s = re.sub(r'([,{])\s*([A-Za-z0-9_]+)\s*:', r'\1"\2":', s)
    s = re.sub(r'"timestamp":([^,}]+)', r'"timestamp":"\1"', s)
    s = re.sub(r'"deviceId":([^,}]+)', r'"deviceId":"\1"', s)
    return json.loads(s)


def enqueue(sql_id: str, params: tuple):
    work_queue.put((sql_id, params))


def db_worker():
    conn = connect_db()
    buffer = []
    last_flush = time.monotonic()

    while not stop_event.is_set():
        try:
            item = work_queue.get(timeout=FLUSH_INTERVAL)
            buffer.append(item)
        except Empty:
            pass

        now = time.monotonic()
        if buffer and (len(buffer) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL):
            try:
                conn = flush_buffer(conn, buffer)
            except Exception as e:
                print(f"[ERROR] flush failed, reconnecting: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect_db()
            buffer.clear()
            last_flush = time.monotonic()

    # final flush on exit
    if buffer:
        try:
            flush_buffer(conn, buffer)
        except Exception as e:
            print(f"[ERROR] final flush failed: {e}")
    try:
        conn.close()
    except Exception:
        pass


def flush_buffer(conn, buffer):
    if not buffer:
        return conn

    # group by statement id
    groups = {"device_status": [], "tablet_telemetry": [], "telemetry_env": []}
    for sql_id, params in buffer:
        if sql_id in groups:
            groups[sql_id].append(params)

    with conn.cursor() as cur:
        if groups["device_status"]:
            execute_values(
                cur,
                """
                INSERT INTO device_status (time, device_id, status, topic)
                VALUES %s
                """,
                groups["device_status"],
            )
        if groups["tablet_telemetry"]:
            execute_values(
                cur,
                """
                INSERT INTO tablet_telemetry (
                    time,
                    ts_epoch_ms,
                    schema_version,
                    device_id,
                    operator_id,
                    equipment_tag,
                    truck_status,
                    seq_id,
                    gnss_fix,
                    gnss_provider,
                    gnss_lat,
                    gnss_lon,
                    gnss_alt,
                    gnss_speed,
                    gnss_course,
                    gnss_hdop,
                    gnss_vdop,
                    gnss_pdop,
                    gnss_num_sats,
                    gnss_sats_visible,
                    gnss_cn0_avg,
                    gnss_cn0_min,
                    gnss_cn0_max,
                    gnss_cn0_p25,
                    gnss_cn0_p50,
                    gnss_cn0_p75,
                    gnss_accuracy_m,
                    gnss_vert_accuracy_m,
                    gnss_speed_accuracy_mps,
                    gnss_bearing_accuracy_deg,
                    gnss_has_l5,
                    gnss_raw_supported,
                    gnss_raw_count,
                    gnss_elapsed_realtime_nanos,
                    gnss_gps_visible,
                    gnss_gps_used,
                    gnss_glonass_visible,
                    gnss_glonass_used,
                    gnss_galileo_visible,
                    gnss_galileo_used,
                    gnss_beidou_visible,
                    gnss_beidou_used,
                    gnss_qzss_visible,
                    gnss_qzss_used,
                    gnss_sbas_visible,
                    gnss_sbas_used,
                    baro_pressure_hpa,
                    baro_altitude_m,
                    imu_acc_norm_rms,
                    imu_jerk_norm_rms,
                    imu_gyro_norm_rms,
                    imu_yaw_rate_deg_s,
                    imu_samples,
                    imu_fps_eff,
                    imu_pitch_deg,
                    imu_roll_deg,
                    imu_yaw_deg,
                    imu_q_w,
                    imu_q_x,
                    imu_q_y,
                    imu_q_z,
                    imu_acc_accuracy,
                    imu_gyro_accuracy,
                    imu_rotation_accuracy,
                    imu_motion_stationary,
                    imu_motion_shock_level,
                    imu_motion_shock_score,
                    imu_acc_longitudinal_mps2,
                    imu_acc_lateral_mps2,
                    imu_acc_vertical_mps2,
                    imu_vehicle_tilt_pitch_deg,
                    imu_vehicle_tilt_roll_deg,
                    imu_mag_field_strength_uT,
                    payload_json,
                    gnss_raw
                )
                VALUES %s
                """,
                groups["tablet_telemetry"],
            )
        if groups["telemetry_env"]:
            execute_values(
                cur,
                """
                INSERT INTO telemetry_env
                    (time, device_id, temperature, humidity, pressure, raw_payload)
                VALUES %s
                """,
                groups["telemetry_env"],
            )
    conn.commit()
    return conn


def handle_device_status(topic: str, payload_str: str):
    device_id = topic.split("/", 1)[1] if "/" in topic else None
    status_value = payload_str.strip()

    if not device_id or not status_value:
        return

    enqueue(
        "device_status",
        (datetime.now(timezone.utc), device_id, status_value, topic),
    )


def handle_tablet_telemetry(topic: str, payload_str: str):
    try:
        data = json.loads(payload_str)
    except Exception as e:
        print(f"[WARN] JSON parse failed on tablet telemetry topic {topic}: {e}")
        print(f"       Raw payload: {payload_str!r}")
        return

    ts_ms = data.get("ts_epoch")
    if ts_ms is None:
        print(f"[WARN] Missing ts_epoch on topic {topic}, skipping.")
        return

    try:
        time_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    except Exception as e:
        print(f"[WARN] Invalid ts_epoch={ts_ms!r} on topic {topic}: {e}")
        return

    values = (
        time_ts,
        ts_ms,
        data.get("schema.version"),
        data.get("device.id"),
        data.get("operator.id"),
        data.get("equipment.tag"),
        data.get("truck.status"),
        data.get("seq_id"),
        data.get("gnss.fix"),
        data.get("gnss.provider"),
        data.get("gnss.lat"),
        data.get("gnss.lon"),
        data.get("gnss.alt"),
        data.get("gnss.speed"),
        data.get("gnss.course"),
        data.get("gnss.hdop"),
        data.get("gnss.vdop"),
        data.get("gnss.pdop"),
        data.get("gnss.num_sats"),
        data.get("gnss.sats_visible"),
        data.get("gnss.cn0_avg"),
        data.get("gnss.cn0.min"),
        data.get("gnss.cn0.max"),
        data.get("gnss.cn0.p25"),
        data.get("gnss.cn0.p50"),
        data.get("gnss.cn0.p75"),
        data.get("gnss.accuracy_m"),
        data.get("gnss.vert_accuracy_m"),
        data.get("gnss.speed_accuracy_mps"),
        data.get("gnss.bearing_accuracy_deg"),
        data.get("gnss.has_l5"),
        data.get("gnss.raw_supported"),
        data.get("gnss.raw_count"),
        data.get("gnss.elapsedRealtimeNanos"),
        data.get("gnss.constellation.gps_visible"),
        data.get("gnss.constellation.gps_used"),
        data.get("gnss.constellation.glonass_visible"),
        data.get("gnss.constellation.glonass_used"),
        data.get("gnss.constellation.galileo_visible"),
        data.get("gnss.constellation.galileo_used"),
        data.get("gnss.constellation.beidou_visible"),
        data.get("gnss.constellation.beidou_used"),
        data.get("gnss.constellation.qzss_visible"),
        data.get("gnss.constellation.qzss_used"),
        data.get("gnss.constellation.sbas_visible"),
        data.get("gnss.constellation.sbas_used"),
        data.get("baro.pressure_hpa"),
        data.get("baro.altitude_m"),
        data.get("imu.acc.norm.rms"),
        data.get("imu.jerk.norm.rms"),
        data.get("imu.gyro.norm.rms"),
        data.get("imu.yaw_rate.deg_s"),
        data.get("imu.samples"),
        data.get("imu.fps_eff"),
        data.get("imu.pitch_deg"),
        data.get("imu.roll_deg"),
        data.get("imu.yaw_deg"),
        data.get("imu.q.w"),
        data.get("imu.q.x"),
        data.get("imu.q.y"),
        data.get("imu.q.z"),
        data.get("imu.acc.accuracy"),
        data.get("imu.gyro.accuracy"),
        data.get("imu.rotation.accuracy"),
        data.get("imu.motion.stationary"),
        data.get("imu.motion.shock_level"),
        data.get("imu.motion.shock_score"),
        data.get("imu.acc_longitudinal_mps2"),
        data.get("imu.acc_lateral_mps2"),
        data.get("imu.acc_vertical_mps2"),
        data.get("imu.vehicle_tilt_pitch_deg"),
        data.get("imu.vehicle_tilt_roll_deg"),
        data.get("imu.mag.field_strength_uT"),
        json.dumps(data),
        json.dumps(data.get("gnss.raw")) if data.get("gnss.raw") is not None else None,
    )

    enqueue("tablet_telemetry", values)


def handle_env_telemetry(data):
    device_id = data.get("deviceId")
    ts = data.get("timestamp")
    temp = data.get("temperature")
    hum = data.get("humidity")
    press = data.get("pressure")
    enqueue(
        "telemetry_env",
        (ts, device_id, temp, hum, press, json.dumps(data)),
    )


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT with result code", rc)
    client.subscribe(MQTT_TOPIC, qos=1)
    print(f"Subscribed to topic filter: {MQTT_TOPIC} (QoS1)")
    client.subscribe("status/#", qos=1)
    client.subscribe("telemetry/#", qos=1)
    print("Subscribed to status/# and telemetry/# (QoS1)")


def on_disconnect(client, userdata, rc):
    print(f"[WARN] MQTT disconnected (rc={rc}), will retry...")


def on_message(client, userdata, msg):
    payload_str = msg.payload.decode("utf-8", errors="ignore").strip()

    if msg.topic.startswith("status/"):
        handle_device_status(msg.topic, payload_str)
        return

    if msg.topic.startswith("telemetry/"):
        handle_tablet_telemetry(msg.topic, payload_str)
        return

    if not payload_str:
        print(f"[WARN] Empty payload on topic {msg.topic}, ignoring.")
        return

    try:
        data = parse_payload(payload_str)
    except Exception as e:
        print(f"[WARN] JSON parse failed on topic {msg.topic}: {e}")
        print(f"       Raw payload: {payload_str!r}")
        return

    try:
        handle_env_telemetry(data)
    except Exception as e:
        print(f"[ERROR] Error enqueueing env telemetry: {e}")


def main():
    worker = threading.Thread(target=db_worker, daemon=True)
    worker.start()

    client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.max_queued_messages_set(0)

    print(f"Connecting to MQTT broker {MQTT_BROKER}:{MQTT_PORT} ...")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    try:
        client.loop_forever(retry_first_connection=True)
    finally:
        stop_event.set()
        worker.join(timeout=5)


if __name__ == "__main__":
    main()
