import os
import threading
import time
from .cp_socket_server import CPSocketServer
from .kafka_producer import KafkaCentralProducer
from .api import app as flask_app
from .data_manager import load_data, save_data, get_all_cps
from werkzeug.serving import make_server

def start_flask_in_thread(host="0.0.0.0", port=8000):
    srv = make_server(host, port, flask_app)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    return srv

def periodic_monitor_publish(producer, interval=5):
    while True:
        try:
            snapshot = {"type":"central_snapshot","cps": list(get_all_cps().values())}
            producer.publish_monitor(snapshot)
        except Exception as e:
            print("[MONITOR PUBLISH] error:", e)
        time.sleep(interval)

def main():
    # configuration via env vars (easy for docker)
    kafka_host = os.environ.get("KAFKA_HOST", "kafka")
    kafka_port = os.environ.get("KAFKA_PORT", "9092")
    print("[CENTRAL] Starting CENTRAL")
    print(f"[CENTRAL] KAFKA at {kafka_host}:{kafka_port}")

    # init producer
    producer = KafkaCentralProducer()

    # start CP socket server
    cp_server = CPSocketServer(host="0.0.0.0", port=int(os.environ.get("CENTRAL_CP_PORT", 5001)), producer=producer)
    cp_server.start()

    # start API
    api_port = int(os.environ.get("CENTRAL_API_PORT", 8000))
    start_flask_in_thread(host="0.0.0.0", port=api_port)
    print(f"[CENTRAL] API disponible en puerto {api_port}")

    # start monitor publisher loop
    t = threading.Thread(target=periodic_monitor_publish, args=(producer, 4), daemon=True)
    t.start()

    # main loop keep alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Central shutting down...")
        cp_server.stop()
        producer.flush()

if __name__ == "__main__":
    main()
