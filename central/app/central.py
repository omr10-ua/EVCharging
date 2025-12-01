import os
import threading
import time
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit

from .cp_socket_server import CPSocketServer
from .kafka_producer import KafkaCentralProducer
from .kafka_consumer import KafkaCentralConsumer  # ✅ NUEVO
from .data_manager import load_data, save_data, get_all_cps, get_cp, update_cp

# Crear aplicación Flask con SocketIO
app = Flask(__name__)
app.config['SECRET_KEY'] = 'evcharging_secret_2025'

socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet',
    logger=False,
    engineio_logger=False
)

# Variable global para el producer y consumer
kafka_producer = None
kafka_consumer = None  # ✅ NUEVO

# ==================== RUTAS WEB ====================

@app.route('/')
def index():
    """Panel principal de monitorización"""
    return render_template('monitor.html')

@app.route('/api/charging_points')
def api_list_cps():
    """API: Lista todos los CPs"""
    return jsonify(get_all_cps())

@app.route('/api/charging_points/<cp_id>')
def api_get_cp(cp_id):
    """API: Obtiene un CP específico"""
    cp = get_cp(cp_id)
    if cp is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(cp)

@app.route('/api/charging_points/<cp_id>/command', methods=['POST'])
def api_cp_command(cp_id):
    """
    API: Envía comandos a un CP (PARAR/REANUDAR)
    Body JSON: {"action":"stop"} or {"action":"resume"}
    """
    payload = request.get_json() or {}
    action = payload.get("action")
    
    if action not in ("stop", "resume"):
        return jsonify({"error": "invalid action"}), 400

    cp = get_cp(cp_id)
    if not cp:
        return jsonify({"error": "CP not found"}), 404

    # ✅ Enviar comando al CP real
    command_sent = False
    if hasattr(app, 'cp_server'):
        command = {
            "type": "command",
            "action": action,
            "cp_id": cp_id
        }
        command_sent = app.cp_server.send_command_to_cp(cp_id, command)
    
    # Actualizar estado en BD
    if action == "stop":
        update_cp(cp_id, state="PARADO")
        msg = f'CP {cp_id} ha sido PARADO'
        if not command_sent:
            msg += ' (CP no conectado - comando no enviado)'
        socketio.emit('notification', {
            'type': 'warning' if not command_sent else 'info',
            'message': msg
        }, namespace='/')
        print(f"[CENTRAL] CP {cp_id} PARADO por comando administrativo")
    else:
        update_cp(cp_id, state="ACTIVADO")
        msg = f'CP {cp_id} ha sido REANUDADO'
        if not command_sent:
            msg += ' (CP no conectado - comando no enviado)'
        socketio.emit('notification', {
            'type': 'warning' if not command_sent else 'success',
            'message': msg
        }, namespace='/')
        print(f"[CENTRAL] CP {cp_id} REANUDADO por comando administrativo")
    
    return jsonify({
        "status": "ok", 
        "cp_id": cp_id, 
        "action": action,
        "command_sent": command_sent
    })

# ==================== WEBSOCKET HANDLERS ====================

@socketio.on('connect')
def handle_connect():
    """Cliente WebSocket conectado"""
    print('[WEBSOCKET] Cliente conectado')
    emit('notification', {
        'type': 'success',
        'message': 'Conectado al sistema de monitorización'
    })

@socketio.on('disconnect')
def handle_disconnect():
    """Cliente WebSocket desconectado"""
    print('[WEBSOCKET] Cliente desconectado')

@socketio.on('request_state')
def handle_request_state():
    """Cliente solicita estado actual"""
    cps = get_all_cps()
    emit('update_cps', {'charging_points': list(cps.values())})

# ==================== THREADS DE SOPORTE ====================

def broadcast_state_loop():
    """Envía actualizaciones de estado cada 2 segundos a todos los clientes web"""
    while True:
        try:
            cps = get_all_cps()
            socketio.emit('update_cps', 
                         {'charging_points': list(cps.values())}, 
                         namespace='/')
        except Exception as e:
            print(f"[BROADCAST] Error: {e}")
        time.sleep(2)

def periodic_monitor_publish(producer, interval=5):
    """Publica snapshot del sistema a Kafka periódicamente"""
    while True:
        try:
            snapshot = {
                "type": "central_snapshot",
                "cps": list(get_all_cps().values()),
                "timestamp": time.time()
            }
            producer.publish_monitor(snapshot)
        except Exception as e:
            print("[MONITOR PUBLISH] error:", e)
        time.sleep(interval)

def reset_all_cps_to_disconnected():
    """Marca todos los CPs como DESCONECTADO al arrancar Central"""
    data = load_data()
    cps = data.get("charging_points", {})
    
    if cps:
        print(f"[CENTRAL] 📝 Marcando {len(cps)} CPs como DESCONECTADO...")
        for cp_id in cps:
            update_cp(cp_id, state="DESCONECTADO")
        print(f"[CENTRAL] ✅ {len(cps)} CPs marcados como DESCONECTADO")
    else:
        print("[CENTRAL] ℹ️  No hay CPs previos registrados")

# ==================== FUNCIÓN PRINCIPAL ====================

def main():
    global kafka_producer, kafka_consumer
    
    # Configuración vía variables de entorno
    kafka_host = os.environ.get("KAFKA_HOST", "kafka")
    kafka_port = os.environ.get("KAFKA_PORT", "9092")
    cp_port = int(os.environ.get("CENTRAL_CP_PORT", 5001))
    api_port = int(os.environ.get("CENTRAL_API_PORT", 8000))
    
    print("="*60)
    print("[CENTRAL] 🚀 Iniciando CENTRAL EVCharging")
    print("="*60)
    print(f"[CENTRAL] 📡 Kafka: {kafka_host}:{kafka_port}")
    print(f"[CENTRAL] 🔌 Socket CP Server: puerto {cp_port}")
    print(f"[CENTRAL] 🌐 Web Panel: http://0.0.0.0:{api_port}")
    print("="*60)
    
    # ✅ NUEVO: Marcar todos los CPs como DESCONECTADO al arrancar
    reset_all_cps_to_disconnected()
    
    # Inicializar Kafka Producer
    kafka_producer = KafkaCentralProducer()
    
    # ✅ NUEVO: Inicializar Kafka Consumer
    kafka_consumer = KafkaCentralConsumer(socketio=socketio)
    kafka_consumer.start()
    print("[CENTRAL] ✅ Kafka Consumer iniciado")
    
    # Iniciar servidor de sockets para CPs
    cp_server = CPSocketServer(
        host="0.0.0.0", 
        port=cp_port, 
        producer=kafka_producer,
        socketio=socketio
    )
    cp_server.start()
    app.cp_server = cp_server  # ✅ Guardar referencia
    print(f"[CENTRAL] ✅ Socket server iniciado en puerto {cp_port}")
    
    # Iniciar thread de broadcast WebSocket
    broadcast_thread = threading.Thread(target=broadcast_state_loop, daemon=True)
    broadcast_thread.start()
    print("[CENTRAL] ✅ Broadcast WebSocket iniciado")
    
    # Iniciar thread de publicación a Kafka
    monitor_thread = threading.Thread(
        target=periodic_monitor_publish, 
        args=(kafka_producer, 4), 
        daemon=True
    )
    monitor_thread.start()
    print("[CENTRAL] ✅ Monitor Kafka iniciado")
    
    # Iniciar servidor Flask con SocketIO
    print(f"[CENTRAL] 🌐 Iniciando panel web en puerto {api_port}...")
    print(f"[CENTRAL] 👉 Accede a: http://localhost:{api_port}")
    print("="*60)
    
    try:
        socketio.run(
            app, 
            host="0.0.0.0", 
            port=api_port, 
            debug=False, 
            use_reloader=False,
            allow_unsafe_werkzeug=True
        )
    except KeyboardInterrupt:
        print("\n[CENTRAL] ⚠️  Señal de interrupción recibida")
        print("[CENTRAL] 🛑 Cerrando servidor...")
        cp_server.stop()
        kafka_consumer.stop()  # ✅ NUEVO
        kafka_producer.flush()
        print("[CENTRAL] ✅ Central finalizada correctamente")

if __name__ == "__main__":
    main()