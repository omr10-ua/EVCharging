from flask import Flask, jsonify, request, render_template, send_from_directory
import os
from .data_manager import get_all_cps, get_cp, load_data, update_cp
from .registry import registry_bp  # ✅ Importar Registry Blueprint

# Configure template folder
template_dir = os.path.join(os.path.dirname(__file__), 'templates')
app = Flask(__name__, template_folder=template_dir)

# ✅ Registrar Registry Blueprint
app.register_blueprint(registry_bp)

@app.route("/")
def index():
    """Página principal de monitorización"""
    return render_template('index.html')

@app.get("/api/charging_points")
def list_cps():
    """API endpoint que devuelve todos los CPs"""
    return jsonify(get_all_cps())

@app.get("/api/charging_points/<cp_id>")
def get_cp_endpoint(cp_id):
    """API endpoint para obtener un CP específico"""
    cp = get_cp(cp_id)
    if cp is None:
        return jsonify({"error":"not found"}), 404
    return jsonify(cp)

@app.post("/api/charging_points/<cp_id>/command")
def cp_command(cp_id):
    """
    Endpoint para enviar comandos administrativos a un CP específico
    Body JSON: {"action":"stop"} or {"action":"resume"}
    """
    payload = request.get_json() or {}
    action = payload.get("action")
    if action not in ("stop","resume"):
        return jsonify({"error":"invalid action"}), 400

    if action == "stop":
        update_cp(cp_id, state="PARADO")
        print(f"[API] CP {cp_id} -> PARADO")
    else:
        update_cp(cp_id, state="ACTIVADO")
        print(f"[API] CP {cp_id} -> ACTIVADO")
    
    return jsonify({"status":"ok","cp_id":cp_id,"action":action})

@app.post("/api/command_all")
def command_all_cps():
    """
    Endpoint para enviar comando a TODOS los CPs
    Body JSON: {"action":"stop"} or {"action":"resume"}
    """
    payload = request.get_json() or {}
    action = payload.get("action")
    if action not in ("stop", "resume"):
        return jsonify({"error":"invalid action"}), 400
    
    cps = get_all_cps()
    new_state = "PARADO" if action == "stop" else "ACTIVADO"
    
    updated = []
    for cp_id in cps.keys():
        update_cp(cp_id, state=new_state)
        updated.append(cp_id)
        print(f"[API] CP {cp_id} -> {new_state}")
    
    return jsonify({
        "status": "ok",
        "action": action,
        "updated_cps": updated,
        "count": len(updated)
    })

@app.get("/api/system_status")
def system_status():
    """Devuelve el estado general del sistema"""
    cps = get_all_cps()
    total = len(cps)
    
    status_count = {}
    for cp in cps.values():
        state = cp.get("state", "DESCONECTADO")
        status_count[state] = status_count.get(state, 0) + 1
    
    return jsonify({
        "total_cps": total,
        "status_breakdown": status_count,
        "system_status": "OK"
    })

@app.get("/api/drivers")
def list_drivers():
    """
    Lista todos los drivers registrados
    Response:
    {
        "DRV001": {
            "id": "DRV001",
            "created_at": "2025-12-10T12:00:00"
        }
    }
    """
    data = load_data()
    drivers = data.get("drivers", {})
    return jsonify(drivers)

@app.get("/api/drivers/<driver_id>")
def get_driver(driver_id):
    """
    Obtiene información de un driver específico
    Response:
    {
        "id": "DRV001",
        "created_at": "2025-12-10T12:00:00"
    }
    """
    data = load_data()
    driver = data.get("drivers", {}).get(driver_id)
    
    if not driver:
        return jsonify({"error": "Driver not found"}), 404
    
    return jsonify(driver)

@app.get("/api/sessions")
def list_sessions():
    """
    Lista todas las sesiones de recarga
    Response:
    [
        {
            "session_id": "...",
            "cp_id": "CP001",
            "driver_id": "DRV001",
            "start_time": "...",
            "end_time": "...",
            "total_kwh": 15.3,
            "total_euros": 5.36,
            "status": "completed"
        }
    ]
    """
    data = load_data()
    sessions = data.get("sessions", [])
    return jsonify(sessions)

@app.get("/api/sessions/<cp_id>")
def get_cp_sessions(cp_id):
    """
    Obtiene sesiones de un CP específico
    """
    data = load_data()
    sessions = data.get("sessions", [])
    
    # Filtrar sesiones del CP
    cp_sessions = [s for s in sessions if s.get("cp_id") == cp_id]
    
    return jsonify(cp_sessions)