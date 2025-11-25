from flask import Flask, jsonify, request
from .data_manager import get_all_cps, get_cp, load_data, update_cp

app = Flask(__name__)

@app.get("/charging_points")
def list_cps():
    return jsonify(get_all_cps())

@app.get("/charging_points/<cp_id>")
def get_cp_endpoint(cp_id):
    cp = get_cp(cp_id)
    if cp is None:
        return jsonify({"error":"not found"}), 404
    return jsonify(cp)

@app.post("/charging_points/<cp_id>/command")
def cp_command(cp_id):
    """
    Endpoint simple para enviar comandos administrativos (parar/reanudar).
    Body JSON: {"action":"stop"} or {"action":"resume"}
    """
    payload = request.get_json() or {}
    action = payload.get("action")
    if action not in ("stop","resume"):
        return jsonify({"error":"invalid action"}), 400

    if action == "stop":
        update_cp(cp_id, state="PARADO")
    else:
        update_cp(cp_id, state="ACTIVADO")
    return jsonify({"status":"ok","cp_id":cp_id,"action":action})
