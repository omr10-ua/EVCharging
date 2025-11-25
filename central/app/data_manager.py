import json
import threading
import os
from datetime import datetime

DATA_FILE = os.path.join(os.path.dirname(__file__), "data.json")
_lock = threading.Lock()

def load_data():
    """Carga el JSON completo (thread-safe lectura simple)."""
    if not os.path.exists(DATA_FILE):
        # crear estructura básica si no existe
        data = {"charging_points": {}, "drivers": {}, "sessions": []}
        save_data(data)
        return data

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_data(data):
    """Guarda el JSON atomizando con un lock para evitar corrupciones."""
    with _lock:
        tmp = DATA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        os.replace(tmp, DATA_FILE)

def ensure_cp_exists(cp_id, location=None, price=None):
    """Si no existe el CP en el JSON, lo crea con valores por defecto."""
    data = load_data()
    if cp_id not in data["charging_points"]:
        data["charging_points"][cp_id] = {
            "id": cp_id,
            "location": location or "unknown",
            "price": float(price) if price is not None else 0.0,
            "state": "DESCONECTADO",
            "current_kw": 0.0,
            "current_euros": 0.0,
            "current_driver": None,
            "last_update": datetime.utcnow().isoformat()
        }
        save_data(data)
    return data["charging_points"][cp_id]

def update_cp(cp_id, **kwargs):
    """Actualiza los campos del CP y guarda."""
    data = load_data()
    if cp_id not in data["charging_points"]:
        return False
    cp = data["charging_points"][cp_id]
    for k, v in kwargs.items():
        cp[k] = v
    cp["last_update"] = datetime.utcnow().isoformat()
    save_data(data)
    return True

def get_all_cps():
    return load_data()["charging_points"]

def get_cp(cp_id):
    return load_data()["charging_points"].get(cp_id)

def add_session(session_record):
    data = load_data()
    data["sessions"].append(session_record)
    save_data(data)
    return True
