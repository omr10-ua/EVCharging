import json
import threading
import os
import time
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

    with _lock:
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[DATA MANAGER] Error loading data: {e}, creating new file")
            data = {"charging_points": {}, "drivers": {}, "sessions": []}
            save_data(data)
            return data

def save_data(data):
    """Guarda el JSON atomizando con un lock para evitar corrupciones."""
    with _lock:
        # Intentar con reintentos
        max_retries = 5
        for attempt in range(max_retries):
            try:
                tmp = DATA_FILE + f".tmp.{os.getpid()}.{time.time()}"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                
                # Intentar replace
                try:
                    os.replace(tmp, DATA_FILE)
                    return
                except OSError:
                    # Si falla, intentar borrar el destino primero
                    try:
                        if os.path.exists(DATA_FILE):
                            os.remove(DATA_FILE)
                        os.rename(tmp, DATA_FILE)
                        return
                    except Exception:
                        pass
                
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"[DATA MANAGER] Error saving data after {max_retries} attempts: {e}")
                    # Último intento: guardar directamente
                    try:
                        with open(DATA_FILE, "w", encoding="utf-8") as f:
                            json.dump(data, f, indent=4, ensure_ascii=False)
                    except Exception as e2:
                        print(f"[DATA MANAGER] CRITICAL: Cannot save data: {e2}")
                else:
                    time.sleep(0.1)  # Esperar un poco antes de reintentar
            
            time.sleep(0.05)  # Pequeña pausa entre reintentos

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
            "total_kwh": 0.0,
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