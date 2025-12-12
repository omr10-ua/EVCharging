import json
import threading
import os
import hashlib
import secrets
import base64
from datetime import datetime

DATA_FILE = os.path.join(os.path.dirname(__file__), "data.json")
_lock = threading.Lock()

def load_data():
    """Carga el JSON completo (thread-safe lectura simple)."""
    if not os.path.exists(DATA_FILE):
        # crear estructura básica si no existe
        data = {
            "charging_points": {}, 
            "drivers": {}, 
            "sessions": [],
            "credentials": {},  # {cp_id: {username, password_hash}}
            "encryption_keys": {}  # {cp_id: encryption_key}
        }
        save_data(data)
        return data

    with _lock:
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Asegurar que existen las nuevas claves
                if "credentials" not in data:
                    data["credentials"] = {}
                if "encryption_keys" not in data:
                    data["encryption_keys"] = {}
                return data
        except (json.JSONDecodeError, IOError) as e:
            print(f"[DATA MANAGER] Error loading data: {e}, creating new file")
            data = {
                "charging_points": {}, 
                "drivers": {}, 
                "sessions": [],
                "credentials": {},
                "encryption_keys": {}
            }
            save_data(data)
            return data

def save_data(data):
    """Guarda el JSON atomizando con un lock para evitar corrupciones."""
    with _lock:
        tmp = DATA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, DATA_FILE)

def ensure_cp_exists(cp_id, location=None, price=None):
    """Si no existe el CP en el JSON, lo crea con valores por defecto."""
    data = load_data()
    if cp_id not in data["charging_points"]:
        data["charging_points"][cp_id] = {
            "id": cp_id,
            "location": location or "Unknown",
            "price": float(price) if price is not None else 0.0,
            "state": "DESCONECTADO",
            "current_kw": 0.0,
            "total_kwh": 0.0,
            "current_euros": 0.0,
            "current_driver": None,
            "last_update": datetime.utcnow().isoformat()
        }
        save_data(data)
        print(f"[DATA] CP {cp_id} creado en BD")
    return data["charging_points"][cp_id]

def update_cp(cp_id, **kwargs):
    """Actualiza los campos del CP y guarda."""
    data = load_data()
    if cp_id not in data["charging_points"]:
        print(f"[DATA] WARNING: Intentando actualizar CP {cp_id} que no existe")
        return False
    
    cp = data["charging_points"][cp_id]
    for k, v in kwargs.items():
        cp[k] = v
    cp["last_update"] = datetime.utcnow().isoformat()
    save_data(data)
    return True

def get_all_cps():
    """Devuelve todos los CPs"""
    return load_data()["charging_points"]

def get_cp(cp_id):
    """Devuelve un CP específico"""
    return load_data()["charging_points"].get(cp_id)

def add_session(session_record):
    """Añade un registro de sesión"""
    data = load_data()
    data["sessions"].append(session_record)
    save_data(data)
    return True

def ensure_driver_exists(driver_id):
    """Crea driver si no existe"""
    data = load_data()
    if driver_id not in data["drivers"]:
        data["drivers"][driver_id] = {
            "id": driver_id,
            "created_at": datetime.utcnow().isoformat()
        }
        save_data(data)
    return data["drivers"][driver_id]

def reset_all_cps_to_disconnected():
    """
    Marca todos los CPs como DESCONECTADO al arrancar Central.
    Esto evita tener CPs fantasma en estado ACTIVADO de sesiones anteriores.
    """
    data = load_data()
    cps = data["charging_points"]
    
    if not cps:
        print("[DATA] No hay CPs en la base de datos")
        return 0
    
    count = 0
    for cp_id, cp_data in cps.items():
        if cp_data.get("state") != "DESCONECTADO":
            cp_data["state"] = "DESCONECTADO"
            cp_data["current_driver"] = None
            cp_data["current_kw"] = 0.0
            cp_data["total_kwh"] = 0.0
            cp_data["current_euros"] = 0.0
            cp_data["last_update"] = datetime.utcnow().isoformat()
            count += 1
    
    if count > 0:
        save_data(data)
        print(f"[DATA] ✅ {count} CPs marcados como DESCONECTADO")
    else:
        print("[DATA] Todos los CPs ya estaban en DESCONECTADO")
    
    return count

# ==================== FUNCIONES DE CREDENCIALES ====================

def hash_password(password):
    """Genera hash SHA-256 de una contraseña"""
    return hashlib.sha256(password.encode()).hexdigest()

def set_cp_credentials(cp_id, username, password):
    """
    Guarda las credenciales de un CP (password hasheado)
    """
    data = load_data()
    
    if "credentials" not in data:
        data["credentials"] = {}
    
    data["credentials"][cp_id] = {
        "username": username,
        "password_hash": hash_password(password),
        "created_at": datetime.utcnow().isoformat()
    }
    
    save_data(data)
    print(f"[DATA] Credenciales guardadas para {cp_id}")
    return True

def verify_cp_credentials(cp_id, username, password):
    """
    Verifica las credenciales de un CP
    Returns: True si son válidas, False si no
    """
    data = load_data()
    
    if "credentials" not in data:
        return False
    
    creds = data["credentials"].get(cp_id)
    if not creds:
        return False
    
    return (creds["username"] == username and 
            creds["password_hash"] == hash_password(password))

def generate_encryption_key(cp_id):
    """
    Genera o recupera la clave de cifrado única para un CP
    Returns: clave en base64
    """
    data = load_data()
    
    if "encryption_keys" not in data:
        data["encryption_keys"] = {}
    
    # Si ya tiene clave, devolverla
    if cp_id in data["encryption_keys"]:
        return data["encryption_keys"][cp_id]
    
    # Generar nueva clave (32 bytes = 256 bits)
    key = secrets.token_bytes(32)
    key_b64 = base64.b64encode(key).decode('utf-8')
    
    data["encryption_keys"][cp_id] = key_b64
    save_data(data)
    
    print(f"[DATA] Clave de cifrado generada para {cp_id}")
    return key_b64

def revoke_encryption_key(cp_id):
    """
    Revoca la clave de cifrado de un CP (forzará re-autenticación)
    """
    data = load_data()
    
    if "encryption_keys" not in data:
        return False
    
    if cp_id in data["encryption_keys"]:
        del data["encryption_keys"][cp_id]
        save_data(data)
        print(f"[DATA] Clave de cifrado revocada para {cp_id}")
        return True
    
    return False

def delete_cp(cp_id):
    """
    Elimina un CP del sistema (incluye credenciales y claves)
    """
    data = load_data()
    
    deleted = False
    
    # Eliminar CP
    if cp_id in data["charging_points"]:
        del data["charging_points"][cp_id]
        deleted = True
    
    # Eliminar credenciales
    if "credentials" in data and cp_id in data["credentials"]:
        del data["credentials"][cp_id]
    
    # Eliminar clave de cifrado
    if "encryption_keys" in data and cp_id in data["encryption_keys"]:
        del data["encryption_keys"][cp_id]
    
    if deleted:
        save_data(data)
        print(f"[DATA] CP {cp_id} eliminado completamente")
    
    return deleted