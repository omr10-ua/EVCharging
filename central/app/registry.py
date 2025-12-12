"""
EV_Registry - API REST para registro y baja de Charging Points
Proporciona endpoints para:
- Registrar nuevo CP (devuelve credenciales)
- Dar de baja CP
- Autenticar CP (valida credenciales)
"""

import secrets
import hashlib
from flask import Blueprint, request, jsonify
from .data_manager import (
    ensure_cp_exists, 
    get_cp, 
    update_cp, 
    delete_cp,
    set_cp_credentials,
    verify_cp_credentials,
    generate_encryption_key
)

# Blueprint para las rutas del Registry
registry_bp = Blueprint('registry', __name__, url_prefix='/api/registry')

def generate_credentials():
    """
    Genera credenciales seguras para un CP
    Returns: (username, password)
    """
    username = secrets.token_urlsafe(8)
    password = secrets.token_urlsafe(16)
    return username, password

# ==================== ENDPOINTS ====================

@registry_bp.route('/register', methods=['POST'])
def register_cp():
    """
    Registra un nuevo CP en el sistema
    
    Body JSON:
    {
        "cp_id": "CP001",
        "location": "Valencia Centro",
        "price": 0.35
    }
    
    Response:
    {
        "status": "ok",
        "cp_id": "CP001",
        "credentials": {
            "username": "abc123",
            "password": "xyz789..."
        }
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        cp_id = data.get('cp_id')
        location = data.get('location', 'Unknown')
        price = data.get('price', 0.35)
        
        if not cp_id:
            return jsonify({"error": "cp_id is required"}), 400
        
        # Verificar si ya existe
        existing = get_cp(cp_id)
        force = data.get('force', False)  # ✅ Permitir forzar re-registro
        
        if existing and not force:
            return jsonify({
                "error": "CP already exists. Use 'force': true to re-register",
                "cp_id": cp_id
            }), 409
        
        # Si force=true y existe, eliminar credenciales antiguas
        if existing and force:
            print(f"[REGISTRY] Re-registrando CP {cp_id} (force=true)")
        
        # Crear o actualizar CP en BD
        ensure_cp_exists(cp_id, location=location, price=price)
        
        # Generar credenciales
        username, password = generate_credentials()
        
        # Guardar credenciales (hash)
        set_cp_credentials(cp_id, username, password)
        
        print(f"[REGISTRY] ✅ CP {cp_id} registrado - User: {username}")
        
        return jsonify({
            "status": "ok",
            "cp_id": cp_id,
            "location": location,
            "price": price,
            "credentials": {
                "username": username,
                "password": password
            }
        }), 201
        
    except Exception as e:
        print(f"[REGISTRY] ❌ Error en register: {e}")
        return jsonify({"error": str(e)}), 500


@registry_bp.route('/unregister', methods=['DELETE'])
def unregister_cp():
    """
    Da de baja un CP del sistema
    
    Body JSON:
    {
        "cp_id": "CP001"
    }
    
    Response:
    {
        "status": "ok",
        "cp_id": "CP001",
        "message": "CP unregistered"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        cp_id = data.get('cp_id')
        
        if not cp_id:
            return jsonify({"error": "cp_id is required"}), 400
        
        # Verificar si existe
        cp = get_cp(cp_id)
        if not cp:
            return jsonify({
                "error": "CP not found",
                "cp_id": cp_id
            }), 404
        
        # Eliminar CP
        delete_cp(cp_id)
        
        print(f"[REGISTRY] ✅ CP {cp_id} dado de baja")
        
        return jsonify({
            "status": "ok",
            "cp_id": cp_id,
            "message": "CP unregistered successfully"
        }), 200
        
    except Exception as e:
        print(f"[REGISTRY] ❌ Error en unregister: {e}")
        return jsonify({"error": str(e)}), 500


@registry_bp.route('/authenticate', methods=['POST'])
def authenticate_cp():
    """
    Autentica un CP y devuelve su clave de cifrado
    
    Body JSON:
    {
        "cp_id": "CP001",
        "username": "abc123",
        "password": "xyz789..."
    }
    
    Response (éxito):
    {
        "status": "authenticated",
        "cp_id": "CP001",
        "encryption_key": "base64_key..."
    }
    
    Response (fallo):
    {
        "status": "denied",
        "reason": "Invalid credentials"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        cp_id = data.get('cp_id')
        username = data.get('username')
        password = data.get('password')
        
        if not all([cp_id, username, password]):
            return jsonify({
                "status": "denied",
                "reason": "Missing credentials"
            }), 400
        
        # Verificar credenciales
        if not verify_cp_credentials(cp_id, username, password):
            print(f"[REGISTRY] ❌ Autenticación fallida para {cp_id}")
            return jsonify({
                "status": "denied",
                "reason": "Invalid credentials"
            }), 401
        
        # Generar/obtener clave de cifrado
        encryption_key = generate_encryption_key(cp_id)
        
        # Actualizar estado a ACTIVADO
        update_cp(cp_id, state="ACTIVADO")
        
        print(f"[REGISTRY] ✅ CP {cp_id} autenticado correctamente")
        
        return jsonify({
            "status": "authenticated",
            "cp_id": cp_id,
            "encryption_key": encryption_key
        }), 200
        
    except Exception as e:
        print(f"[REGISTRY] ❌ Error en authenticate: {e}")
        return jsonify({"error": str(e)}), 500


@registry_bp.route('/status/<cp_id>', methods=['GET'])
def get_cp_status(cp_id):
    """
    Obtiene el estado de registro de un CP
    
    Response:
    {
        "cp_id": "CP001",
        "registered": true,
        "state": "ACTIVADO",
        "location": "Valencia Centro"
    }
    """
    try:
        cp = get_cp(cp_id)
        
        if not cp:
            return jsonify({
                "cp_id": cp_id,
                "registered": False
            }), 404
        
        return jsonify({
            "cp_id": cp_id,
            "registered": True,
            "state": cp.get('state', 'UNKNOWN'),
            "location": cp.get('location', 'Unknown'),
            "price": cp.get('price', 0.0)
        }), 200
        
    except Exception as e:
        print(f"[REGISTRY] ❌ Error en status: {e}")
        return jsonify({"error": str(e)}), 500