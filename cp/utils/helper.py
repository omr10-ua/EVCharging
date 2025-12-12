#!/usr/bin/env python3
"""
Funciones helper para los CPs (Engine y Monitor)
"""
import json
import time
import os

def log_message(msg, prefix="CP"):
    """Función simple de logging con timestamp"""
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{ts}] [{prefix}] {msg}')

def serialize_json(data):
    """Serializa un diccionario a JSON"""
    try:
        return json.dumps(data)
    except Exception as e:
        log_message(f'Error serializando JSON: {e}', 'HELPER')
        return '{}'

def deserialize_json(json_str):
    """Deserializa JSON string a diccionario"""
    try:
        return json.loads(json_str)
    except Exception as e:
        log_message(f'Error deserializando JSON: {e}', 'HELPER')
        return {}

# ==================== GESTIÓN DE CONFIGURACIÓN ====================

def load_config(config_file='cp_config.json'):
    """
    Carga la configuración del CP desde archivo JSON.
    Si no existe, retorna None (se usarán variables de entorno).
    """
    # Buscar en el directorio padre (cp/)
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), config_file)
    
    if not os.path.exists(config_path):
        return None  # No error, simplemente no hay archivo
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            log_message(f'✅ Configuración cargada: {config_path}', 'CONFIG')
            return config
    except json.JSONDecodeError as e:
        log_message(f'❌ Error JSON en configuración: {e}', 'CONFIG')
        return None
    except Exception as e:
        log_message(f'❌ Error cargando configuración: {e}', 'CONFIG')
        return None

def save_config(config, config_file='cp_config.json'):
    """Guarda la configuración del CP en archivo JSON"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), config_file)
    
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        log_message(f'✅ Configuración guardada: {config_path}', 'CONFIG')
        return True
    except Exception as e:
        log_message(f'❌ Error guardando configuración: {e}', 'CONFIG')
        return False

def update_config(key, value, config_file='cp_config.json'):
    """Actualiza un campo específico de la configuración"""
    config = load_config(config_file)
    if config:
        config[key] = value
        return save_config(config, config_file)
    return False

def save_credentials(username, password, config_file='cp_config.json'):
    """Guarda las credenciales recibidas del Registry"""
    config = load_config(config_file)
    if config:
        config['credentials'] = {
            'username': username,
            'password': password
        }
        return save_config(config, config_file)
    return False

def save_encryption_key(key, config_file='cp_config.json'):
    """Guarda la clave de cifrado recibida de Central"""
    return update_config('encryption_key', key, config_file)

def clear_credentials(config_file='cp_config.json'):
    """Limpia las credenciales (para re-registro)"""
    config = load_config(config_file)
    if config:
        config['credentials'] = None
        config['encryption_key'] = None
        return save_config(config, config_file)
    return False