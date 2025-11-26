#!/usr/bin/env python3
"""
Funciones helper para los CPs (Engine y Monitor)
"""
import json
import time

def log_message(msg):
    """Función simple de logging con timestamp"""
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{ts}] {msg}')

def serialize_json(data):
    """Serializa un diccionario a JSON"""
    try:
        return json.dumps(data)
    except Exception as e:
        log_message(f'[HELPER] Error serializando JSON: {e}')
        return '{}'
