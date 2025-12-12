#!/usr/bin/env python3
"""
EV_W - Weather Control Office
Monitoriza el clima en las localizaciones de los CPs
y notifica a Central cuando hay condiciones adversas (temp < 0°C)

CONFIGURACIÓN VÍA VARIABLES DE ENTORNO:
  - OPENWEATHER_API_KEY: API key de OpenWeatherMap (OBLIGATORIO)
  - CENTRAL_API_URL: URL de Central API (default: http://localhost:8000/api/charging_points)
  - WEATHER_CHECK_INTERVAL: Intervalo de chequeo en segundos (default: 4)
  - MIN_TEMP: Temperatura mínima en °C (default: 0.0)
  - CP_LOCATIONS: Localizaciones en formato "CP001:Valencia,CP002:Madrid,CP003:Barcelona"
"""

import os
import time
import json
import requests
import threading
from datetime import datetime

# ==================== CONFIGURACIÓN ====================

# OpenWeatherMap API
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', 'TU_API_KEY_AQUI')
OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

# Central API
CENTRAL_API_URL = os.getenv('CENTRAL_API_URL', 'http://localhost:8000/api/charging_points')

# Intervalo de chequeo (en segundos)
CHECK_INTERVAL = int(os.getenv('WEATHER_CHECK_INTERVAL', '4'))

# Temperatura mínima (en °C)
MIN_TEMP = float(os.getenv('MIN_TEMP', '0.0'))

# Localizaciones desde variable de entorno
# Formato: "CP001:Valencia,CP002:Madrid,CP003:Barcelona"
CP_LOCATIONS_STR = os.getenv('CP_LOCATIONS', '')

# ==================== CLASE PRINCIPAL ====================

class WeatherControlOffice:
    def __init__(self):
        self.api_key = OPENWEATHER_API_KEY
        self.central_url = CENTRAL_API_URL
        self.check_interval = CHECK_INTERVAL
        self.min_temp = MIN_TEMP
        
        # Localizaciones a monitorizar (CP_ID -> city)
        self.locations = {}
        
        # Estado de alertas (CP_ID -> alerted: bool)
        self.alerts = {}
        
        # Control
        self.running = True
        
        # Cargar localizaciones desde variable de entorno
        self.load_locations_from_env()
        
        # Cargar localizaciones desde variable de entorno
        self._load_locations_from_env()
        
    def _load_locations_from_env(self):
        """Carga localizaciones desde variable de entorno CP_LOCATIONS"""
        if not CP_LOCATIONS_STR:
            return
        
        try:
            # Formato: "CP001:Valencia,CP002:Madrid,CP003:Barcelona"
            for entry in CP_LOCATIONS_STR.split(','):
                entry = entry.strip()
                if ':' in entry:
                    cp_id, city = entry.split(':', 1)
                    cp_id = cp_id.strip()
                    city = city.strip()
                    if cp_id and city:
                        self.locations[cp_id] = city
                        self.alerts[cp_id] = False
                        self.log(f"✓ Cargada desde ENV: {cp_id} → {city}")
            
            if self.locations:
                self.log(f"Total localizaciones desde ENV: {len(self.locations)}")
        except Exception as e:
            self.log(f"✗ Error parseando CP_LOCATIONS: {e}")
        
    def log(self, msg):
        """Log con timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{timestamp}] [WEATHER] {msg}')
    
    # ==================== GESTIÓN DE LOCALIZACIONES ====================
    
    def load_locations_from_env(self):
        """
        Carga localizaciones desde variable de entorno WEATHER_LOCATIONS
        Formato: "CP001:Valencia,CP002:Madrid,CP003:Barcelona"
        """
        locations_env = os.getenv('WEATHER_LOCATIONS', '')
        
        if not locations_env:
            self.log("⚠️  No hay localizaciones en WEATHER_LOCATIONS")
            return False
        
        try:
            # Parse: "CP001:Valencia,CP002:Madrid"
            pairs = locations_env.split(',')
            
            for pair in pairs:
                pair = pair.strip()
                if ':' not in pair:
                    continue
                
                cp_id, city = pair.split(':', 1)
                cp_id = cp_id.strip()
                city = city.strip()
                
                if cp_id and city:
                    self.locations[cp_id] = city
                    self.alerts[cp_id] = False
                    self.log(f"✓ Cargada: {cp_id} → {city}")
            
            self.log(f"Total localizaciones desde ENV: {len(self.locations)}")
            return True
            
        except Exception as e:
            self.log(f"✗ Error parseando WEATHER_LOCATIONS: {e}")
            return False
    
    def load_locations_from_file(self, filename='locations.txt'):
        """Carga localizaciones desde archivo"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    parts = line.split(',')
                    if len(parts) >= 2:
                        cp_id = parts[0].strip()
                        city = parts[1].strip()
                        self.locations[cp_id] = city
                        self.alerts[cp_id] = False
                        self.log(f"✓ Cargada: {cp_id} → {city}")
            
            self.log(f"Total localizaciones: {len(self.locations)}")
            return True
            
        except FileNotFoundError:
            self.log(f"⚠️  Archivo {filename} no encontrado")
            return False
        except Exception as e:
            self.log(f"✗ Error cargando localizaciones: {e}")
            return False
    
    def load_locations_from_central(self):
        """Obtiene localizaciones desde Central API"""
        try:
            response = requests.get(self.central_url, timeout=5)
            
            if response.status_code == 200:
                cps = response.json()
                
                if not cps:
                    self.log("⚠️  Central no tiene CPs registrados")
                    return False
                
                # Limpiar localizaciones anteriores
                self.locations.clear()
                self.alerts.clear()
                
                for cp_id, cp_data in cps.items():
                    location = cp_data.get('location', 'Unknown')
                    state = cp_data.get('state', 'DESCONECTADO')
                    
                    if location != 'Unknown':
                        self.locations[cp_id] = location
                        self.alerts[cp_id] = False
                        self.log(f"  ✓ {cp_id}: {location:20s} (estado: {state})")
                    else:
                        self.log(f"  ⚠️  {cp_id}: Sin ubicación definida (ignorado)")
                
                if not self.locations:
                    self.log("⚠️  Ningún CP tiene ubicación definida")
                    return False
                
                return True
            else:
                self.log(f"✗ Error consultando Central: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            self.log(f"✗ No se pudo conectar con Central en: {self.central_url}")
            self.log(f"ℹ️  Verifica que Central esté ejecutándose")
            return False
        except requests.exceptions.Timeout:
            self.log(f"✗ Timeout conectando con Central")
            return False
        except Exception as e:
            self.log(f"✗ Error conectando con Central: {e}")
            return False
    
    def add_location(self, cp_id, city):
        """Añade una localización manualmente"""
        self.locations[cp_id] = city
        self.alerts[cp_id] = False
        self.log(f"✓ Añadida: {cp_id} → {city}")
    
    def remove_location(self, cp_id):
        """Elimina una localización"""
        if cp_id in self.locations:
            city = self.locations[cp_id]
            del self.locations[cp_id]
            if cp_id in self.alerts:
                del self.alerts[cp_id]
            self.log(f"✓ Eliminada: {cp_id} → {city}")
            return True
        return False
    
    def list_locations(self):
        """Lista todas las localizaciones"""
        if not self.locations:
            self.log("No hay localizaciones configuradas")
            return
        
        self.log("="*60)
        self.log("LOCALIZACIONES MONITORIZADAS:")
        self.log("="*60)
        for cp_id, city in self.locations.items():
            alert_status = "🔴 ALERTA" if self.alerts.get(cp_id) else "🟢 OK"
            self.log(f"  {cp_id}: {city:20s} {alert_status}")
        self.log("="*60)
    
    # ==================== CONSULTA CLIMA ====================
    
    def get_weather(self, city):
        """Consulta clima de una ciudad en OpenWeatherMap"""
        try:
            params = {
                'q': city,
                'appid': self.api_key,
                'units': 'metric'  # Temperatura en Celsius
            }
            
            response = requests.get(OPENWEATHER_URL, params=params, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                temp = data['main']['temp']
                weather = data['weather'][0]['description']
                return {
                    'temp': temp,
                    'description': weather,
                    'success': True
                }
            else:
                self.log(f"✗ Error API OpenWeather ({city}): {response.status_code}")
                return {'success': False, 'error': response.status_code}
                
        except requests.exceptions.Timeout:
            self.log(f"✗ Timeout consultando clima de {city}")
            return {'success': False, 'error': 'timeout'}
        except Exception as e:
            self.log(f"✗ Error consultando {city}: {e}")
            return {'success': False, 'error': str(e)}
    
    # ==================== NOTIFICACIÓN A CENTRAL ====================
    
    def notify_central(self, cp_id, action):
        """
        Notifica a Central sobre estado del clima
        action: 'stop' (temp < 0°C) o 'resume' (temp >= 0°C)
        """
        try:
            url = f"{self.central_url}/{cp_id}/command"
            payload = {'action': action}
            
            response = requests.post(url, json=payload, timeout=5)
            
            if response.status_code == 200:
                self.log(f"✓ Central notificada: {cp_id} → {action.upper()}")
                return True
            else:
                self.log(f"✗ Error notificando Central ({cp_id}): {response.status_code}")
                return False
                
        except Exception as e:
            self.log(f"✗ Error notificando Central: {e}")
            return False
    
    # ==================== MONITORIZACIÓN ====================
    
    def check_weather_all(self):
        """Verifica clima de todas las localizaciones"""
        if not self.locations:
            return
        
        for cp_id, city in self.locations.items():
            # Consultar clima
            weather = self.get_weather(city)
            
            if not weather['success']:
                continue
            
            temp = weather['temp']
            description = weather['description']
            
            # Log
            temp_icon = "❄️" if temp < 0 else "🌡️"
            self.log(f"{temp_icon} {city:15s} ({cp_id}): {temp:5.1f}°C - {description}")
            
            # Verificar temperatura
            currently_alerted = self.alerts.get(cp_id, False)
            
            if temp < self.min_temp and not currently_alerted:
                # ALERTA: Temperatura baja
                self.log(f"⚠️  ALERTA FRIO: {city} ({cp_id}) - {temp:.1f}°C < {self.min_temp}°C")
                self.notify_central(cp_id, 'stop')
                self.alerts[cp_id] = True
                
            elif temp >= self.min_temp and currently_alerted:
                # Temperatura recuperada
                self.log(f"✓ Temperatura normal: {city} ({cp_id}) - {temp:.1f}°C")
                self.notify_central(cp_id, 'resume')
                self.alerts[cp_id] = False
    
    def monitoring_loop(self):
        """Loop principal de monitorización"""
        self.log("Iniciando loop de monitorización...")
        
        while self.running:
            try:
                self.check_weather_all()
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.log(f"✗ Error en monitoring loop: {e}")
                time.sleep(self.check_interval)
    
    # ==================== MENÚ INTERACTIVO ====================
    
    def show_menu(self):
        """Muestra menú de opciones"""
        print("\n" + "="*60)
        print("WEATHER CONTROL OFFICE - MENÚ")
        print("="*60)
        print("[1] Ver localizaciones actuales")
        print("[2] Añadir localización manual (CP_ID, Ciudad)")
        print("[3] Eliminar localización (CP_ID)")
        print("[4] 🔄 Recargar desde Central API (recomendado)")
        print("[5] Forzar chequeo inmediato")
        print("[6] Ver estado de alertas")
        print("[q] Salir")
        print("="*60)
        print("TIP: Usa [4] para actualizar CPs después de registrar nuevos en Central")
        print("="*60)
    
    def interactive_menu(self):
        """Menú interactivo (thread separado)"""
        while self.running:
            try:
                self.show_menu()
                choice = input("Opción: ").strip().lower()
                
                if choice == '1':
                    self.list_locations()
                
                elif choice == '2':
                    cp_id = input("CP_ID: ").strip()
                    city = input("Ciudad: ").strip()
                    if cp_id and city:
                        self.add_location(cp_id, city)
                
                elif choice == '3':
                    cp_id = input("CP_ID a eliminar: ").strip()
                    self.remove_location(cp_id)
                
                elif choice == '4':
                    self.log("🔄 Recargando CPs desde Central API...")
                    if self.load_locations_from_central():
                        self.log(f"✅ {len(self.locations)} CPs recargados correctamente")
                    else:
                        self.log("❌ No se pudieron recargar los CPs")
                
                elif choice == '5':
                    self.log("Ejecutando chequeo inmediato...")
                    self.check_weather_all()
                
                elif choice == '6':
                    self.list_locations()
                
                elif choice == 'q':
                    self.log("Saliendo...")
                    self.running = False
                    break
                
                else:
                    print("Opción inválida")
                
                time.sleep(1)
                
            except EOFError:
                time.sleep(0.5)
            except KeyboardInterrupt:
                self.running = False
                break
    
    # ==================== EJECUCIÓN ====================
    
    def run(self):
        """Función principal"""
        self.log("="*60)
        self.log("EV_W - Weather Control Office INICIANDO")
        self.log("="*60)
        self.log(f"OpenWeather API: {'Configurado ✓' if self.api_key != 'TU_API_KEY_AQUI' else '⚠️  NO configurado'}")
        self.log(f"Central API: {self.central_url}")
        self.log(f"Intervalo chequeo: {self.check_interval} segundos")
        self.log(f"Temperatura mínima: {self.min_temp}°C")
        self.log("="*60)
        
        # Verificar API key
        if self.api_key == 'TU_API_KEY_AQUI':
            self.log("⚠️  API Key de OpenWeather NO configurada")
            self.log("⚠️  Establece la variable: OPENWEATHER_API_KEY")
            self.log("⚠️  Obtén tu API key en: https://openweathermap.org/api")
            
            choice = input("\n¿Continuar de todos modos? (s/n): ").strip().lower()
            if choice != 's':
                return
        
        # Cargar localizaciones SIEMPRE desde Central primero
        self.log("\n🔄 Cargando localizaciones desde Central API...")
        if self.load_locations_from_central():
            self.log(f"✓ {len(self.locations)} CPs cargados desde Central")
        else:
            # Si falla Central, intentar desde variable de entorno como fallback
            self.log("⚠️  No se pudo conectar con Central")
            if self.locations:
                self.log(f"ℹ️  Usando {len(self.locations)} CPs desde WEATHER_LOCATIONS (fallback)")
            else:
                self.log("⚠️  No hay localizaciones disponibles")
                self.log("⚠️  Verifica que Central esté ejecutándose")
                self.log("⚠️  O configura WEATHER_LOCATIONS='CP001:Valencia,CP002:Madrid'")
        
        # Iniciar thread de monitorización
        monitor_thread = threading.Thread(target=self.monitoring_loop, daemon=True)
        monitor_thread.start()
        self.log("✓ Thread de monitorización iniciado")
        
        # Menú interactivo (blocking)
        try:
            self.interactive_menu()
        except KeyboardInterrupt:
            self.log("\nInterrumpido por usuario")
        
        self.running = False
        self.log("Weather Control Office finalizado")

# ==================== MAIN ====================

if __name__ == '__main__':
    weather = WeatherControlOffice()
    weather.run()