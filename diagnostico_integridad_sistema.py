# -*- coding: utf-8 -*-
"""
SCRIPT DE DIAGNÓSTICO DE INTEGRIDAD INSTITUCIONAL - DITIC
Archivo: diagnostico_integridad_sistema.py
Versión: V82.9.34 (Refactorización a Código Limpio)
Lógica: Verifica la carga del archivo .env y la conectividad con SQL Server.
"""

# Importamos la nueva configuración institucional
from configuracion_sistema_encuestas import ConfiguracionSistema
import os
from dotenv import load_dotenv

# Cargamos las variables de entorno
load_dotenv()

print("\n" + "="*55)
print("🔍 DIAGNÓSTICO DE INTEGRIDAD - ENCUESTA DE PERCEPCIÓN")
print("="*55)

# 1. Verificación del Archivo de Secretos
existe_env = os.path.exists('.env')
print(f"✅ Archivo .env detectado localmente: {'SÍ' if existe_env else 'NO'}")

# 2. Verificación de Variables Cargadas en Memoria
print(f"✅ Servidor SQL Origen: {ConfiguracionSistema.SERVIDOR_BASE_DATOS_ORIGEN}")
client_id = ConfiguracionSistema.IDENTIFICADOR_CLIENTE_SHAREPOINT
print(f"✅ SharePoint Client ID: {client_id[:5]}*** (Oculto por seguridad)")

# 3. Prueba de Fuego: Conexión Real a SQL Server
try:
    # Usamos el nuevo método explícito
    motor_sql = ConfiguracionSistema.obtener_motor_base_datos_origen()
    with motor_sql.connect() as conexion:
        print("✅ Conexión a SQL Server (SQLAppsWeb): EXITOSA")
except Exception as error_tecnico:
    print(f"❌ Error Crítico en SQL: {error_tecnico}")

print("="*55 + "\n")