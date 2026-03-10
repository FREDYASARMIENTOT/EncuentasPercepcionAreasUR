# -*- coding: utf-8 -*-
"""
GESTOR DE TRAZABILIDAD, RENDIMIENTO Y AUDITORÍA - DITIC
Archivo: gestion_trazabilidad_auditoria.py
Versión: V82.9.35 (Saneado de Caracteres Invisibles)
Autor: Mg. Fredy Alejandro Sarmiento Torres
Analista de Información - Universidad del Rosario
"""

import os
import logging
import time
import datetime
import psutil
import traceback
from functools import wraps
from pathlib import Path
from sqlalchemy import text
from collections import defaultdict
from contextlib import contextmanager

class GestorRegistroEventos:
    """Clase encargada de configurar la escritura de eventos en consola y archivos .log."""
    
    @staticmethod
    def configurar_registro_eventos(directorio_registros, nombre_script="Ejecucion"):
        """Configura el motor de logs asegurando codificación UTF-8."""
        ruta_directorio = Path(directorio_registros)
        ruta_directorio.mkdir(parents=True, exist_ok=True)
            
        marca_tiempo = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        archivo_log_completo = ruta_directorio / f"{nombre_script}_{marca_tiempo}.log"
        
        # Limpieza de manejadores previos para evitar duplicidad de mensajes
        for manejador in logging.root.handlers[:]:
            logging.root.removeHandler(manejador)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | [%(module)s:%(funcName)s] %(message)s',
            handlers=[
                logging.FileHandler(archivo_log_completo, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(), str(archivo_log_completo)

class MonitorRendimientoSistema:
    """Motor de telemetría para tiempos de ejecución y consumo de RAM."""
    
    def __init__(self, registrador_eventos):
        self.registrador_eventos = registrador_eventos
        self.metricas_rendimiento = defaultdict(lambda: {'conteo': 0, 'tiempo_total': 0, 'errores': 0, 'pico_memoria': 0})

    def _obtener_uso_memoria_ram_mb(self):
        """Obtiene el consumo de RAM del proceso actual en Megabytes."""
        try:
            return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        except:
            return 0

    @contextmanager
    def medir_rendimiento_bloque(self, nombre_etapa):
        """Bloque 'with' para medir performance de fragmentos de código."""
        tiempo_inicio = time.time()
        memoria_antes = self._obtener_uso_memoria_ram_mb()
        try:
            yield
            
            tiempo_transcurrido = time.time() - tiempo_inicio
            memoria_despues = self._obtener_uso_memoria_ram_mb()
            diferencia_memoria = memoria_despues - memoria_antes
            
            self.metricas_rendimiento[nombre_etapa]['conteo'] += 1
            self.metricas_rendimiento[nombre_etapa]['tiempo_total'] += tiempo_transcurrido
            
            if memoria_despues > self.metricas_rendimiento[nombre_etapa]['pico_memoria']:
                self.metricas_rendimiento[nombre_etapa]['pico_memoria'] = memoria_despues
                
            self.registrador_eventos.info(
                f"⏱️  [{nombre_etapa}] {tiempo_transcurrido:.2f}s | "
                f"Delta RAM: {diferencia_memoria:+.2f}MB | Pico: {memoria_despues:.1f}MB"
            )
        except Exception as e:
            self.metricas_rendimiento[nombre_etapa]['errores'] += 1
            self.registrador_eventos.error(f"❌ Error en [{nombre_etapa}]: {str(e)}")
            raise

    def generar_resumen_rendimiento(self):
        """Construye el reporte tabular final de rendimiento."""
        lineas = ["\n" + "="*70, "📊 REPORTE INSTITUCIONAL DE RENDIMIENTO DE ETL", "="*70]
        for etapa, datos in sorted(self.metricas_rendimiento.items()):
            promedio = datos['tiempo_total'] / datos['conteo'] if datos['conteo'] > 0 else 0
            lineas.append(
                f"• {etapa:25} | Ciclos: {datos['conteo']:3} | "
                f"Promedio: {promedio:6.2f}s | Pico RAM: {datos['pico_memoria']:7.1f}MB"
            )
        lineas.append("="*70)
        return "\n".join(lineas)

class AuditorBaseDatos:
    """Inserta registros de éxito/fracaso en la tabla Log_ExportacionEncuestas."""
    
    def __init__(self, motor_base_datos, version_script, registrador_eventos):
        self.motor_base_datos = motor_base_datos
        self.version_script = version_script
        self.registrador_eventos = registrador_eventos

    def insertar_registro_auditoria(self, anio, mes, area, total, exportados, estado, mensaje, archivo, sp_ok):
        """Escribe la traza en SQL Server de manera segura."""
        if not self.motor_base_datos:
            self.registrador_eventos.warning("⚠️ Auditoría SQL omitida: No hay motor de base de datos.")
            return
        
        try:
            consulta_sql = text("""
                INSERT INTO chatbot.Log_ExportacionEncuestas 
                (FechaEjecucion, ScriptVersion, Anio, Mes, Area, TotalRegistros, Estado, Mensaje, ArchivoDestino, SharePointUpload, FechaUpload) 
                VALUES (GETDATE(), :version, :anio, :mes, :area, :total, :estado, :mensaje, :archivo, :sp, GETDATE())
            """)
            with self.motor_base_datos.begin() as conexion:
                conexion.execute(consulta_sql, {
                    "version": self.version_script,
                    "anio": anio,
                    "mes": mes,
                    "area": area,
                    "total": total,
                    "estado": estado,
                    "mensaje": str(mensaje)[:500],
                    "archivo": os.path.basename(archivo),
                    "sp": 1 if sp_ok else 0
                })
            self.registrador_eventos.info(f"💾 Auditoría SQL completada para: {area}.")
        except Exception as e:
            self.registrador_eventos.error(f"⚠️ Fallo en auditoría SQL: {e}")

def decorador_registrar_paso_rendimiento(monitor_rendimiento, nombre_etapa):
    """Decorador para monitoreo automático de funciones."""
    def decorador_interno(funcion_original):
        @wraps(funcion_original)
        def envoltorio(*args, **kwargs):
            with monitor_rendimiento.medir_rendimiento_bloque(nombre_etapa):
                return funcion_original(*args, **kwargs)
        return envoltorio
    return decorador_interno