# -*- coding: utf-8 -*-
"""
GESTOR DE LOGS Y PERFORMANCE - DITIC
Versión: V82.0 Sovereign Elite
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

class LogManager:
    """Configuración de Logging con soporte para rotación y encoding robusto."""
    @staticmethod
    def configurar_log(log_dir, script_name="Ejecucion"):
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
            
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_path / f"{script_name}_{timestamp}.log"
        
        # Limpieza de handlers para evitar duplicidad en loops
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | [%(module)s:%(funcName)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(), str(log_file)

class PerformanceMonitor:
    """Motor de métricas con seguimiento de recursos en tiempo real."""
    def __init__(self, logger):
        self.logger = logger
        self.metrics = defaultdict(lambda: {'count': 0, 'total_time': 0, 'errors': 0, 'peak_mem': 0})

    def _get_memory_usage(self):
        """Obtiene el uso actual de RAM en MB."""
        try:
            return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        except:
            return 0

    @contextmanager
    def measure(self, stage_name):
        """Context Manager para telemetría de procesos."""
        start_time = time.time()
        mem_before = self._get_memory_usage()
        try:
            yield
            elapsed = time.time() - start_time
            mem_after = self._get_memory_usage()
            mem_delta = mem_after - mem_before
            
            self.metrics[stage_name]['count'] += 1
            self.metrics[stage_name]['total_time'] += elapsed
            if mem_after > self.metrics[stage_name]['peak_mem']:
                self.metrics[stage_name]['peak_mem'] = mem_after
                
            self.logger.info(f"⏱️  [{stage_name}] {elapsed:.2f}s | Delta RAM: {mem_delta:+.2f}MB | Pico: {mem_after:.1f}MB")
        except Exception as e:
            self.metrics[stage_name]['errors'] += 1
            self.logger.error(f"❌ Error en [{stage_name}]: {str(e)}")
            raise

    def get_summary(self):
        """Genera un reporte ejecutivo de performance."""
        summary = ["\n" + "="*70, "📊 REPORTE DE PERFORMANCE SOBERANO", "="*70]
        for stage, data in sorted(self.metrics.items()):
            avg = data['total_time'] / data['count'] if data['count'] > 0 else 0
            summary.append(f"• {stage:25} | Ciclos: {data['count']:3} | Avg: {avg:6.2f}s | Pico RAM: {data['peak_mem']:7.1f}MB")
        summary.append("="*70)
        return "\n".join(summary)

class DBLogger:
    """Gestor de auditoría persistente en SQL Server."""
    def __init__(self, engine_log, script_version, logger):
        self.engine = engine_log
        self.version = script_version
        self.logger = logger

    def insertar(self, anio, mes, area, total, exportados, estado, mensaje, archivo, sp_ok):
        if not self.engine:
            self.logger.warning("⚠️ Auditoría DB omitida (Sin conexión).")
            return
        
        try:
            sql = text("""
                INSERT INTO chatbot.Log_ExportacionEncuestas 
                (FechaEjecucion, ScriptVersion, Anio, Mes, Area, TotalRegistros, Estado, Mensaje, ArchivoDestino, SharePointUpload, FechaUpload) 
                VALUES (GETDATE(), :v, :a, :m, :ar, :t, :e, :msg, :f, :sp, GETDATE())
            """)
            with self.engine.begin() as conn:
                conn.execute(sql, {
                    "v": self.version, "a": anio, "m": mes, "ar": area, 
                    "t": total, "e": estado, "msg": str(mensaje)[:500], 
                    "f": os.path.basename(archivo), "sp": 1 if sp_ok else 0
                })
            self.logger.info(f"💾 Auditoría DB completada para {area}.")
        except Exception as e:
            self.logger.error(f"⚠️ Fallo en persistencia de log: {e}")

def log_step(perf_monitor, stage_name):
    """Decorador para automatizar el monitoreo de funciones críticas."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with perf_monitor.measure(stage_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator