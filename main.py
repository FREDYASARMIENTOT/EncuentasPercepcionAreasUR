# -*- coding: utf-8 -*-
"""
ORQUESTADOR CENTRAL DE ENCUESTAS - DITIC
Versión: V82.9.27 Encueta Percepcion (Fix: SharePoint Arg Conflict)
Autor: Mg. Fredy Alejandro Sarmiento Torres
"""

import argparse
import sys
import datetime
import traceback
import gc
import time
import pandas as pd
from sqlalchemy import text
from pathlib import Path

# Importación de Módulos de la Arquitectura Sovereign
from config import Config
from gestorlogs import LogManager, PerformanceMonitor, DBLogger
from generarexcelmensual import ExcelSovereignReporter, DataProcessor
from generarexcelacumulativo import ExcelAcumuladoReporter
from gestornotificaciones import NotificationManager
from gestorsharepoint import SharePointManager 

# --------------------------------------------------------------------------
# 🔌 LÓGICA DE CONEXIÓN Y DATOS (DATA ENGINE)
# --------------------------------------------------------------------------
class DataEngine:
    @staticmethod
    def extraer_datos(anio, mes, area):
        """Extrae información cruda desde SQLAppsWeb."""
        query = text(f"SELECT * FROM {Config.SQL_VIEW} WHERE Año=:a AND Mes=:m AND areaNombre=:ar")
        engine = Config.get_engine_data()
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params={'a': anio, 'm': mes, 'ar': area})

    @staticmethod
    def descubrir_areas(anio):
        """Identifica dinámicamente qué áreas procesar."""
        query = text(f"SELECT DISTINCT areaNombre FROM {Config.SQL_VIEW} WHERE Año=:a")
        engine = Config.get_engine_data()
        with engine.connect() as conn:
            res = conn.execute(query, {'a': anio}).fetchall()
            return [r[0] for r in res if r[0]]

# --------------------------------------------------------------------------
# 🚀 FUNCIÓN CORE: PROCESAR ÁREA (ENCUETA PERCEPCION)
# --------------------------------------------------------------------------
def ejecutar_flujo_area(area, anio, mes, perf, db_log, notifier, is_job=False):
    """Ciclo de vida: Acumulado (Raíz) -> Subcarpeta Anual -> Triple Link Notificación."""
    start_time = datetime.datetime.now()
    log_tail = f"[{start_time.strftime('%H:%M:%S')}] > INIT: Encueta Percepcion V82.9.27\n"
    log_tail += f"[{start_time.strftime('%H:%M:%S')}] > TARGET: {area} (Periodo {mes}/{anio})\n"
    
    sp_ok_status = False
    path_anual = "N/A"
    
    try:
        print(f"\n>>> 🔄 Procesando: {area} [{mes}/{anio}]")
        
        # 1. Extracción y Transformación
        raw_data = DataEngine.extraer_datos(anio, mes, area)
        if raw_data.empty:
            print(f"⚠️ Sin datos para {area}.")
            return

        df_proc = DataProcessor.procesar(raw_data) 
        log_tail += f"[{datetime.datetime.now().strftime('%H:%M:%S')}] > DATA: {len(raw_data)} registros SQL cargados.\n"
        
        # 2. Configuración de Identidad y Rutas locales
        nombre_limpio = area.replace("/", "_").replace(" ", "_")
        folder_path = Config.BASE_CARPETA_TEMP / nombre_limpio
        folder_path.mkdir(parents=True, exist_ok=True)
        
        path_anual = folder_path / f"Acumulado_{anio}_{nombre_limpio}.xlsx"
        path_mensual = folder_path / f"Reporte_{nombre_limpio}_{anio}_{mes}.xlsx"

        sp_manager = SharePointManager(Config, db_log.logger)

        # ------------------------------------------------------------------
        # 🥇 PRIORIDAD 1: ACUMULADO (RAÍZ)
        # ------------------------------------------------------------------
        print(f"📊 [1/2] Generando Acumulado...")
        ExcelAcumuladoReporter.procesar(df_proc, raw_data, area, anio, mes, path_anual)
        
        print(f"☁️ Subiendo Acumulado a Raíz SharePoint...")
        # FIX: Eliminamos 'anio' si la función subir_reporte_prioritario solo espera (path, area, es_acumulado)
        link_a_sp = sp_manager.subir_reporte_prioritario(str(path_anual), nombre_limpio, es_acumulado=True)
        
        # ------------------------------------------------------------------
        # 🥈 PRIORIDAD 2: MENSUAL (SUBCARPETA)
        # ------------------------------------------------------------------
        print(f"📊 [2/2] Generando Mensual...")
        ok_m, kpi_m = ExcelSovereignReporter.generar(df_proc, raw_data, area, anio, mes, path_mensual)
        
        print(f"📂 Organizando en carpeta anual...")
        # FIX: Alineación de argumentos igual que arriba
        link_m_sp = sp_manager.subir_reporte_prioritario(str(path_mensual), nombre_limpio, es_acumulado=False)

        # ------------------------------------------------------------------
        # 🔗 TRIPLE LINK HIJACKING
        # ------------------------------------------------------------------
        _, link_f_mensual = sp_manager.get_folder_info(f"{Config.SP_BASE_PATH}/{nombre_limpio}/{nombre_limpio}_{anio}")
        _, link_f_raiz = sp_manager.get_folder_info(f"{Config.SP_BASE_PATH}/{nombre_limpio}")
        
        log_tail += f"[{datetime.datetime.now().strftime('%H:%M:%S')}] > CLOUD: Sincronización OK.\n"
        log_tail += f"[{datetime.datetime.now().strftime('%H:%M:%S')}] > KPI: Desempeño {df_proc['Indicador_0_100'].mean():.1f}%\n"

        # 3. Notificación Ejecutiva
        data_package = {
            'area': area, 'mes': mes, 'anio': anio,
            'link_acumulado_sp': link_a_sp,
            'link_folder_mensual_sp': link_f_mensual,
            'link_folder_raiz_sp': link_f_raiz,
            'k_consecutivos': kpi_m.get('cons', 0) if kpi_m else 0,
            'duration': (datetime.datetime.now() - start_time).total_seconds(),
            'script_version': "V82.9.27 Titanium",
            'ejecucion_mode': "JOB" if is_job else "MANUAL",
            'log_tail': log_tail,
            'df_proc': df_proc
        }
        
        notifier.enviar_reporte_ejecutivo(Config.get_engine_log(), data_package)
        sp_ok_status = True if (link_a_sp and link_m_sp) else False

        db_log.insertar(anio, mes, area, len(raw_data), len(df_proc), 
                        "EXITOSO", "Flujo finalizado.", str(path_anual), sp_ok_status)

    except Exception as e:
        error_msg = f"Error Crítico: {str(e)}"
        print(f"❌ {error_msg}")
        trace = traceback.format_exc()
        notifier.enviar_alerta_tecnica(area, str(e), trace)
        db_log.insertar(anio, mes, area, 0, 0, "ERROR", error_msg, str(path_anual), False)
    finally:
        gc.collect()

# --------------------------------------------------------------------------
# 🛠️ ENTRY POINT
# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="ETL Encueta Percepcion V82.9.27")
    parser.add_argument('--anio', type=int)
    parser.add_argument('--mes', type=int)
    parser.add_argument('--area', type=str)
    args = parser.parse_args()

    today = datetime.datetime.now()
    anio_proc = args.anio or today.year
    mes_proc = args.mes or today.month

    logger, log_file = LogManager.configurar_log(Config.LOG_DIR, "EncuetaPercepcion_Master")
    perf = PerformanceMonitor(logger)
    
    db_log = DBLogger(Config.get_engine_log(), "V82.9.27 Titanium", logger)
    notifier = NotificationManager(Config, logger)

    logger.info(f"🏁 INICIO: Periodo {mes_proc}/{anio_proc}")

    areas = [args.area] if args.area else DataEngine.descubrir_areas(anio_proc)
    logger.info(f"📂 Áreas a procesar: {len(areas)}")

    with perf.measure("PROCESO_GLOBAL"):
        for area in areas:
            ejecutar_flujo_area(area, anio_proc, mes_proc, perf, db_log, notifier)

    logger.info(perf.get_summary())
    logger.info(f"✅ Proceso Finalizado. Log en: {log_file}")

if __name__ == "__main__":
    main()