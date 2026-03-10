# -*- coding: utf-8 -*-
"""
ORQUESTADOR CENTRAL DE ENCUESTAS DE PERCEPCIÓN - DITIC
Archivo: orquestador_principal_flujo_encuestas.py
Versión: V82.9.34 (Refactorización a Código Limpio Institucional)
Autor: Mg. Fredy Alejandro Sarmiento Torres
Lógica: Cerebro maestro que enlaza bases de datos, procesamiento de Excel, SharePoint y envío de correos.
"""

# Importamos librerías del sistema y línea de comandos
import argparse
import sys
import datetime
import traceback
import gc
import time

# Importamos librerías de datos
import pandas as pd
from sqlalchemy import text
from pathlib import Path

# ==========================================================================
# 🧩 IMPORTACIÓN DE LOS NUEVOS MÓDULOS REFACTORIZADOS (Arquitectura Limpia)
# ==========================================================================
from configuracion_sistema_encuestas import ConfiguracionSistema
from gestion_trazabilidad_auditoria import GestorRegistroEventos, MonitorRendimientoSistema, AuditorBaseDatos
from reporte_excel_ejecutivo_mensual import GeneradorReporteEjecutivoMensual, TransformadorDatos
from reporte_excel_historico_acumulado import GeneradorReporteAcumulado
from comunicacion_notificaciones_html_smtp import GestorComunicacionNotificaciones
from integracion_nube_microsoft_graph_api import AdministradorSharePointGraphAPI 

# --------------------------------------------------------------------------
# 🔌 LÓGICA DE CONEXIÓN A ORIGEN (DATA ENGINE)
# --------------------------------------------------------------------------
class MotorExtraccionDatosSQL:
    """Clase encargada de leer directamente la vista de encuestas en SQL Server."""
    
    @staticmethod
    def extraer_datos_crudos_sql(anio_filtro, mes_filtro, nombre_area_filtro):
        """Extrae la información cruda y exacta desde la base de datos de producción (SQLAppsWeb)."""
        # Preparamos la consulta SQL segura con parámetros
        consulta_sql = text(f"SELECT * FROM {ConfiguracionSistema.VISTA_SQL_ORIGEN_DATOS} WHERE Año=:a AND Mes=:m AND areaNombre=:ar")
        # Obtenemos el motor de conexión
        motor_sql = ConfiguracionSistema.obtener_motor_base_datos_origen()
        # Ejecutamos la consulta y la convertimos en un DataFrame de pandas
        with motor_sql.connect() as conexion:
            return pd.read_sql(consulta_sql, conexion, params={'a': anio_filtro, 'm': mes_filtro, 'ar': nombre_area_filtro})

    @staticmethod
    def descubrir_areas_con_encuestas(anio_filtro, mes_filtro):
        """Identifica dinámicamente qué áreas de la Universidad realmente tuvieron encuestas este mes."""
        # Consultamos solo los nombres de áreas distintos (DISTINCT) que tengan datos en la fecha dada
        consulta_sql = text(f"SELECT DISTINCT areaNombre FROM {ConfiguracionSistema.VISTA_SQL_ORIGEN_DATOS} WHERE Año=:a AND Mes=:m")
        motor_sql = ConfiguracionSistema.obtener_motor_base_datos_origen()
        with motor_sql.connect() as conexion:
            resultados = conexion.execute(consulta_sql, {'a': anio_filtro, 'm': mes_filtro}).fetchall()
            # Retornamos la lista limpia de nombres de áreas
            return [fila[0] for fila in resultados if fila[0]]

# --------------------------------------------------------------------------
# 🚀 FUNCIÓN CENTRAL: PROCESAR UN ÁREA ESPECÍFICA
# --------------------------------------------------------------------------
def ejecutar_flujo_completo_por_area(nombre_area, anio, mes, monitor_rendimiento, auditor_bd, gestor_notificaciones, es_tarea_programada=False):
    """
    Controla el ciclo de vida de un área: 
    1. Descarga datos -> 2. Limpia -> 3. Crea Excels -> 4. Sube a Nube -> 5. Notifica.
    """
    # Iniciamos el cronómetro y la consola de auditoría técnica que irá en el correo
    tiempo_inicio = datetime.datetime.now()
    registro_consola = f"[{tiempo_inicio.strftime('%H:%M:%S')}] > INIT: Motor Encuestas DITIC V82.9.34\n"
    registro_consola += f"[{tiempo_inicio.strftime('%H:%M:%S')}] > TARGET: {nombre_area} (Periodo {mes}/{anio})\n"
    
    # Banderas por defecto en caso de error temprano
    estado_subida_nube_exitoso = False
    ruta_archivo_acumulado = "N/A"
    
    try:
        print(f"\n{'='*50}")
        print(f">>> 🔄 Procesando: {nombre_area} [{mes}/{anio}]")
        print(f"{'='*50}")
        
        # 1. Extracción y Transformación Numérica
        dataframe_crudo = MotorExtraccionDatosSQL.extraer_datos_crudos_sql(anio, mes, nombre_area)
        
        # Si la consulta devuelve 0 filas, saltamos esta área y ahorramos recursos
        if dataframe_crudo.empty:
            print(f"⚠️ Sin datos detectados para {nombre_area} en el periodo {mes}/{anio}. Saltando al siguiente...")
            return

        # Transformamos las columnas anchas en filas e interpretamos sentimientos
        dataframe_procesado = TransformadorDatos.procesar_datos_crudos(dataframe_crudo) 
        registro_consola += f"[{datetime.datetime.now().strftime('%H:%M:%S')}] > DATA: {len(dataframe_crudo)} registros extraídos de SQL Server.\n"
        
        # 2. Configuración de Nombres Físicos y Rutas Temporales
        # Reemplazamos caracteres que Windows o SharePoint no soportan bien
        nombre_area_limpio_nube = nombre_area.replace("/", "_").replace(" ", "_")
        ruta_carpeta_temporal = ConfiguracionSistema.DIRECTORIO_TEMPORAL_EXPORTACION / nombre_area_limpio_nube
        ruta_carpeta_temporal.mkdir(parents=True, exist_ok=True)
        
        # Definimos cómo se llamarán los archivos de Excel
        ruta_archivo_acumulado = ruta_carpeta_temporal / f"Acumulado_{anio}_{nombre_area_limpio_nube}.xlsx"
        ruta_archivo_mensual = ruta_carpeta_temporal / f"Reporte_{nombre_area_limpio_nube}_{anio}_{mes}.xlsx"

        # Instanciamos el conector de Microsoft Graph
        gestor_sharepoint = AdministradorSharePointGraphAPI(ConfiguracionSistema, auditor_bd.registrador_eventos)

        # ------------------------------------------------------------------
        # 🥇 PRIORIDAD 1: PROCESAMIENTO DEL ARCHIVO ACUMULADO (RAÍZ)
        # ------------------------------------------------------------------
        print(f"📊 [1/2] Generando Archivo Acumulado Histórico...")
        GeneradorReporteAcumulado.procesar_historico_mensual(dataframe_procesado, dataframe_crudo, nombre_area, anio, mes, ruta_archivo_acumulado)
        
        print(f"☁️ Subiendo Acumulado a la Raíz del Área en SharePoint...")
        enlace_acumulado_sharepoint = gestor_sharepoint.subir_archivo_excel_segun_prioridad(str(ruta_archivo_acumulado), nombre_area_limpio_nube, es_archivo_acumulado=True)
        
        # ------------------------------------------------------------------
        # 🥈 PRIORIDAD 2: PROCESAMIENTO DEL ARCHIVO MENSUAL (SUBCARPETA)
        # ------------------------------------------------------------------
        print(f"📊 [2/2] Generando Archivo Ejecutivo Mensual...")
        exito_mensual, kpis_mensuales = GeneradorReporteEjecutivoMensual.construir_libro_excel(dataframe_procesado, dataframe_crudo, nombre_area, anio, mes, ruta_archivo_mensual)
        
        print(f"📂 Organizando Mensual en Subcarpeta Anual en la Nube...")
        enlace_mensual_sharepoint = gestor_sharepoint.subir_archivo_excel_segun_prioridad(str(ruta_archivo_mensual), nombre_area_limpio_nube, es_archivo_acumulado=False)

        # ------------------------------------------------------------------
        # 🔗 RECUPERACIÓN DE ENLACES WEB PARA EL CORREO
        # ------------------------------------------------------------------
        # Obtenemos los links directos a las carpetas para crear los botones HTML
        _, enlace_directorio_mensual = gestor_sharepoint.obtener_informacion_enlace_carpeta(f"{ConfiguracionSistema.RUTA_BASE_DOCUMENTOS_SHAREPOINT}/{nombre_area_limpio_nube}/{nombre_area_limpio_nube}_{anio}")
        _, enlace_directorio_raiz = gestor_sharepoint.obtener_informacion_enlace_carpeta(f"{ConfiguracionSistema.RUTA_BASE_DOCUMENTOS_SHAREPOINT}/{nombre_area_limpio_nube}")
        
        # Actualizamos la consola
        registro_consola += f"[{datetime.datetime.now().strftime('%H:%M:%S')}] > CLOUD: Sincronización OneDrive/SharePoint finalizada.\n"
        registro_consola += f"[{datetime.datetime.now().strftime('%H:%M:%S')}] > KPI: Promedio Desempeño General {dataframe_procesado['Indicador_0_100'].mean():.1f}%\n"

        # 3. Empaquetado y Notificación Ejecutiva
        paquete_datos_correo = {
            'area': nombre_area, 'mes': mes, 'anio': anio,
            'link_acumulado_sp': enlace_acumulado_sharepoint,
            'link_folder_mensual_sp': enlace_directorio_mensual,
            'link_folder_raiz_sp': enlace_directorio_raiz,
            'k_consecutivos': kpis_mensuales.get('cons', 0) if kpis_mensuales else 0,
            'duration': (datetime.datetime.now() - tiempo_inicio).total_seconds(),
            'script_version': "V82.9.34 (Clean Code)",
            'ejecucion_mode': "AUTOMÁTICA (SERVER)" if es_tarea_programada else "MANUAL (CONSOLA)",
            'log_tail': registro_consola,
            'df_proc': dataframe_procesado
        }
        
        # Enviamos el correo con las gráficas HTML
        gestor_notificaciones.enviar_reporte_ejecutivo_mensual(ConfiguracionSistema.obtener_motor_base_datos_auditoria(), paquete_datos_correo)
        
        # Si ambos archivos tienen link, asumimos que SharePoint fue un éxito total
        estado_subida_nube_exitoso = True if (enlace_acumulado_sharepoint and enlace_mensual_sharepoint) else False

        # 4. Auditoría en Base de Datos (Insertamos fila en la tabla de logs)
        auditor_bd.insertar_registro_auditoria(
            anio, mes, nombre_area, len(dataframe_crudo), len(dataframe_procesado), 
            "EXITOSO", "Flujo orquestado exitosamente.", str(ruta_archivo_acumulado), estado_subida_nube_exitoso
        )

    except Exception as excepcion_capturada:
        # En caso de fallo en CUALQUIER punto del proceso del área
        mensaje_error_tecnico = f"Error Crítico en el Orquestador: {str(excepcion_capturada)}"
        print(f"❌ {mensaje_error_tecnico}")
        traza_completa = traceback.format_exc()
        
        # Enviamos alerta de emergencia en rojo
        gestor_notificaciones.enviar_alerta_fallo_tecnico(nombre_area, str(excepcion_capturada), traza_completa)
        
        # Registramos el fracaso en la tabla de logs SQL
        auditor_bd.insertar_registro_auditoria(anio, mes, nombre_area, 0, 0, "ERROR", mensaje_error_tecnico, str(ruta_archivo_acumulado), False)
    finally:
        # Forzamos la liberación de memoria sin importar si fue éxito o fracaso
        gc.collect()

# --------------------------------------------------------------------------
# 🛠️ ENTRY POINT: ARRANQUE DESDE LA LÍNEA DE COMANDOS
# --------------------------------------------------------------------------
def orquestador_principal():
    """Función de entrada del programa. Lee parámetros y distribuye el trabajo."""
    analizador_argumentos = argparse.ArgumentParser(description="ETL Encuestas Percepción DITIC V82.9.34")
    analizador_argumentos.add_argument('--anio', type=int, help="Año exacto a procesar (ej. 2026)")
    analizador_argumentos.add_argument('--mes', type=int, help="Mes exacto a procesar (ej. 2 para Febrero)")
    analizador_argumentos.add_argument('--area', type=str, default="TODAS", help="Nombre del área (ej. 'CRAI') o 'TODAS' por defecto")
    analizador_argumentos.add_argument('--auto_date', action='store_true', help="Ignora año/mes y autocalcula el mes inmediatamente anterior")
    argumentos_recibidos = analizador_argumentos.parse_args()

    # --- PLAN DE INTELIGENCIA DE FECHA ---
    if argumentos_recibidos.auto_date:
        fecha_hoy = datetime.date.today()
        # Restar un día al primer día de este mes nos garantiza caer en el último día del mes anterior
        primer_dia_mes_actual = fecha_hoy.replace(day=1)
        fecha_mes_pasado = primer_dia_mes_actual - datetime.timedelta(days=1)
        anio_proceso = fecha_mes_pasado.year
        mes_proceso = fecha_mes_pasado.month
        print(f"🕒 Modo Automatizado: El sistema autodetectó el periodo anterior -> {mes_proceso}/{anio_proceso}")
    else:
        # Si no es automático, usa lo que pasaron por consola. Si no pasaron nada, usa el mes actual
        fecha_actual = datetime.datetime.now()
        anio_proceso = argumentos_recibidos.anio or fecha_actual.year
        mes_proceso = argumentos_recibidos.mes or fecha_actual.month

    # Inicializamos todos los gestores de infraestructura con los nuevos nombres de clase
    registrador_eventos, ruta_archivo_log = GestorRegistroEventos.configurar_registro_eventos(ConfiguracionSistema.DIRECTORIO_REGISTROS_AUDITORIA, "Orquestador_Encuestas")
    monitor_rendimiento = MonitorRendimientoSistema(registrador_eventos)
    auditor_bd = AuditorBaseDatos(ConfiguracionSistema.obtener_motor_base_datos_auditoria(), "V82.9.34", registrador_eventos)
    gestor_notificaciones = GestorComunicacionNotificaciones(ConfiguracionSistema, registrador_eventos)

    registrador_eventos.info(f"🏁 INICIO CENTRAL DE OPERACIONES: Periodo {mes_proceso}/{anio_proceso}")

    # --- PLAN DE DESCUBRIMIENTO DE ÁREAS ---
    if argumentos_recibidos.area.upper() == "TODAS":
        # Leemos SQL para saber cuáles de las 38 áreas posibles tuvieron actividad este mes
        lista_areas_a_procesar = MotorExtraccionDatosSQL.descubrir_areas_con_encuestas(anio_proceso, mes_proceso)
        print(f"📦 MODO MASIVO: El sistema ha detectado {len(lista_areas_a_procesar)} áreas con datos reales para este periodo.")
    else:
        # Modo quirúrgico: Solo reprocesar un área
        lista_areas_a_procesar = [argumentos_recibidos.area]

    registrador_eventos.info(f"📂 Total de áreas en la cola de procesamiento: {len(lista_areas_a_procesar)}")

    # 🚀 ALERTA DE INICIO MASIVA: Enviamos correo al administrador informando que el servidor despertó
    es_modo_masivo = argumentos_recibidos.auto_date or argumentos_recibidos.area.upper() == "TODAS"
    if es_modo_masivo:
        gestor_notificaciones.enviar_alerta_global_orquestador('INICIO', lista_areas_a_procesar, mes_proceso, anio_proceso)

    tiempo_global_inicio = datetime.datetime.now()

    # Bloque de medición de rendimiento de la Orquestación
    with monitor_rendimiento.medir_rendimiento_bloque("PROCESO_GLOBAL_ORQUESTADOR"):
        # Iteramos área por área
        for indice, nombre_area_actual in enumerate(lista_areas_a_procesar, 1):
            print(f"\n[{indice}/{len(lista_areas_a_procesar)}] Preparando motores para el área...")
            ejecutar_flujo_completo_por_area(
                nombre_area_actual, anio_proceso, mes_proceso, 
                monitor_rendimiento, auditor_bd, gestor_notificaciones, 
                es_tarea_programada=argumentos_recibidos.auto_date
            )

    # ✅ ALERTA DE FIN MASIVA: Informamos al administrador que el bucle completo terminó
    if es_modo_masivo:
        # Formateamos el tiempo total (ej. 00:05:43)
        cadena_duracion_total = str(datetime.datetime.now() - tiempo_global_inicio).split('.')[0] 
        gestor_notificaciones.enviar_alerta_global_orquestador('FIN', lista_areas_a_procesar, mes_proceso, anio_proceso, informacion_adicional=cadena_duracion_total)

    # Imprimimos y guardamos el reporte final de consumo de memoria RAM y Tiempos
    registrador_eventos.info(monitor_rendimiento.generar_resumen_rendimiento())
    registrador_eventos.info(f"✅ Orquestación Finalizada con Éxito. El log técnico se ha guardado en: {ruta_archivo_log}")

if __name__ == "__main__":
    orquestador_principal()