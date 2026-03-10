# -*- coding: utf-8 -*-
"""
ARCHIVO DE CONFIGURACIÓN CENTRALIZADA E INSTITUCIONAL - DITIC
Archivo: configuracion_sistema_encuestas.py
Versión: V82.9.34 (Refactorización a Código Limpio en Español)
Responsable: Mg. Fredy Alejandro Sarmiento Torres
"""

# Importamos la librería para interactuar con el sistema operativo y variables de entorno
import os
# Importamos la librería para codificar los parámetros de conexión a la base de datos
import urllib.parse
# Importamos la librería para el manejo de motores y conexiones SQL
import sqlalchemy as sa
# Importamos Path para el manejo robusto de rutas de carpetas y archivos en Windows
from pathlib import Path
# Importamos la función para cargar las variables ocultas desde el archivo local .env
from dotenv import load_dotenv

# Cargamos las variables de seguridad del archivo .env local para inyectar credenciales reales sin exponerlas
load_dotenv()

class ConfiguracionSistema:
    """Clase maestra que centraliza todas las variables de entorno, rutas y credenciales del proyecto."""

    # ==========================================================================
    # 1. 🛡️ SEGURIDAD Y CREDENCIALES (Bases de Datos SQL Server)
    # ==========================================================================
    
    # --- CONEXIÓN DE ORIGEN DE DATOS (Servidor SQLAppsWeb) ---
    # Capturamos el servidor de origen o asignamos el valor por defecto de la Universidad
    SERVIDOR_BASE_DATOS_ORIGEN = os.getenv('DB_DATA_SERVER') or r"SRVSQLAPPSWEB\URSQL01,14331"
    # Capturamos el nombre de la base de datos de percepcion
    NOMBRE_BASE_DATOS_ORIGEN = os.getenv('DB_DATA_NAME') or "dbpercepcion"
    # Capturamos el usuario de lectura de la base de datos
    USUARIO_BASE_DATOS_ORIGEN = os.getenv('DB_DATA_USER') or "USR_dbpercepcion"
    # Capturamos la contraseña inyectada desde el archivo .env (Seguridad)
    CONTRASENA_BASE_DATOS_ORIGEN = os.getenv('DB_DATA_PASS') or "TU_PASSWORD_SQL_AQUI"

    # --- CONEXIÓN DE AUDITORÍA Y REGISTROS (Servidor CBQABI) ---
    # Capturamos el servidor de auditoría
    SERVIDOR_BASE_DATOS_AUDITORIA = os.getenv('DB_LOG_SERVER') or r"SRVCBQABI,1433"
    # Capturamos el nombre de la base de datos de modelos donde guardamos logs
    NOMBRE_BASE_DATOS_AUDITORIA = os.getenv('DB_LOG_NAME') or "BA_MODELS"
    # Capturamos el usuario administrador de inteligencia de negocios
    USUARIO_BASE_DATOS_AUDITORIA = os.getenv('DB_LOG_USER') or "admin.bisql02"
    # Capturamos la contraseña de auditoría desde el .env
    CONTRASENA_BASE_DATOS_AUDITORIA = os.getenv('DB_LOG_PASS') or "TU_PASSWORD_LOG_AQUI"
    # Definimos explícitamente el esquema de la tabla de logs
    ESQUEMA_BASE_DATOS_AUDITORIA = "chatbot"
    # Definimos explícitamente la tabla donde se insertará la trazabilidad
    TABLA_BASE_DATOS_AUDITORIA = "Log_ExportacionEncuestas"

    # ==========================================================================
    # 2. 📧 COMUNICACIONES (Servidor SMTP Institucional Office 365)
    # ==========================================================================
    
    # Definimos el servidor de correo corporativo de Microsoft
    SERVIDOR_CORREO_SMTP = "smtp.office365.com"
    # Definimos el puerto seguro para envío de correos
    PUERTO_CORREO_SMTP = 587
    # Capturamos el usuario remitente (Tu correo institucional)
    USUARIO_CORREO_SMTP = os.getenv('SMTP_USER') or "fredya.sarmiento@urosario.edu.co"
    # Capturamos la contraseña de correo desde el entorno seguro
    CONTRASENA_CORREO_SMTP = os.getenv('SMTP_PASS') or "TU_PASSWORD_SMTP_AQUI"
    # Definimos quién aparecerá como remitente de los correos automáticos
    REMITENTE_CORREO_SMTP = USUARIO_CORREO_SMTP
    # Definimos una lista de destinatarios de emergencia si falla la consulta en SQL
    DESTINATARIOS_CORREO_POR_DEFECTO = [USUARIO_CORREO_SMTP]

    # Diccionario maestro para la clasificación de sentimientos en comentarios abiertos
    PALABRAS_CLAVE_SENTIMIENTO = {
        'POSITIVO': ['EXCELENTE', 'MUY BUENO', 'MUY BIEN', 'OPTIMO', 'GRACIAS', 'RÁPIDO', 'AMABLE', 'SI'],
        'NEGATIVO': ['MALO', 'PÉSIMO', 'TARDADO', 'INSUFICIENTE', 'TERRIBLE', 'DEMORA', 'LENTO', 'NO'],
        'NEUTRAL': ['NINGUNA', 'N/A', 'REGULAR', 'ACEPTABLE', 'NORMAL']
    }

    # ==========================================================================
    # 3. 🎨 IDENTIDAD VISUAL Y METAS INSTITUCIONALES (KPIs)
    # ==========================================================================
    
    # Código hexadecimal para el Rojo institucional de la Universidad del Rosario
    COLOR_INSTITUCIONAL_ROJO = "AF2024"
    # Código hexadecimal para el Azul institucional de la Universidad del Rosario
    COLOR_INSTITUCIONAL_AZUL = "1F4E78"
    # Meta de excelencia esperada para las encuestas (80%)
    META_INDICADOR_DESEMPENO = 80.0
    # Umbral de criticidad para enviar alertas (70%)
    LIMITE_CRITICO_INDICADOR_DESEMPENO = 70.0
    # URL pública del logo para incrustar en los correos HTML
    URL_LOGOTIPO_INSTITUCIONAL = "https://www.urosario.edu.co/img/logo_ur.png"

    # ==========================================================================
    # 4. 📁 RUTAS ESTRUCTURALES LOCALES (Servidor Windows)
    # ==========================================================================
    
    # Ruta absoluta donde vive el orquestador y los archivos de Python
    DIRECTORIO_RAIZ_PROYECTO = Path(r"F:\ETL_DITIC\DWHencuestaPercepcion")
    # Ruta absoluta donde se generarán los Excel antes de subirlos a SharePoint
    DIRECTORIO_TEMPORAL_EXPORTACION = Path(r"F:\ETL_DITIC\temp_exportacion_multiarea")
    # Ruta relativa para almacenar los archivos de texto de log de eventos
    DIRECTORIO_REGISTROS_AUDITORIA = DIRECTORIO_RAIZ_PROYECTO / "Logs"
    # Ruta relativa del logo para pegarlo dentro de los reportes Excel
    RUTA_LOGOTIPO_LOCAL = DIRECTORIO_RAIZ_PROYECTO / "LogoUR.png"
    # Nombre completo de la vista SQL de donde se extraen los registros crudos
    VISTA_SQL_ORIGEN_DATOS = "dbo.View_respuestas_encuesta_percepcion_historica_optimizada"

    # ==========================================================================
    # 5. 🌐 INTEGRACIÓN EN LA NUBE (Microsoft Graph API - SharePoint)
    # ==========================================================================
    
    # Identificador de la aplicación en Azure AD para acceso desatendido
    IDENTIFICADOR_CLIENTE_SHAREPOINT = os.getenv('SP_CLIENT_ID') or "TU_CLIENT_ID_AQUI"
    # Secreto de la aplicación en Azure AD (Protegido en .env)
    SECRETO_CLIENTE_SHAREPOINT = os.getenv('SP_CLIENT_SECRET') or "TU_SECRET_AQUI"
    # Identificador del entorno institucional de la Universidad (Tenant)
    IDENTIFICADOR_INQUILINO_SHAREPOINT = os.getenv('SP_TENANT_ID') or "ae525757-89ba-4d30-a2f7-49796ef8c604"
    # Identificador único del sitio de Datamining en SharePoint
    IDENTIFICADOR_SITIO_SHAREPOINT = os.getenv('SP_SITE_ID') or "TU_SITE_ID_AQUI"
    # Ruta base dentro de la carpeta compartida donde se crearán las áreas
    RUTA_BASE_DOCUMENTOS_SHAREPOINT = os.getenv('SP_BASE_PATH') or "Documentos/Proyectos/EncuestasPercepcionPorAreas"
    # URL amigable del sitio principal para enlaces
    URL_SITIO_SHAREPOINT = "https://uredu.sharepoint.com/sites/DocumentosDatamining"

    # ==========================================================================
    # 6. 🔌 MÉTODOS DE CONECTIVIDAD (Generadores de Motores de Base de Datos)
    # ==========================================================================
    
    @classmethod
    def obtener_cadena_conexion_sql(cls, servidor, base_datos, usuario=None, contrasena=None, conexion_confiada=False):
        """Genera y codifica la cadena de conexión segura para el driver ODBC 17."""
        # Definimos el driver estándar recomendado por Microsoft para Python
        driver = "{ODBC Driver 17 for SQL Server}"
        
        # Validamos si se usa Autenticación Integrada de Windows
        if conexion_confiada:
            # Ensamblamos la cadena con Trusted_Connection=yes y la codificamos para SQLAlchemy
            parametros = urllib.parse.quote_plus(f"DRIVER={driver};SERVER={servidor};DATABASE={base_datos};Trusted_Connection=yes;")
        else:
            # Ensamblamos la cadena usando Usuario y Contraseña explícitos
            parametros = urllib.parse.quote_plus(f"DRIVER={driver};SERVER={servidor};DATABASE={base_datos};UID={usuario};PWD={contrasena};")
        
        # Retornamos el formato exacto requerido por SQLAlchemy (mssql+pyodbc)
        return f"mssql+pyodbc:///?odbc_connect={parametros}"

    @classmethod
    def obtener_motor_base_datos_origen(cls):
        """Construye el motor SQLAlchemy para leer la vista de respuestas."""
        # Retornamos el motor creado con las credenciales explícitas del servidor de origen
        return sa.create_engine(
            cls.obtener_cadena_conexion_sql(
                cls.SERVIDOR_BASE_DATOS_ORIGEN, 
                cls.NOMBRE_BASE_DATOS_ORIGEN, 
                cls.USUARIO_BASE_DATOS_ORIGEN, 
                cls.CONTRASENA_BASE_DATOS_ORIGEN
            ),
            # Activamos fast_executemany para optimizar lecturas/escrituras masivas
            fast_executemany=True
        )

    @classmethod
    def obtener_motor_base_datos_auditoria(cls):
        """Construye el motor SQLAlchemy para escribir en la tabla de logs."""
        try:
            # Intento 1: Conexión usando la identidad de Windows (admin.bisql02 / Windows Authentication)
            cadena_conexion = cls.obtener_cadena_conexion_sql(
                cls.SERVIDOR_BASE_DATOS_AUDITORIA, 
                cls.NOMBRE_BASE_DATOS_AUDITORIA, 
                conexion_confiada=True
            )
            # Creamos el motor temporal
            motor = sa.create_engine(cadena_conexion, fast_executemany=True)
            # Hacemos una prueba de conexión rápida (ping)
            with motor.connect() as conexion: pass 
            # Si pasa la prueba, retornamos este motor
            return motor
        except Exception:
            # Intento 2 (Fallback): Si falla la autenticación de Windows, intentamos con Usuario/Contraseña explícitos
            cadena_conexion = cls.obtener_cadena_conexion_sql(
                cls.SERVIDOR_BASE_DATOS_AUDITORIA, 
                cls.NOMBRE_BASE_DATOS_AUDITORIA, 
                cls.USUARIO_BASE_DATOS_AUDITORIA, 
                cls.CONTRASENA_BASE_DATOS_AUDITORIA
            )
            # Retornamos el motor con autenticación estándar
            return sa.create_engine(cadena_conexion, fast_executemany=True)

    @classmethod
    def inicializar_entorno_directorios(cls):
        """Asegura la existencia de las carpetas requeridas en el disco del servidor antes de iniciar."""
        # Iteramos sobre la lista de carpetas necesarias
        for ruta in [cls.DIRECTORIO_REGISTROS_AUDITORIA, cls.DIRECTORIO_TEMPORAL_EXPORTACION]: 
            # Creamos la carpeta y todos sus padres (parents=True) sin generar error si ya existe (exist_ok=True)
            ruta.mkdir(parents=True, exist_ok=True)

# Al leer este archivo, Python ejecutará automáticamente la creación de las carpetas locales.
ConfiguracionSistema.inicializar_entorno_directorios()