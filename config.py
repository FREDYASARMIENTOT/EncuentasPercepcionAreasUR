# -*- coding: utf-8 -*-
"""
ARCHIVO DE CONFIGURACIÓN CENTRALIZADA - DITIC
Versión: V82.9.30 Titanium Sovereign (SMTP Auth & Env Injection)
Responsable: Mg. Fredy Alejandro Sarmiento Torres
"""

import os
import urllib.parse
import datetime
import sqlalchemy as sa
from pathlib import Path
from dotenv import load_dotenv

# Cargamos las variables del archivo .env local para inyectar credenciales reales
load_dotenv()

class Config:
    # ==========================================================================
    # 1. 🛡️ SEGURIDAD Y CREDENCIALES (Variables de Entorno)
    # ==========================================================================
    # ORIGEN: SQL Server Authentication (SQLAppsWeb)
    DB_DATA_SERVER = os.getenv('DB_DATA_SERVER') or r"SRVSQLAPPSWEB\URSQL01,14331"
    DB_DATA_NAME = os.getenv('DB_DATA_NAME') or "dbpercepcion"
    DB_DATA_USER = os.getenv('DB_DATA_USER') or "USR_dbpercepcion"
    DB_DATA_PASS = os.getenv('DB_DATA_PASS') or "TU_PASSWORD_SQL_AQUI"

    # AUDITORÍA: Hybrid Authentication (CBQABI)
    DB_LOG_SERVER = os.getenv('DB_LOG_SERVER') or r"SRVCBQABI,1433"
    DB_LOG_NAME = os.getenv('DB_LOG_NAME') or "BA_MODELS"
    DB_LOG_USER = os.getenv('DB_LOG_USER') or "admin.bisql02"
    DB_LOG_PASS = os.getenv('DB_LOG_PASS') or "TU_PASSWORD_LOG_AQUI"
    DB_LOG_SCHEMA = "chatbot"
    DB_LOG_TABLE = "Log_ExportacionEncuestas"

    # ==========================================================================
    # 2. 📧 COMUNICACIONES (SMTP Office 365 con Autenticación)
    # ==========================================================================
    SMTP_SERVER = "smtp.office365.com"
    SMTP_PORT = 587
    SMTP_USER = os.getenv('SMTP_USER') or "fredya.sarmiento@urosario.edu.co"
    SMTP_PASS = os.getenv('SMTP_PASS') or "TU_PASSWORD_SMTP_AQUI"
    SMTP_FROM = SMTP_USER
    FALLBACK_RECIPIENTS = [SMTP_USER]

    SENTIMENT_KEYWORDS = {
        'POSITIVE': ['EXCELENTE', 'MUY BUENO', 'MUY BIEN', 'OPTIMO', 'GRACIAS', 'RÁPIDO', 'AMABLE', 'SI'],
        'NEGATIVE': ['MALO', 'PÉSIMO', 'TARDADO', 'INSUFICIENTE', 'TERRIBLE', 'DEMORA', 'LENTO', 'NO'],
        'NEUTRAL': ['NINGUNA', 'N/A', 'REGULAR', 'ACEPTABLE', 'NORMAL']
    }

    # ==========================================================================
    # 3. 🎨 IDENTIDAD VISUAL Y ESTILOS
    # ==========================================================================
    COLOR_UR_RED = "AF2024"
    COLOR_UR_BLUE = "1F4E78"
    KPI_GOAL = 80.0
    KPI_CRITICAL = 70.0
    LOGO_UR_URL = "https://www.urosario.edu.co/img/logo_ur.png"

    # ==========================================================================
    # 4. 📁 RUTAS ESTRUCTURALES
    # ==========================================================================
    ROOT_DIR = Path(r"F:\ETL_DITIC\DWHencuestaPercepcion")
    BASE_CARPETA_TEMP = Path(r"F:\ETL_DITIC\temp_exportacion_multiarea")
    LOG_DIR = ROOT_DIR / "Logs"
    LOGO_LOCAL = ROOT_DIR / "LogoUR.png"
    SQL_VIEW = "dbo.View_respuestas_encuesta_percepcion_historica_optimizada"

    # ==========================================================================
    # 5. 🌐 CONFIGURACIÓN SHAREPOINT (Graph API)
    # ==========================================================================
    SP_CLIENT_ID = os.getenv('SP_CLIENT_ID') or "TU_CLIENT_ID_AQUI"
    SP_CLIENT_SECRET = os.getenv('SP_CLIENT_SECRET') or "TU_SECRET_AQUI"
    SP_TENANT_ID = os.getenv('SP_TENANT_ID') or "ae525757-89ba-4d30-a2f7-49796ef8c604"
    SP_SITE_ID = os.getenv('SP_SITE_ID') or "TU_SITE_ID_AQUI"
    SP_BASE_PATH = os.getenv('SP_BASE_PATH') or "Documentos/Proyectos/EncuestasPercepcionPorAreas"
    SP_SITE_URL = "https://uredu.sharepoint.com/sites/DocumentosDatamining"

    # ==========================================================================
    # 6. 🔌 MÉTODOS DE CONECTIVIDAD (Engine Factory)
    # ==========================================================================
    @classmethod
    def get_conn_str(cls, server, db, user=None, pwd=None, trusted=False):
        driver = "{ODBC Driver 17 for SQL Server}"
        if trusted:
            params = urllib.parse.quote_plus(f"DRIVER={driver};SERVER={server};DATABASE={db};Trusted_Connection=yes;")
        else:
            params = urllib.parse.quote_plus(f"DRIVER={driver};SERVER={server};DATABASE={db};UID={user};PWD={pwd};")
        return f"mssql+pyodbc:///?odbc_connect={params}"

    @classmethod
    def get_engine_data(cls):
        return sa.create_engine(
            cls.get_conn_str(cls.DB_DATA_SERVER, cls.DB_DATA_NAME, cls.DB_DATA_USER, cls.DB_DATA_PASS),
            fast_executemany=True
        )

    @classmethod
    def get_engine_log(cls):
        try:
            conn_str = cls.get_conn_str(cls.DB_LOG_SERVER, cls.DB_LOG_NAME, trusted=True)
            engine = sa.create_engine(conn_str, fast_executemany=True)
            with engine.connect() as conn: pass 
            return engine
        except Exception:
            conn_str = cls.get_conn_str(cls.DB_LOG_SERVER, cls.DB_LOG_NAME, cls.DB_LOG_USER, cls.DB_LOG_PASS)
            return sa.create_engine(conn_str, fast_executemany=True)

    @classmethod
    def init_environment(cls):
        for p in [cls.LOG_DIR, cls.BASE_CARPETA_TEMP]: 
            p.mkdir(parents=True, exist_ok=True)

# Inicialización automática
Config.init_environment()