# -*- coding: utf-8 -*-
"""
GESTOR DE PERSISTENCIA EN LA NUBE (MICROSOFT GRAPH API) - DITIC
Archivo: integracion_nube_microsoft_graph_api.py
Versión: V82.9.34 (Refactorización a Código Limpio en Español)
Autor: Mg. Fredy Alejandro Sarmiento Torres
Lógica: Administra la subida de archivos Excel a SharePoint con resiliencia y creación de carpetas anidadas.
"""

# Importamos os para extraer nombres de archivos de las rutas locales
import os
# Importamos time para manejar pausas (sleep) durante los reintentos de conexión
import time
# Importamos requests para realizar llamadas HTTP/REST a la API de Microsoft Graph
import requests
# Importamos Path para manejo seguro de rutas (aunque aquí se usa de apoyo)
from pathlib import Path

class AdministradorSharePointGraphAPI:
    """
    Clase maestra que centraliza la comunicación con Microsoft 365.
    Estrategia de almacenamiento institucional:
    - Archivo Acumulado Histórico -> Se guarda directamente en la carpeta raíz del área.
    - Archivo Reporte Mensual -> Se guarda dentro de una subcarpeta anual (ej. Área_2026).
    """
    
    def __init__(self, configuracion_sistema, registrador_eventos):
        """Inicializa la sesión HTTP y solicita inmediatamente el token de seguridad a Azure AD."""
        self.configuracion = configuracion_sistema
        self.registrador_eventos = registrador_eventos
        # Creamos una sesión persistente para reutilizar conexiones TCP y acelerar múltiples peticiones
        self.sesion_http = requests.Session()
        
        # Obtenemos el token OAuth 2.0 apenas instanciamos la clase
        self.token_acceso = self._obtener_token_seguridad_oauth2()
        
        # Si Microsoft autoriza el acceso, configuramos la sesión para usar este token en adelante
        if self.token_acceso:
            self.sesion_http.headers.update({'Authorization': f'Bearer {self.token_acceso}'})
            self.registrador_eventos.info("🔗 Conexión estable establecida vía Microsoft Graph API.")
        else:
            self.registrador_eventos.error("❌ Fallo crítico de autenticación. Revisar las credenciales (SP_TENANT_ID) en el archivo .env.")

    def _obtener_token_seguridad_oauth2(self):
        """Solicita a Microsoft Azure un token de acceso 'Client Credentials' usando los secretos del .env."""
        try:
            # Extraemos las credenciales usando getattr por seguridad (evita que el código colapse si la variable no existe)
            identificador_inquilino = getattr(self.configuracion, 'IDENTIFICADOR_INQUILINO_SHAREPOINT', None)
            identificador_cliente = getattr(self.configuracion, 'IDENTIFICADOR_CLIENTE_SHAREPOINT', None)
            secreto_cliente = getattr(self.configuracion, 'SECRETO_CLIENTE_SHAREPOINT', None)

            # Validamos que los 3 datos críticos existan, de lo contrario detenemos el proceso tempranamente
            if not all([identificador_inquilino, identificador_cliente, secreto_cliente]):
                raise ValueError("Faltan las llaves de seguridad de SharePoint en el archivo de configuración.")

            # URL oficial de Microsoft para obtener tokens de aplicaciones desatendidas (Daemon apps)
            url_autenticacion = f"https://login.microsoftonline.com/{identificador_inquilino}/oauth2/v2.0/token"
            
            # Formato estándar requerido por OAuth 2.0
            cuerpo_peticion = {
                'grant_type': 'client_credentials',
                'client_id': identificador_cliente,
                'client_secret': secreto_cliente,
                'scope': 'https://graph.microsoft.com/.default' # Solicitamos acceso a todos los permisos otorgados en Azure
            }
            
            # Ejecutamos la petición POST con un tiempo máximo de espera de 15 segundos
            respuesta_servidor = self.sesion_http.post(url_autenticacion, data=cuerpo_peticion, timeout=15)
            # Si el servidor responde con error (ej. 401 Unauthorized), esto lanzará una excepción
            respuesta_servidor.raise_for_status()
            
            # Si fue exitoso, extraemos y retornamos el string del token
            return respuesta_servidor.json().get('access_token')
            
        except Exception as excepcion_capturada:
            # Capturamos cualquier error (red, credenciales inválidas, caída de Microsoft)
            self.registrador_eventos.error(f"💥 Error crítico obteniendo Token de Azure Graph: {excepcion_capturada}")
            return None

    def _obtener_o_crear_subcarpeta_anual(self, ruta_carpeta_padre, nombre_nueva_carpeta):
        """
        Busca si la subcarpeta del año (ej. CRAI_2026) ya existe en SharePoint. 
        Si no existe, la crea automáticamente.
        """
        
        
        # 1. PASO DE VERIFICACIÓN (Consulta GET)
        url_verificacion_existencia = f"https://graph.microsoft.com/v1.0/sites/{self.configuracion.IDENTIFICADOR_SITIO_SHAREPOINT}/drive/root:/{ruta_carpeta_padre}/{nombre_nueva_carpeta}"
        
        try:
            respuesta_verificacion = self.sesion_http.get(url_verificacion_existencia)
            # Si responde 200 (OK), la carpeta ya existe. Retornamos su ID único.
            if respuesta_verificacion.status_code == 200:
                return respuesta_verificacion.json().get('id')
            
            # 2. PASO DE CREACIÓN (Si la verificación falló, asumimos que no existe - 404)
            self.registrador_eventos.info(f"📂 Creando nueva subcarpeta anual en SharePoint: {nombre_nueva_carpeta}")
            
            # URL para inyectar "hijos" (children) dentro de la carpeta padre
            url_creacion_carpeta = f"https://graph.microsoft.com/v1.0/sites/{self.configuracion.IDENTIFICADOR_SITIO_SHAREPOINT}/drive/root:/{ruta_carpeta_padre}:/children"
            
            # Estructura JSON que Microsoft Graph exige para crear un directorio
            cuerpo_creacion = {
                "name": nombre_nueva_carpeta,
                "folder": {}, # Un objeto vacío indica que queremos crear una carpeta (no un archivo)
                "@microsoft.graph.conflictBehavior": "fail" # Si justo alguien la crea al mismo tiempo, fallamos para no duplicar
            }
            
            # Ejecutamos la petición POST para crearla
            respuesta_creacion = self.sesion_http.post(url_creacion_carpeta, json=cuerpo_creacion)
            respuesta_creacion.raise_for_status()
            
            # Retornamos el ID de la carpeta recién nacida
            return respuesta_creacion.json().get('id')
            
        except Exception as excepcion_capturada:
            # Si falla la creación (ej. falta de permisos en SharePoint), reportamos el error
            self.registrador_eventos.error(f"❌ Error gestionando la creación de la carpeta {nombre_nueva_carpeta}: {excepcion_capturada}")
            return None

    def subir_archivo_excel_segun_prioridad(self, ruta_archivo_local, nombre_area_institucional, es_archivo_acumulado=True):
        """
        Orquesta la subida del Excel hacia la ruta correcta en la nube.
        Acumulados van a la carpeta principal del área. Mensuales van a la subcarpeta del año.
        """
        # Si no tenemos token de seguridad, no podemos hacer nada
        if not self.token_acceso: 
            return None
        
        # Extraemos solo el nombre del Excel (ej. 'Acumulado_2026_CRAI.xlsx') ignorando la ruta del disco local F:\
        nombre_archivo_excel = os.path.basename(ruta_archivo_local)
        # Obtenemos el año actual del sistema para saber en qué subcarpeta debe ir el archivo mensual
        anio_actual_sistema = time.strftime("%Y") 
        
        if es_archivo_acumulado:
            # LÓGICA 1: Archivo Acumulado (Va directo a Documentos/Proyectos/EncuestasPercepcionPorAreas/Area)
            ruta_destino_nube = f"{self.configuracion.RUTA_BASE_DOCUMENTOS_SHAREPOINT}/{nombre_area_institucional}"
            self.registrador_eventos.info(f"🥇 Preparando subida de archivo Acumulado a la RAÍZ del área: {nombre_area_institucional}")
        else:
            # LÓGICA 2: Archivo Mensual (Requiere crear subcarpeta)
            nombre_subcarpeta_anual = f"{nombre_area_institucional}_{anio_actual_sistema}"
            ruta_padre_area = f"{self.configuracion.RUTA_BASE_DOCUMENTOS_SHAREPOINT}/{nombre_area_institucional}"
            
            # Intentamos obtener o crear la subcarpeta
            id_carpeta_creada = self._obtener_o_crear_subcarpeta_anual(ruta_padre_area, nombre_subcarpeta_anual)
            
            if not id_carpeta_creada:
                # PLAN DE CONTINGENCIA: Si no se pudo crear la subcarpeta, guardamos el archivo en la raíz para no perderlo
                self.registrador_eventos.warning("⚠️ Falló la creación de la subcarpeta anual. Aplicando Plan B: Subida directa a la carpeta raíz.")
                ruta_destino_nube = ruta_padre_area
            else:
                # RUTA IDEAL: CarpetaBase/Area/Area_Año
                ruta_destino_nube = f"{ruta_padre_area}/{nombre_subcarpeta_anual}"
                self.registrador_eventos.info(f"🥈 Preparando subida de archivo Mensual a SUB-CARPETA: {nombre_subcarpeta_anual}")

        # Construimos el Endpoint final de Graph API para subir contenido (PUT)
        endpoint_subida_archivo = f"https://graph.microsoft.com/v1.0/sites/{self.configuracion.IDENTIFICADOR_SITIO_SHAREPOINT}/drive/root:/{ruta_destino_nube}/{nombre_archivo_excel}:/content"
        
        # Invocamos el método robusto que maneja la transferencia física de los bytes
        return self._ejecutar_transferencia_bytes_robusta(endpoint_subida_archivo, ruta_archivo_local, nombre_archivo_excel)

    def _ejecutar_transferencia_bytes_robusta(self, endpoint_api, ruta_archivo_local, nombre_archivo_excel):
        """Lee el archivo local en binario y lo envía a Microsoft Graph manejando bloqueos de SharePoint (Error 423)."""
        # Intentaremos subir el archivo un máximo de 3 veces si hay fallos temporales
        for numero_intento in range(3):
            try:
                # Abrimos el archivo Excel en modo 'rb' (Lectura Binaria)
                with open(ruta_archivo_local, 'rb') as archivo_binario:
                    # Hacemos la petición PUT (Subir/Sobrescribir). Damos hasta 5 minutos (300s) por si el archivo es gigante
                    respuesta_servidor = self.sesion_http.put(endpoint_api, data=archivo_binario, timeout=300)
                
                # Códigos 200 (OK) o 201 (Created) significan éxito total
                if respuesta_servidor.status_code in [200, 201]:
                    self.registrador_eventos.info(f"✅ Archivo sincronizado con éxito en SharePoint: {nombre_archivo_excel}")
                    # Retornamos la URL web pública/institucional que Graph nos devuelve, para usarla en los correos
                    return respuesta_servidor.json().get('webUrl')
                
                # ERROR 423 (LOCKED): Ocurre frecuentemente si OneDrive/SharePoint está indexando o escaneando el archivo con el antivirus
                if respuesta_servidor.status_code == 423:
                    self.registrador_eventos.warning(f"⚠️ El archivo en la nube está bloqueado temporalmente por SharePoint (Error 423). Archivo: {nombre_archivo_excel}. Reintento {numero_intento+1} de 3...")
                    # Hacemos una pausa táctica de 15 segundos para que Microsoft suelte el candado del archivo
                    time.sleep(15) 
                    continue # Saltamos al siguiente intento del bucle 'for'
                
                # Si es cualquier otro error (ej. 403 Forbidden, 404 Not Found), lo reportamos y salimos del bucle (break)
                self.registrador_eventos.error(f"❌ Fallo desconocido en Microsoft Graph (Código {respuesta_servidor.status_code}): {respuesta_servidor.text}")
                break
                
            except Exception as excepcion_capturada:
                # Si falla la red local o el disco duro antes de siquiera hablar con Microsoft
                self.registrador_eventos.error(f"❌ Error físico de red o disco durante la transferencia de {nombre_archivo_excel}: {excepcion_capturada}")
                # Pausa breve antes de reintentar
                time.sleep(5)
        
        # Si agotamos los 3 intentos y llegamos aquí, la subida fracasó
        return None

    def obtener_informacion_enlace_carpeta(self, ruta_carpeta_nube):
        """Consulta Graph API para obtener el ID y el link web directo de una carpeta en específico (para los botones del correo)."""
        if not self.token_acceso: 
            return None, None
            
        # Endpoint GET para obtener metadata de un Item (Carpeta) en SharePoint
        url_metadata_carpeta = f"https://graph.microsoft.com/v1.0/sites/{self.configuracion.IDENTIFICADOR_SITIO_SHAREPOINT}/drive/root:/{ruta_carpeta_nube}"
        
        try:
            respuesta_servidor = self.sesion_http.get(url_metadata_carpeta)
            if respuesta_servidor.status_code == 200:
                datos_carpeta = respuesta_servidor.json()
                # Retornamos una tupla: (ID Técnico de la carpeta, Enlace URL para el usuario final)
                return datos_carpeta.get('id'), datos_carpeta.get('webUrl')
        except: 
            # Omitimos silenciosamente si falla, para no interrumpir el flujo principal del ETL
            pass
            
        return None, None