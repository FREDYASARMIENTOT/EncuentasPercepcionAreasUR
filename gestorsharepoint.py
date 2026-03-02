# -*- coding: utf-8 -*-
"""
GESTOR DE PERSISTENCIA CLOUD (GRAPH API) - DITIC
Versión: V82.9.16 Encueta Percepcion (Folder Nesting & 423 Auto-Recovery)
Autor: Mg. Fredy Alejandro Sarmiento Torres
"""

import os
import time
import requests
from pathlib import Path

class SharePointManager:
    """
    Administrador de persistencia en SharePoint vía Microsoft Graph.
    Prioridad: Acumulado en Raíz | Mensual en Subcarpeta Anual.
    """
    
    def __init__(self, config, logger):
        self.cfg = config
        self.logger = logger
        self.session = requests.Session()
        # Obtención del token de acceso inicial
        self.token = self._get_access_token()
        
        if self.token:
            self.session.headers.update({'Authorization': f'Bearer {self.token}'})
            self.logger.info("🔗 Conexión estable establecida vía Microsoft Graph API.")
        else:
            self.logger.error("❌ No se pudo establecer la conexión con SharePoint. Revisar SP_TENANT_ID.")

    def _get_access_token(self):
        """Obtiene el token de acceso con resiliencia ante fallos de configuración."""
        try:
            # Encueta Percepcion: Acceso seguro a Config para evitar AttributeError
            tenant_id = getattr(self.cfg, 'SP_TENANT_ID', None)
            client_id = getattr(self.cfg, 'SP_CLIENT_ID', None)
            client_secret = getattr(self.cfg, 'SP_CLIENT_SECRET', None)

            if not all([tenant_id, client_id, client_secret]):
                raise ValueError("Faltan credenciales de SharePoint en la clase Config.")

            url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            data = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret,
                'scope': 'https://graph.microsoft.com/.default'
            }
            response = self.session.post(url, data=data, timeout=15)
            response.raise_for_status()
            return response.json().get('access_token')
        except Exception as e:
            self.logger.error(f"💥 Error crítico obteniendo Token de Graph: {e}")
            return None

    def _get_or_create_folder(self, parent_path, folder_name):
        """Busca una carpeta o la crea si no existe (Recursividad Lógica)."""
        check_url = f"https://graph.microsoft.com/v1.0/sites/{self.cfg.SP_SITE_ID}/drive/root:/{parent_path}/{folder_name}"
        
        try:
            resp = self.session.get(check_url)
            if resp.status_code == 200:
                return resp.json().get('id')
            
            # Si no existe (404), la creamos
            self.logger.info(f"📂 Creando subcarpeta anual: {folder_name}")
            create_url = f"https://graph.microsoft.com/v1.0/sites/{self.cfg.SP_SITE_ID}/drive/root:/{parent_path}:/children"
            payload = {
                "name": folder_name,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "fail"
            }
            resp = self.session.post(create_url, json=payload)
            resp.raise_for_status()
            return resp.json().get('id')
        except Exception as e:
            self.logger.error(f"❌ Error gestionando carpeta {folder_name}: {e}")
            return None

    def subir_reporte_prioritario(self, ruta_local, area_nombre, es_acumulado=True):
        """
        Estrategia V82.9.16: 
        - Acumulado -> Va a la raíz del área.
        - Mensual -> Va a la subcarpeta Area_Año.
        """
        if not self.token: return None
        
        nombre_archivo = os.path.basename(ruta_local)
        anio = time.strftime("%Y") 
        
        if es_acumulado:
            target_path = f"{self.cfg.SP_BASE_PATH}/{area_nombre}"
            self.logger.info(f"🥇 Subiendo Acumulado a RAÍZ: {area_nombre}")
        else:
            nombre_sub = f"{area_nombre}_{anio}"
            parent_area = f"{self.cfg.SP_BASE_PATH}/{area_nombre}"
            
            folder_id = self._get_or_create_folder(parent_area, nombre_sub)
            if not folder_id:
                self.logger.warning("⚠️ Falló creación de subcarpeta, intentando subida a raíz.")
                target_path = parent_area
            else:
                target_path = f"{parent_area}/{nombre_sub}"
                self.logger.info(f"🥈 Subiendo Mensual a SUB-CARPETA: {nombre_sub}")

        endpoint = f"https://graph.microsoft.com/v1.0/sites/{self.cfg.SP_SITE_ID}/drive/root:/{target_path}/{nombre_archivo}:/content"
        return self._ejecutar_transferencia_robusta(endpoint, ruta_local, nombre_archivo)

    def _ejecutar_transferencia_robusta(self, endpoint, ruta_local, nombre_archivo):
        """Lógica de subida con manejo de Error 423 (Locked) y reintentos."""
        for intento in range(3):
            try:
                with open(ruta_local, 'rb') as f:
                    resp = self.session.put(endpoint, data=f, timeout=300)
                
                if resp.status_code in [200, 201]:
                    self.logger.info(f"✅ Sincronizado: {nombre_archivo}")
                    return resp.json().get('webUrl')
                
                if resp.status_code == 423:
                    self.logger.warning(f"⚠️ Recurso bloqueado (423) en {nombre_archivo}. Reintento {intento+1}/3...")
                    time.sleep(15) 
                    continue
                
                self.logger.error(f"❌ Fallo Graph ({resp.status_code}): {resp.text}")
                break
            except Exception as e:
                self.logger.error(f"❌ Error en transferencia de {nombre_archivo}: {e}")
                time.sleep(5)
        
        return None

    def get_folder_info(self, path_sp):
        """Recupera el ID y Link de una carpeta específica."""
        if not self.token: return None, None
        url = f"https://graph.microsoft.com/v1.0/sites/{self.cfg.SP_SITE_ID}/drive/root:/{path_sp}"
        try:
            resp = self.session.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('id'), data.get('webUrl')
        except: pass
        return None, None