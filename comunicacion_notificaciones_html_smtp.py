# -*- coding: utf-8 -*-
"""
GESTOR DE COMUNICACIONES Y RENDERIZADO DE CORREOS HTML - DITIC
Archivo: comunicacion_notificaciones_html_smtp.py
Versión: V82.9.34 (Refactorización a Código Limpio en Español)
Autor: Mg. Fredy Alejandro Sarmiento Torres
Lógica: Envío de alertas ejecutivas, técnicas y de orquestación global vía Office 365.
"""

# Importamos smtplib para el protocolo de envío de correos
import smtplib
# Importamos datetime para estampar la fecha y hora de los envíos
import datetime
# Importamos os para interactuar con el sistema
import os
# Importamos traceback para capturar errores técnicos y enviarlos por correo
import traceback
# Importamos EmailMessage para estructurar correos modernos con soporte HTML
from email.message import EmailMessage
# Importamos text para ejecutar la consulta SQL de destinatarios
from sqlalchemy import text

class GestorComunicacionNotificaciones:
    """Clase encargada de renderizar reportes HTML y enviar correos usando autenticación SMTP Institucional."""
    
    def __init__(self, configuracion_sistema, registrador_eventos):
        """Inicializa el gestor cargando las metas institucionales y el logger."""
        # Guardamos la configuración centralizada (el archivo 1 que ya refactorizamos)
        self.configuracion = configuracion_sistema
        # Guardamos el logger para la auditoría de envíos
        self.registrador_eventos = registrador_eventos
        # Extraemos la meta de desempeño (ej. 80.0%) desde la configuración de forma segura
        self.meta_indicador_desempeno = getattr(configuracion_sistema, 'META_INDICADOR_DESEMPENO', 80.0)
        # Extraemos el límite crítico (ej. 70.0%) para pintar de rojo los resultados bajos
        self.limite_critico_desempeno = getattr(configuracion_sistema, 'LIMITE_CRITICO_INDICADOR_DESEMPENO', 70.0)
        # Asignamos la URL oficial del logo de la Universidad para las cabeceras de los correos
        self.url_logotipo_institucional = "https://www.urosario.edu.co/sites/default/files/logo-urosario-footer.png"

    def _obtener_lista_destinatarios_bd(self, motor_base_datos, nombre_area):
        """Consulta en SQL Server la lista de correos autorizados para recibir el reporte de un área específica."""
        try:
            # Abrimos la conexión al motor de base de datos de auditoría
            with motor_base_datos.connect() as conexion_sql:
                # Preparamos la consulta para traer los correos activos del área (o los que aplican a todas: NULL)
                consulta_sql = text("""
                    SELECT correo FROM chatbot.NotificacionesEncuestas 
                    WHERE activo=1 AND (areaNombre=:area_parametro OR areaNombre IS NULL)
                """)
                # Ejecutamos la consulta inyectando el nombre del área
                resultados = conexion_sql.execute(consulta_sql, {'area_parametro': nombre_area}).fetchall()
                # Extraemos el primer elemento de cada tupla, eliminamos duplicados con 'set()' y lo pasamos a lista
                return list(set([fila[0] for fila in resultados])) if resultados else self.configuracion.DESTINATARIOS_CORREO_POR_DEFECTO
        except Exception as excepcion_capturada:
            # Si la consulta falla (ej. tabla no existe), advertimos y usamos tu correo de emergencia (Fallback)
            self.registrador_eventos.warning(f"⚠️ Error obteniendo destinatarios de BD, usando lista por defecto: {excepcion_capturada}")
            return self.configuracion.DESTINATARIOS_CORREO_POR_DEFECTO

    def _renderizar_tablas_resumen_html(self, dataframe_procesado):
        """Genera el código HTML de la tabla de desempeño cruzando Sedes y Servicios."""
        # Si no hay datos, retornamos un mensaje sutil en gris
        if dataframe_procesado is None or dataframe_procesado.empty:
            return "<p style='color:#777;'>Analítica tabular no generada debido a falta de datos.</p>"
        
        try:
            # Agrupamos los datos por Sede y Servicio
            datos_agrupados = dataframe_procesado.groupby(['Sede', 'Servicio']).agg(
                # Contamos encuestas únicas (nunique)
                Encuestas_Unicas=('consecutivo', 'nunique'),
                # Calculamos el promedio del indicador de 0 a 100
                Promedio_Indicador=('Indicador_0_100', 'mean')
            ).reset_index()

            # Iniciamos la construcción del HTML con la cabecera de la tabla y estilos en línea institucionales
            codigo_html_tabla = """
            <h3 style="color:#1F4E78; font-family:Arial,sans-serif; border-bottom:2px solid #E7F0F7; padding-bottom:10px;">
                📊 Desempeño Ejecutivo por Servicio
            </h3>
            <table border="0" cellpadding="10" cellspacing="0" style="width:100%; font-family:Arial,sans-serif; font-size:12px; border-radius:8px; overflow:hidden;">
                <tr style="background-color:#1F4E78; color:white; text-align:center;">
                    <th>Sede</th><th>Servicio</th><th>Muestra</th><th>Indicador</th>
                </tr>
            """
            
            # Iteramos sobre los resultados agrupados
            for indice_fila, fila_datos in datos_agrupados.iterrows():
                puntaje_promedio = fila_datos['Promedio_Indicador']
                # Alternamos colores de fondo (blanco/gris claro) para facilitar la lectura (efecto cebra)
                color_fondo_fila = "#FFFFFF" if indice_fila % 2 == 0 else "#F9F9F9"
                
                # Semáforo de colores: Verde (>= Meta), Rojo (<= Crítico), Amarillo (Intermedio)
                if puntaje_promedio >= self.meta_indicador_desempeno:
                    color_texto_semaforo = "28a745" # Verde
                elif puntaje_promedio <= self.limite_critico_desempeno:
                    color_texto_semaforo = "dc3545" # Rojo
                else:
                    color_texto_semaforo = "ffc107" # Amarillo
                
                # Inyectamos la fila en el HTML
                codigo_html_tabla += f"""
                <tr style="background-color:{color_fondo_fila};">
                    <td style="padding:10px; border-bottom:1px solid #eee;">{fila_datos['Sede']}</td>
                    <td style="padding:10px; border-bottom:1px solid #eee;">{fila_datos['Servicio']}</td>
                    <td align="center" style="padding:10px; border-bottom:1px solid #eee;">{int(fila_datos['Encuestas_Unicas'])}</td>
                    <td align="center" style="padding:10px; font-weight:bold; color:#{color_texto_semaforo}; border-bottom:1px solid #eee;">{puntaje_promedio:.1f}%</td>
                </tr>
                """
            # Cerramos la etiqueta de la tabla y retornamos el bloque HTML
            return codigo_html_tabla + "</table><br>"
        except Exception: 
            return ""

    def enviar_reporte_ejecutivo_mensual(self, motor_base_datos, diccionario_parametros):
        """Construye y envía el correo con el informe mensual, los 3 enlaces de SharePoint y la consola de auditoría."""
        # Consultamos a quién debe llegar el correo
        lista_destinatarios = self._obtener_lista_destinatarios_bd(motor_base_datos, diccionario_parametros['area'])
        
        # Estilos compartidos para los botones interactivos
        estilo_boton = "padding:12px 18px; text-decoration:none; border-radius:5px; font-weight:bold; display:inline-block; margin:5px; font-size:13px; color:white; text-align:center; min-width:150px;"
        
        # Extraemos los enlaces desde el diccionario de parámetros
        enlace_acumulado = diccionario_parametros.get('link_acumulado_sp')
        enlace_carpeta_mensual = diccionario_parametros.get('link_folder_mensual_sp')
        enlace_carpeta_raiz = diccionario_parametros.get('link_folder_raiz_sp')

        # Si el enlace existe, creamos su botón, de lo contrario lo dejamos en blanco
        bloque_boton_acumulado = f'<a href="{enlace_acumulado}" style="{estilo_boton} background-color:#1F4E78;">📈 ACUMULADO (RAÍZ)</a>' if enlace_acumulado else ""
        bloque_boton_mensual = f'<a href="{enlace_carpeta_mensual}" style="{estilo_boton} background-color:#28a745;">📂 CARPETA {diccionario_parametros["anio"]}</a>' if enlace_carpeta_mensual else ""
        bloque_boton_raiz = f'<a href="{enlace_carpeta_raiz}" style="{estilo_boton} background-color:#ffc107; color:#333;">🏠 INICIO ÁREA</a>' if enlace_carpeta_raiz else ""

        # Procesamos el texto del log técnico reemplazando saltos de línea por <br> para HTML
        registro_consola_crudo = diccionario_parametros.get('log_tail', 'Iniciando trazabilidad...')
        registro_consola_limpio = registro_consola_crudo.replace('\n', '<br>') 

        # Creamos la mini-consola negra con letras verdes (estilo hacker/terminal)
        codigo_html_consola = f"""
        <div style="background-color:#1e1e1e; color:#00ff00; padding:15px; border-radius:5px; font-family:'Courier New', monospace; font-size:11px; margin-top:20px; border:1px solid #333;">
            <div style="color:#aaa; border-bottom:1px solid #333; padding-bottom:5px; margin-bottom:10px;">> CONSOLA_ORQUESTADOR_DITIC [V82.9.34]</div>
            {registro_consola_limpio}
            <div style="margin-top:10px; color:#555;">_SISTEMA_COMPLETADO_</div>
        </div>
        """

        # Ensamblamos la maqueta completa del correo institucional
        cuerpo_html_final = f"""
        <html>
            <body style="background-color:#f4f4f4; padding:20px; font-family:'Segoe UI',Arial,sans-serif;">
                <div style="max-width:700px; margin:0 auto; background:white; border-radius:12px; overflow:hidden; box-shadow:0 8px 30px rgba(0,0,0,0.12);">
                    <div style="background-color:#AF2024; padding:30px; text-align:center;">
                        <img src="{self.url_logotipo_institucional}" width="160" style="filter: brightness(0) invert(1);">
                        <h2 style="color:white; margin:15px 0 0 0; font-weight:300;">Análisis de Percepción de Servicios</h2>
                    </div>
                    
                    <div style="padding:30px; color:#333;">
                        <p style="font-size:15px;">Proceso automatizado finalizado para el área: <strong>{diccionario_parametros['area']}</strong> (Periodo: {diccionario_parametros['mes']}/{diccionario_parametros['anio']}).</p>
                        
                        <div style="background:#F8F9FA; padding:20px; border-radius:8px; margin:20px 0; border-left:5px solid #1F4E78;">
                            <table width="100%" style="font-size:13px;">
                                <tr>
                                    <td><strong>Encuestas Procesadas:</strong> {diccionario_parametros.get('k_consecutivos', 0):,}</td>
                                    <td><strong>Duración del Proceso:</strong> {str(datetime.timedelta(seconds=int(diccionario_parametros.get('duration', 0))))}</td>
                                </tr>
                                <tr>
                                    <td><strong>Versión del Motor:</strong> {diccionario_parametros.get('script_version', 'V82.9.34')}</td>
                                    <td><strong>Modo de Ejecución:</strong> {diccionario_parametros.get('ejecucion_mode', 'MANUAL')}</td>
                                </tr>
                            </table>
                        </div>

                        {self._renderizar_tablas_resumen_html(diccionario_parametros.get('df_proc'))}

                        <div style="text-align:center; margin:30px 0; padding:20px; background:#fafafa; border:1px dashed #ccc; border-radius:8px;">
                            <p style="margin-bottom:15px; font-weight:bold; color:#666;">ACCESO DIRECTO A REPOSITORIO SHAREPOINT:</p>
                            {bloque_boton_acumulado} {bloque_boton_mensual} {bloque_boton_raiz}
                        </div>

                        {codigo_html_consola}
                    </div>
                    <div style="background:#1F4E78; padding:15px; text-align:center; font-size:11px; color:#aec6cf;">
                        DITIC - Coordinación de Analítica de Información | Ing. Fredy Alejandro Sarmiento Torres
                    </div>
                </div>
            </body>
        </html>
        """
        # Ejecutamos el envío final
        asunto_correo = f"✅ Éxito ETL: {diccionario_parametros['area']} ({diccionario_parametros['mes']}/{diccionario_parametros['anio']})"
        return self._ejecutar_envio_correo_smtp(asunto_correo, lista_destinatarios, cuerpo_html_final)

    def enviar_alerta_fallo_tecnico(self, nombre_area, mensaje_error, traza_pila_error):
        """Notifica inmediatamente si el motor Python falla procesando un área."""
        color_alerta = "dc3545" # Rojo
        cuerpo_html_alerta = f"""
        <div style="font-family:Arial; border:2px solid #{color_alerta}; padding:25px; border-radius:10px;">
            <h2 style="color:#{color_alerta}; margin-top:0;">🚨 FALLO CRÍTICO - MOTOR DE ENCUESTAS DITIC</h2>
            <p><strong>Área Afectada:</strong> {nombre_area} | <strong>Error Reportado:</strong> {mensaje_error}</p>
            <pre style="background:#f8f8f8; padding:15px; font-size:11px; color:#555; overflow-x:auto;">{traza_pila_error}</pre>
        </div>
        """
        asunto_error = f"🔥 ERROR CRÍTICO ETL: {nombre_area}"
        # Se envía la alerta técnica solo a la lista de emergencia (tu correo)
        return self._ejecutar_envio_correo_smtp(asunto_error, self.configuracion.DESTINATARIOS_CORREO_POR_DEFECTO, cuerpo_html_alerta)

    def enviar_alerta_global_orquestador(self, tipo_alerta, lista_areas, mes_proceso, anio_proceso, informacion_adicional=""):
        """Envía una notificación de texto plano (sin HTML) para anunciar el arranque o fin masivo de las tareas programadas."""
        mensaje_correo = EmailMessage()
        nombre_servidor_host = "srvcbqabi.urosario.edu"
        
        if tipo_alerta == 'INICIO':
            mensaje_correo['Subject'] = f"🚀 INICIO DE EJECUCIÓN MASIVA: Encuestas Percepción {mes_proceso}/{anio_proceso}"
            # Convertimos la lista de áreas en un texto con saltos de línea y viñetas
            texto_lista_areas = '\n'.join([f"  - {area}" for area in lista_areas])
            cuerpo_texto_plano = f"""Hola Fredy,

El servidor {nombre_servidor_host} ha iniciado la ejecución automática masiva del ETL de Encuestas de Percepción.

🕒 Hora de arranque: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📅 Periodo objetivo: {mes_proceso}/{anio_proceso}
📂 Áreas detectadas en la base de datos ({len(lista_areas)}):
{texto_lista_areas}

Recibirás correos ejecutivos a medida que se procese cada área, y un aviso final cuando todo concluya.

Atentamente,
Orquestador Central de Analítica - DITIC"""
        else:
            mensaje_correo['Subject'] = f"✅ FIN DE EJECUCIÓN MASIVA: Encuestas Percepción {mes_proceso}/{anio_proceso}"
            cuerpo_texto_plano = f"""Hola Fredy,

El servidor {nombre_servidor_host} ha finalizado el ciclo masivo de procesamiento de encuestas.

🕒 Hora de finalización: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏱️ Duración total de la tarea: {informacion_adicional}
📂 Total de áreas gestionadas: {len(lista_areas)}

El proceso se completó. Puedes revisar el archivo maestro de Logs en el disco local F:.

Atentamente,
Orquestador Central de Analítica - DITIC"""
            
        # Asignamos el cuerpo de texto plano al correo
        mensaje_correo.set_content(cuerpo_texto_plano)
        # Remitente institucional
        mensaje_correo['From'] = self.configuracion.REMITENTE_CORREO_SMTP
        # Destinatario exclusivo (Fredy) para las alertas globales del servidor
        mensaje_correo['To'] = self.configuracion.USUARIO_CORREO_SMTP

        try:
            # Iniciamos la conexión SMTP explícita con Office 365
            with smtplib.SMTP(self.configuracion.SERVIDOR_CORREO_SMTP, self.configuracion.PUERTO_CORREO_SMTP) as servidor_smtp:
                servidor_smtp.starttls()
                servidor_smtp.login(self.configuracion.USUARIO_CORREO_SMTP, self.configuracion.CONTRASENA_CORREO_SMTP)
                servidor_smtp.send_message(mensaje_correo)
            self.registrador_eventos.info(f"📧 Alerta de orquestador ({tipo_alerta}) enviada exitosamente al administrador.")
        except Exception as excepcion_capturada:
            self.registrador_eventos.error(f"❌ Error enviando alerta de orquestador ({tipo_alerta}): {excepcion_capturada}")

    def _ejecutar_envio_correo_smtp(self, asunto_correo, lista_destinatarios, contenido_cuerpo_html):
        """Método privado maestro que realiza el envío real autenticado hacia el servidor SMTP de la Universidad."""
        try:
            mensaje_correo = EmailMessage()
            mensaje_correo['Subject'] = asunto_correo
            mensaje_correo['From'] = self.configuracion.REMITENTE_CORREO_SMTP
            # Unimos la lista de destinatarios separados por comas
            mensaje_correo['To'] = ", ".join(lista_destinatarios)
            # Agregamos el contenido HTML al mensaje
            mensaje_correo.add_alternative(contenido_cuerpo_html, subtype='html')
            
            # Apertura segura de conexión SMTP (se cierra sola al salir del bloque 'with')
            with smtplib.SMTP(self.configuracion.SERVIDOR_CORREO_SMTP, self.configuracion.PUERTO_CORREO_SMTP) as servidor_smtp:
                # Activamos el cifrado TLS exigido por Microsoft
                servidor_smtp.starttls()
                # Autenticación con credenciales extraídas de forma segura
                servidor_smtp.login(self.configuracion.USUARIO_CORREO_SMTP, self.configuracion.CONTRASENA_CORREO_SMTP)
                # Ejecutamos el envío
                servidor_smtp.send_message(mensaje_correo)
            return True
        except Exception as excepcion_capturada:
            self.registrador_eventos.error(f"❌ Error crítico en protocolo SMTP: {excepcion_capturada}")
            return False