# -*- coding: utf-8 -*-
"""
GESTOR DE NOTIFICACIONES Y RENDERIZADO HTML - DITIC
Versión: V82.9.27 Titanium Sovereign (Fix: SyntaxError compatible)
Autor: Mg. Fredy Alejandro Sarmiento Torres
"""

import smtplib
import datetime
import os
import traceback
from email.message import EmailMessage
from sqlalchemy import text

class NotificationManager:
    """Gestor de comunicaciones con Mini-Consola de Hitos y Triple Acceso SharePoint."""
    
    def __init__(self, config, logger):
        self.cfg = config
        self.logger = logger
        self.kpi_goal = getattr(config, 'KPI_GOAL', 80.0)
        self.kpi_critical = getattr(config, 'KPI_CRITICAL', 70.0)
        # Logo Institucional UR
        self.logo_url = "https://www.urosario.edu.co/sites/default/files/logo-urosario-footer.png"

    def _obtener_destinatarios(self, engine, area):
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT correo FROM chatbot.NotificacionesEncuestas 
                    WHERE activo=1 AND (areaNombre=:a OR areaNombre IS NULL)
                """)
                res = conn.execute(query, {'a': area}).fetchall()
                return list(set([r[0] for r in res])) if res else self.cfg.FALLBACK_RECIPIENTS
        except Exception as e:
            self.logger.warning(f"⚠️ Error en destinatarios: {e}")
            return self.cfg.FALLBACK_RECIPIENTS

    def _render_summary_tables(self, df):
        if df is None or df.empty:
            return "<p style='color:#777;'>Analítica tabular no generada.</p>"
        
        try:
            agg = df.groupby(['Sede', 'Servicio']).agg(
                Encuestas=('consecutivo', 'nunique'),
                Promedio=('Indicador_0_100', 'mean')
            ).reset_index()

            html = """
            <h3 style="color:#1F4E78; font-family:Arial,sans-serif; border-bottom:2px solid #E7F0F7; padding-bottom:10px;">
                📊 Desempeño Ejecutivo por Servicio
            </h3>
            <table border="0" cellpadding="10" cellspacing="0" style="width:100%; font-family:Arial,sans-serif; font-size:12px; border-radius:8px; overflow:hidden;">
                <tr style="background-color:#1F4E78; color:white; text-align:center;">
                    <th>Sede</th><th>Servicio</th><th>Muestra</th><th>Indicador</th>
                </tr>
            """
            for i, r in agg.iterrows():
                score = r['Promedio']
                bg = "#FFFFFF" if i % 2 == 0 else "#F9F9F9"
                color = "28a745" if score >= self.kpi_goal else "dc3545" if score <= self.kpi_critical else "ffc107"
                
                html += f"""
                <tr style="background-color:{bg};">
                    <td style="padding:10px; border-bottom:1px solid #eee;">{r['Sede']}</td>
                    <td style="padding:10px; border-bottom:1px solid #eee;">{r['Servicio']}</td>
                    <td align="center" style="padding:10px; border-bottom:1px solid #eee;">{int(r['Encuestas'])}</td>
                    <td align="center" style="padding:10px; font-weight:bold; color:#{color}; border-bottom:1px solid #eee;">{score:.1f}%</td>
                </tr>
                """
            return html + "</table><br>"
        except Exception: return ""

    def enviar_reporte_ejecutivo(self, engine, p):
        """Reporte Soberano con Triple Link y Consola de Hitos."""
        destinatarios = self._obtener_destinatarios(engine, p['area'])
        
        btn = "padding:12px 18px; text-decoration:none; border-radius:5px; font-weight:bold; display:inline-block; margin:5px; font-size:13px; color:white; text-align:center; min-width:150px;"
        
        # --- MAPEO DE ACCESOS SHAREPOINT ---
        link_acumulado = p.get('link_acumulado_sp')
        link_subcarpeta_mensual = p.get('link_folder_mensual_sp')
        link_raiz_area = p.get('link_folder_raiz_sp')

        blk_a = f'<a href="{link_acumulado}" style="{btn} background-color:#1F4E78;">📈 ACUMULADO (RAÍZ)</a>' if link_acumulado else ""
        blk_m = f'<a href="{link_subcarpeta_mensual}" style="{btn} background-color:#28a745;">📂 CARPETA {p["anio"]}</a>' if link_subcarpeta_mensual else ""
        blk_r = f'<a href="{link_raiz_area}" style="{btn} background-color:#ffc107; color:#333;">🏠 INICIO ÁREA</a>' if link_raiz_area else ""

        # --- CORRECCIÓN DE SYNTAX ERROR: Procesar log_tail antes de la f-string ---
        raw_log = p.get('log_tail', 'Iniciando trazabilidad...')
        clean_log_tail = raw_log.replace('\n', '<br>') # Eliminada la barra invertida problemática de la f-string

        log_console = f"""
        <div style="background-color:#1e1e1e; color:#00ff00; padding:15px; border-radius:5px; font-family:'Courier New', monospace; font-size:11px; margin-top:20px; border:1px solid #333;">
            <div style="color:#aaa; border-bottom:1px solid #333; padding-bottom:5px; margin-bottom:10px;">> TITANIUM_CONSOLE_OUTPUT [v82.9.27]</div>
            {clean_log_tail}
            <div style="margin-top:10px; color:#555;">_SYSTEM_READY_</div>
        </div>
        """

        html = f"""
        <html>
            <body style="background-color:#f4f4f4; padding:20px; font-family:'Segoe UI',Arial,sans-serif;">
                <div style="max-width:700px; margin:0 auto; background:white; border-radius:12px; overflow:hidden; box-shadow:0 8px 30px rgba(0,0,0,0.12);">
                    <div style="background-color:#AF2024; padding:30px; text-align:center;">
                        <img src="{self.logo_url}" width="160" style="filter: brightness(0) invert(1);">
                        <h2 style="color:white; margin:15px 0 0 0; font-weight:300;">Análisis de Percepción Soberano</h2>
                    </div>
                    
                    <div style="padding:30px; color:#333;">
                        <p style="font-size:15px;">Proceso finalizado para: <strong>{p['area']}</strong> ({p['mes']}/{p['anio']}).</p>
                        
                        <div style="background:#F8F9FA; padding:20px; border-radius:8px; margin:20px 0; border-left:5px solid #1F4E78;">
                            <table width="100%" style="font-size:13px;">
                                <tr>
                                    <td><strong>Encuestas:</strong> {p.get('k_consecutivos', 0):,}</td>
                                    <td><strong>Duración:</strong> {str(datetime.timedelta(seconds=int(p.get('duration', 0))))}</td>
                                </tr>
                                <tr>
                                    <td><strong>Motor:</strong> {p.get('script_version', 'V82.9.27')}</td>
                                    <td><strong>Modo:</strong> {p.get('ejecucion_mode', 'MANUAL')}</td>
                                </tr>
                            </table>
                        </div>

                        {self._render_summary_tables(p.get('df_proc'))}

                        <div style="text-align:center; margin:30px 0; padding:20px; background:#fafafa; border:1px dashed #ccc; border-radius:8px;">
                            <p style="margin-bottom:15px; font-weight:bold; color:#666;">ACCESO DIRECTO SHAREPOINT:</p>
                            {blk_a} {blk_m} {blk_r}
                        </div>

                        {log_console}
                    </div>
                    <div style="background:#1F4E78; padding:15px; text-align:center; font-size:11px; color:#aec6cf;">
                        DITIC - Analítica de Información | Ing. Fredy Alejandro Sarmiento Torres
                    </div>
                </div>
            </body>
        </html>
        """
        return self._send_smtp(f"✅ Éxito ETL: {p['area']} ({p['mes']}/{p['anio']})", destinatarios, html)

    def enviar_alerta_tecnica(self, area, error_msg, trace):
        color = "dc3545"
        html = f"""
        <div style="font-family:Arial; border:2px solid #{color}; padding:25px; border-radius:10px;">
            <h2 style="color:#{color}; margin-top:0;">🚨 FALLO CRÍTICO TITANIUM</h2>
            <p><strong>Área:</strong> {area} | <strong>Error:</strong> {error_msg}</p>
            <pre style="background:#f8f8f8; padding:15px; font-size:11px; color:#555;">{trace}</pre>
        </div>
        """
        return self._send_smtp(f"🔥 ERROR CRÍTICO: {area}", self.cfg.FALLBACK_RECIPIENTS, html)

    def _send_smtp(self, subject, recipients, html):
        try:
            msg = EmailMessage()
            msg['Subject'] = subject
            msg['From'] = self.cfg.SMTP_FROM
            msg['To'] = ", ".join(recipients)
            msg.add_alternative(html, subtype='html')
            with smtplib.SMTP(self.cfg.SMTP_SERVER, self.cfg.SMTP_PORT) as server:
                server.starttls()
                server.login(self.cfg.SMTP_USER, self.cfg.SMTP_PASS)
                server.send_message(msg)
            return True
        except Exception as e:
            self.logger.error(f"❌ Error SMTP: {e}")
            return False