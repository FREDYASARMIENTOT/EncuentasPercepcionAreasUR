# -*- coding: utf-8 -*-
"""
MÓDULO DE INTELIGENCIA ANALÍTICA E IDENTIFICACIÓN DE ANOMALÍAS - DITIC
Archivo: analitica_sentimiento_inteligencia_datos.py
Versión: V82.9.34 (Refactorización a Código Limpio en Español)
Lógica: Detección estadística de anomalías (Z-Score) y perfilamiento de representatividad de datos.
"""

# Importamos numpy para cálculos matemáticos avanzados (aunque pandas lo usa por debajo, es buena práctica declararlo)
import numpy as np
# Importamos pandas con su alias estándar en la industria para el manejo ágil de estructuras de datos masivas
import pandas as pd

class AnaliticaInteligenciaDatosDITIC:
    """Capa de Inteligencia de Negocios para detección automática de riesgos, desviaciones atípicas y control de calidad."""

    @staticmethod
    def detectar_anomalias_estadisticas_z_score(dataframe_procesado_actual, dataframe_historico_acumulado, umbral_desviacion_estandar_sigma=2):
        """
        Detecta si el promedio del mes actual es una anomalía estadística en comparación con todo el comportamiento histórico.
        Utiliza la regla empírica de las varianzas (Z-Score) para identificar caídas o picos anormales.
        """
        
        
        # Validamos preventivamente que ninguno de los dos conjuntos de datos esté vacío para evitar errores matemáticos
        if dataframe_historico_acumulado.empty or dataframe_procesado_actual.empty:
            # Si alguno está vacío, no hay forma de comparar, retornamos una lista vacía de alertas
            return []

        # Inicializamos la lista donde almacenaremos los mensajes de advertencia de las métricas atípicas
        lista_alertas_generadas = []
        
        # 1. PERFILAMIENTO HISTÓRICO:
        # Agrupamos el archivo acumulado por 'Métrica' y calculamos su Media ('mean') y Desviación Estándar ('std')
        estadisticas_historicas = dataframe_historico_acumulado.groupby('Métrica')['Indicador_0_100'].agg(['mean', 'std']).reset_index()
        
        # 2. PERFILAMIENTO ACTUAL:
        # Agrupamos los datos del mes en curso por 'Métrica' y calculamos su promedio actual
        estadisticas_actuales = dataframe_procesado_actual.groupby('Métrica')['Indicador_0_100'].mean().reset_index()
        
        # 3. CRUCE DE DATOS:
        # Unimos ambas tablas usando la columna 'Métrica' como llave primaria, agregando sufijos para no confundir las columnas
        datos_estadisticos_cruzados = pd.merge(estadisticas_actuales, estadisticas_historicas, on='Métrica', suffixes=('_actual', '_historico'))
        
        # 4. EVALUACIÓN ESTADÍSTICA:
        # Iteramos sobre cada fila de la tabla cruzada para aplicar la fórmula Z-Score
        for indice_fila, fila_datos in datos_estadisticos_cruzados.iterrows():
            # Solo podemos calcular la anomalía si la desviación estándar es mayor a cero (evita división por cero)
            if fila_datos['std'] > 0:
                # Fórmula Z-Score = (Valor Actual - Media Histórica) / Desviación Estándar Histórica
                puntaje_z_score = (fila_datos['Indicador_0_100_actual'] - fila_datos['mean']) / fila_datos['std']
                
                # Comparamos el valor absoluto del Z-Score contra el umbral permitido (por defecto 2 sigmas)
                if abs(puntaje_z_score) > umbral_desviacion_estandar_sigma:
                    # Si el Z-Score es negativo, significa que la calificación cayó; si es positivo, mejoró atípicamente
                    sentido_anomalia = "CAÍDA" if puntaje_z_score < 0 else "MEJORA"
                    # Ensamblamos el mensaje de alerta y lo agregamos a nuestra lista
                    lista_alertas_generadas.append(f"🚨 {fila_datos['Métrica']}: {sentido_anomalia} atípico (Puntaje Z-Score: {puntaje_z_score:.2f})")
        
        # Devolvemos la lista final de anomalías detectadas para que sean informadas por correo
        return lista_alertas_generadas

    @staticmethod
    def validar_representatividad_muestra_estadistica(dataframe_procesado_actual, meta_minima_encuestas=10):
        """
        Audita el volumen de datos para advertir si la cantidad de encuestas respondidas es insuficiente
        para tomar decisiones ejecutivas o sacar conclusiones válidas sobre un servicio en particular.
        """
        # Agrupamos por 'Servicio' y contamos cuántos identificadores únicos ('consecutivo') de encuesta existen
        conteo_encuestas_por_servicio = dataframe_procesado_actual.groupby('Servicio')['consecutivo'].nunique()
        
        # Filtramos los servicios cuyo volumen total de encuestas sea estrictamente menor a la meta exigida
        # Luego extraemos los nombres de esos servicios (el índice) y los convertimos en una lista de Python
        servicios_con_muestra_insuficiente = conteo_encuestas_por_servicio[conteo_encuestas_por_servicio < meta_minima_encuestas].index.tolist()
        
        # Retornamos la lista de servicios que carecen de significancia estadística
        return servicios_con_muestra_insuficiente