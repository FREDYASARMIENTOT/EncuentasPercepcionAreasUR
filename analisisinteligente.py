# -*- coding: utf-8 -*-
"""
MÓDULO DE INTELIGENCIA ANALÍTICA - V82.0
Lógica: Detección de Anomalías y Perfilamiento de Datos
"""
import numpy as np
import pandas as pd

class SovereignIntelligence:
    """Capa de Inteligencia para detección de riesgos y atípicos."""

    @staticmethod
    def detectar_anomalias(df_proc, df_hist, umbral_sigma=2):
        """
        Detecta si el promedio actual es una anomalía estadística respecto al histórico.
        Usa la regla de las 3 sigmas (Z-Score).
        """
        if df_hist.empty or df_proc.empty:
            return []

        alertas = []
        # Promedio histórico por métrica
        hist_stats = df_hist.groupby('Métrica')['Indicador_0_100'].agg(['mean', 'std']).reset_index()
        
        # Promedio actual
        curr_stats = df_proc.groupby('Métrica')['Indicador_0_100'].mean().reset_index()
        
        merged = pd.merge(curr_stats, hist_stats, on='Métrica', suffixes=('_curr', '_hist'))
        
        for _, r in merged.iterrows():
            if r['std'] > 0:
                z_score = (r['Indicador_0_100_curr'] - r['mean']) / r['std']
                if abs(z_score) > umbral_sigma:
                    sentido = "CAÍDA" if z_score < 0 else "MEJORA"
                    alertas.append(f"🚨 {r['Métrica']}: {sentido} atípico (Z-Score: {z_score:.2f})")
        
        return alertas

    @staticmethod
    def validar_representatividad(df_proc, meta_encuestas=10):
        """Valora si la cantidad de datos es suficiente para tomar decisiones."""
        conteo = df_proc.groupby('Servicio')['consecutivo'].nunique()
        servicios_insuficientes = conteo[conteo < meta_encuestas].index.tolist()
        return servicios_insuficientes