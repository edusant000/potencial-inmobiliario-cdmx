# /src/diagnostics/inspector.py

import pandas as pd
import geopandas as gpd
import logging
from io import StringIO

def inspect_df(df, title="Data Inspector"):
    """Imprime un resumen de diagnóstico para un DataFrame o GeoDataFrame."""
    buf = StringIO()
    df.info(buf=buf, show_counts=True)
    info_str = buf.getvalue()

    report = f"""
    ===============================================================
    |                INSPECTOR DE DATOS: {title.upper()}
    ===============================================================
    
    --- 1. FORMA DEL DATASET ---
    Filas:    {df.shape[0]:,}
    Columnas: {df.shape[1]}

    --- 2. TIPOS DE DATOS Y NULOS (desde .info()) ---
{info_str}
    --- 3. VISTA PREVIA (primeras 3 filas) ---
{df.head(3).to_string()}
    """
    if isinstance(df, gpd.GeoDataFrame):
        report += f"""
    --- 4. INFORMACIÓN GEOESPACIAL ---
    Sistema de Coordenadas (CRS): {df.crs}
    Columna de Geometría activa: '{df.geometry.name}'
        """
    report += "\n    ===============================================================\n"
    logging.info(report)

def inspect_column(series: pd.Series, title="Column Inspector"):
    """
    Imprime un análisis de diagnóstico detallado para una única columna (Serie).
    
    Args:
        series (pd.Series): La columna del DataFrame a inspeccionar.
        title (str): Un título para el reporte.
    """
    if not isinstance(series, pd.Series):
        logging.error("La entrada para 'inspect_column' debe ser una Serie de pandas (una columna de un DataFrame).")
        return

    # Análisis de tipos de datos individuales
    non_null_series = series.dropna()
    try:
        # Intenta un muestreo, si hay muy pocos elementos, toma todos
        sample_size = min(20, len(non_null_series))
        type_counts = non_null_series.sample(sample_size, random_state=1).apply(type).value_counts()
    except ValueError: # Ocurre si la serie está vacía
        type_counts = "La columna no tiene valores no nulos para analizar."

    # Intenta obtener estadísticas descriptivas
    try:
        desc_stats = series.describe().to_string()
    except Exception:
        desc_stats = "No se pudieron calcular estadísticas descriptivas (probablemente tipo no numérico)."

    report = f"""
    ===============================================================
    |                INSPECTOR DE COLUMNA: {title.upper()}
    ===============================================================
    
    --- 1. INFORMACIÓN BÁSICA ---
    Nombre de la Columna: '{series.name}'
    Tipo de Dato (Dtype): {series.dtype}
    Total de Elementos:   {len(series):,}
    Valores No Nulos:     {series.count():,}
    Valores Nulos:        {series.isnull().sum():,}
    Valores Únicos:       {series.nunique():,}

    --- 2. ANÁLISIS DE TIPOS DE VALOR (Muestra de hasta 20 elementos) ---
{type_counts}

    --- 3. VALORES MÁS FRECUENTES (Top 10) ---
{series.value_counts().head(10).to_string()}

    --- 4. ESTADÍSTICAS DESCRIPTIVAS ---
{desc_stats}
    
    ===============================================================
    """
    logging.info(report)