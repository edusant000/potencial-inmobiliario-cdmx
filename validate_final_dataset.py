# /validate_final_dataset.py

import yaml
import logging
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def print_header(title):
    """Imprime un encabezado formateado para las secciones del reporte."""
    print("\n" + "="*80)
    print(f"| {title.upper():^76} |")
    print("="*80)

def validate_final_dataset():
    """
    Realiza una validación exhaustiva del dataset final del proyecto,
    imprimiendo la mayoría de los resultados en la consola.
    """
    logging.info("--- Iniciando Validación Exhaustiva del Dataset Final ---")

    # --- 1. Carga de Datos y Configuración ---
    config_path = Path("config/params.yaml")
    if not config_path.exists():
        logging.error("No se encontró el archivo de configuración. Abortando.")
        return

    with open(config_path, "r") as f:
        params = yaml.safe_load(f)
    
    dataset_path = Path(params['paths']['output']['primary_dataset'])
    if not dataset_path.exists():
        logging.error(f"No se encontró el dataset final en '{dataset_path}'. Ejecute 'main.py' primero.")
        return
        
    logging.info(f"Cargando dataset desde: {dataset_path}")
    gdf = gpd.read_file(dataset_path)

    # --- 2. Análisis Estructural ---
    print_header("Análisis Estructural")
    print(f"Forma del dataset (filas, columnas): {gdf.shape}")
    print(f"Sistema de Coordenadas (CRS): {gdf.crs}")
    print(f"Uso de memoria: {gdf.memory_usage(deep=True).sum() / 1e6:.2f} MB")
    print("\nColumnas y Tipos de Datos (Dtypes):")
    print(gdf.dtypes)

    # --- 3. Análisis de Integridad ---
    print_header("Análisis de Integridad de Datos")
    null_counts = gdf.isnull().sum()
    total_nulls = null_counts.sum()
    print(f"Conteo total de valores nulos en el dataset: {total_nulls}")
    if total_nulls > 0:
        print("Columnas con valores nulos:")
        print(null_counts[null_counts > 0])
    
    inf_counts = np.isinf(gdf.select_dtypes(include=np.number)).sum().sum()
    print(f"Conteo total de valores infinitos en el dataset: {inf_counts}")

    # --- 4. Análisis Estadístico por Dimensión ---
    print_header("Análisis Estadístico Descriptivo")
    pd.set_option('display.float_format', '{:,.2f}'.format)

    demographic_cols = ['pob_total', 'escolaridad_promedio', 'porc_viv_con_internet']
    economic_cols = ['num_negocios', 'indice_diversidad', 'densidad_negocios', 'densidad_diversidad']
    security_cols = ['tasa_delitos_km2']
    
    print("\n--- A) Dimensión Demográfica (Censo) ---")
    print(gdf[demographic_cols].describe())
    
    print("\n--- B) Dimensión Económica (DENUE) ---")
    print(gdf[economic_cols].describe())

    print("\n--- C) Dimensión de Seguridad (FGJ) ---")
    print(gdf[security_cols].describe())

    # --- 5. Análisis de Correlación ---
    print_header("Análisis de Correlación entre Features")
    corr_cols = demographic_cols + economic_cols + security_cols
    correlation_matrix = gdf[corr_cols].corr()
    print("Matriz de Correlación:")
    print(correlation_matrix)
    
    # Generar y guardar el único gráfico indispensable: el heatmap
    plt.figure(figsize=(12, 10))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5)
    plt.title('Mapa de Calor de Correlaciones entre Features', fontsize=16)
    plt.tight_layout()
    heatmap_path = "matriz_correlacion_final.png"
    plt.savefig(heatmap_path)
    plt.close()
    logging.info(f"Mapa de calor de correlaciones guardado en: '{heatmap_path}'")

    # --- 6. Análisis de Rankings (Top 5) ---
    print_header("Análisis de Rankings (Top 5)")
    ranking_cols = ['nombre_unidad_territorial', 'pob_total', 'num_negocios', 'tasa_delitos_km2']
    
    print("\n--- Top 5 por Población Total ---")
    print(gdf.sort_values('pob_total', ascending=False)[ranking_cols].head(5).to_string(index=False))
    
    print("\n--- Top 5 por Número de Negocios ---")
    print(gdf.sort_values('num_negocios', ascending=False)[ranking_cols].head(5).to_string(index=False))

    print("\n--- Top 5 por Tasa de Delitos (más alta) ---")
    print(gdf.sort_values('tasa_delitos_km2', ascending=False)[ranking_cols].head(5).to_string(index=False))

    print("\n" + "="*80)
    logging.info("Validación finalizada exitosamente.")


if __name__ == '__main__':
    validate_final_dataset()