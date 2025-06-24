# /src/features/build_features.py

import geopandas as gpd
import pandas as pd
import logging
import numpy as np
from pathlib import Path

from src.diagnostics.inspector import inspect_column

def merge_agebs_with_census(agebs_gdf: gpd.GeoDataFrame, census_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Une los datos del censo al GeoDataFrame de AGEBs y maneja los faltantes."""
    logging.info("Uniendo geometrías de AGEB con datos censales.")
    enriched_gdf = pd.merge(agebs_gdf, census_df, on='cvegeo', how='left')
    
    faltantes = enriched_gdf['pob_total'].isnull().sum()
    logging.info(f"Prueba de Sanidad: {faltantes} AGEBs no encontraron datos censales.")
    
    enriched_gdf.dropna(subset=['pob_total'], inplace=True)
    logging.info(f"AGEBs con datos válidos restantes: {len(enriched_gdf)}")
    
    return enriched_gdf

def perform_areal_interpolation(units_gdf: gpd.GeoDataFrame, enriched_agebs_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Realiza la interpolación areal para transferir datos de AGEBs a Unidades Territoriales."""
    logging.info("Iniciando interpolación areal (overlay/intersección)... Esto puede tardar.")
    # Asegurarse de que ambos GeoDataFrames tienen geometrías válidas
    units_gdf = units_gdf[~units_gdf.geometry.is_empty & units_gdf.geometry.is_valid]
    enriched_agebs_gdf = enriched_agebs_gdf[~enriched_agebs_gdf.geometry.is_empty & enriched_agebs_gdf.geometry.is_valid]

    intersection_gdf = gpd.overlay(units_gdf, enriched_agebs_gdf, how="intersection")

    logging.info("Calculando pesos de área para la ponderación.")
    intersection_gdf['fragmento_area'] = intersection_gdf.geometry.area
    
    # Se asegura de que la columna de área total del AGEB no sea cero para evitar divisiones por cero.
    # También se ponderan tanto las columnas del Censo como las del DENUE.
    
    intersection_gdf['ageb_area_total_safe'] = intersection_gdf['ageb_area_total'] + 1e-9
    intersection_gdf['peso_area'] = intersection_gdf['fragmento_area'] / intersection_gdf['ageb_area_total_safe']

    count_cols_to_pond = [
        'pob_total', 'viviendas_totales', 'viv_con_internet',
        'num_negocios', 'indice_diversidad'
    ]
    for col in count_cols_to_pond:
        if col in intersection_gdf.columns:
            intersection_gdf[col + '_pond'] = intersection_gdf[col] * intersection_gdf['peso_area']

    if 'escolaridad_promedio' in intersection_gdf.columns and 'pob_total_pond' in intersection_gdf.columns:
        intersection_gdf['escolaridad_x_pob'] = intersection_gdf['escolaridad_promedio'] * intersection_gdf['pob_total_pond']
    
    return intersection_gdf


def aggregate_to_territorial_units(intersection_gdf: gpd.GeoDataFrame, epsilon: float) -> pd.DataFrame:
    """Agrega TODOS los datos ponderados a nivel de Unidad Territorial."""
    logging.info("Agregando todos los datos a nivel de Unidad Territorial.")

    try:
        epsilon_float = float(epsilon)
    except (ValueError, TypeError):
        logging.warning(f"Valor de epsilon no válido. Usando 1e-9 por defecto.")
        epsilon_float = 1e-9

    agg_dict = {
        'pob_total_pond': 'sum',
        'viviendas_totales_pond': 'sum',
        'viv_con_internet_pond': 'sum',
        'escolaridad_x_pob': 'sum',
        'num_negocios_pond': 'sum',
        'indice_diversidad_pond': 'sum',
        'densidad_negocios': lambda s: np.average(s, weights=intersection_gdf.loc[s.index, 'fragmento_area']),
        'densidad_diversidad': lambda s: np.average(s, weights=intersection_gdf.loc[s.index, 'fragmento_area']),
        'tasa_delitos_km2': lambda s: np.average(s, weights=intersection_gdf.loc[s.index, 'fragmento_area'])
    }
    
    final_agg_dict = {k: v for k, v in agg_dict.items() if k in intersection_gdf.columns}
    aggregated_data = intersection_gdf.groupby('cve_unidad_territorial').agg(final_agg_dict)
    
    aggregated_data.rename(columns={
        'pob_total_pond': 'pob_total',
        'viviendas_totales_pond': 'viviendas_totales',
        'viv_con_internet_pond': 'viv_con_internet',
        'num_negocios_pond': 'num_negocios',
        'indice_diversidad_pond': 'indice_diversidad'
    }, inplace=True)

    logging.info("Calculando indicadores finales (promedios y porcentajes).")
    # Se usa la variable epsilon convertida a float
    aggregated_data['escolaridad_promedio'] = aggregated_data['escolaridad_x_pob'] / (aggregated_data['pob_total'] + epsilon_float)
    aggregated_data['porc_viv_con_internet'] = 100 * aggregated_data['viv_con_internet'] / (aggregated_data['viviendas_totales'] + epsilon_float)
    
    return aggregated_data


def assemble_final_gdf(units_gdf: gpd.GeoDataFrame, aggregated_data: pd.DataFrame) -> gpd.GeoDataFrame:
    """Ensambla el GeoDataFrame final uniendo geometrías con todos los datos agregados."""
    logging.info("Ensamblando el GeoDataFrame final.")

    # Se elimina la selección manual de columnas. Ahora se hace un merge de TODAS las columnas
    # del dataframe 'aggregated_data', asegurando que no se pierda ninguna característica.
    final_gdf = pd.merge(
        units_gdf,
        aggregated_data, # <--- Simplemente pasamos el DataFrame completo
        on='cve_unidad_territorial',
        how='left'
    )

    final_gdf.dropna(subset=['pob_total'], inplace=True) # Mantenemos esta limpieza por si acaso
    
    # Redondeamos solo las columnas que sabemos que existen y son numéricas
    cols_to_round = {
        'pob_total': 0,
        'escolaridad_promedio': 2,
        'porc_viv_con_internet': 2
    }
    for col, decimals in cols_to_round.items():
        if col in final_gdf.columns:
            final_gdf[col] = final_gdf[col].round(decimals)
    
    logging.info(f"GeoDataFrame final creado con {len(final_gdf)} Unidades Territoriales.")
    return final_gdf


def clean_and_standardize_denue(input_path: Path, output_path: Path, estrato_map: dict):
    """Aplica limpieza y estandarización avanzada al DENUE consolidado."""
    logging.info(f"Iniciando limpieza del DENUE desde '{input_path}'...")
    df = pd.read_parquet(input_path)
    
    # Filtrar coordenadas y cve_ageb nulos
    df = df[~((df['latitud'] == 0) & (df['longitud'] == 0))]
    df.dropna(subset=['cve_ageb'], inplace=True)
    
    # Estandarizar estrato de personal
    if 'personal_ocupado_estrato' in df.columns:
        source_col_norm = df['personal_ocupado_estrato'].str.lower()
        df['estrato_personal'] = source_col_norm.map(estrato_map)
    
    # Asegurar formato de cve_ageb
    df['cve_ageb'] = df['cve_ageb'].astype(str).str.split('.').str[0].str.zfill(4)
    
    final_cols = [
        'id_denue', 'timestamp', 'latitud', 'longitud',  
        'cve_ageb', 'codigo_postal', 'cve_scian', 'estrato_personal'
    ]
    # Mantener solo las columnas que existen en el dataframe
    existing_cols = [col for col in final_cols if col in df.columns]
    df_clean = df[existing_cols]
    
    logging.info(f"Guardando DENUE limpio en '{output_path}'...")
    df_clean.to_parquet(output_path, index=False)


def create_economic_features_at_ageb_level(agebs_gdf: gpd.GeoDataFrame, denue_df: pd.DataFrame, target_crs: str) -> gpd.GeoDataFrame:
    """Genera features económicos a nivel AGEB a partir de los datos del DENUE."""
    logging.info("Generando características económicas a nivel AGEB.")
    
    # Reproyectar GDF de AGEBs y calcular su área
    agebs_gdf_proj = agebs_gdf.to_crs(target_crs)
    agebs_gdf_proj['ageb_area_km2'] = agebs_gdf_proj.geometry.area / 1_000_000
    
    # Usar solo el snapshot más reciente del DENUE y convertirlo a GeoDataFrame
    denue_actual_df = denue_df[denue_df['timestamp'] == denue_df['timestamp'].max()]
    gdf_denue_actual = gpd.GeoDataFrame(
        denue_actual_df,
        geometry=gpd.points_from_xy(denue_actual_df.longitud, denue_actual_df.latitud, crs="EPSG:4326")
    ).to_crs(target_crs)

    # Se realiza la unión espacial. La primera corrección (tener 'cve_ageb' en agebs_gdf_proj) permite que esta línea se ejecute.
    gdf_joined = gpd.sjoin(gdf_denue_actual, agebs_gdf_proj[['cve_ageb', 'geometry']], how="inner", predicate='intersects')
    

    # Se manejan los sufijos '_left' y '_right' que 'sjoin' crea debido a la colisión de nombres en 'cve_ageb'.
    # Agrupamos por la clave del polígono ('cve_ageb_right'), que es la fuente geométrica autoritativa.
    logging.info("Agregando negocios por AGEB y calculando diversidad.")
    features_ageb = gdf_joined.groupby('cve_ageb_right').agg(
        num_negocios=('id_denue', 'count'),
        indice_diversidad=('cve_scian', 'nunique')
    )
    
    # Renombramos el índice del resultado para que vuelva a ser 'cve_ageb' y poder usarlo en el merge.
    features_ageb.index.name = 'cve_ageb'
    features_ageb = features_ageb.reset_index()
    
    # Unir los features calculados de vuelta al GeoDataFrame de AGEBs
    gdf_ageb_con_features = agebs_gdf_proj.merge(features_ageb, on='cve_ageb', how='left').fillna(0)
    
    # Calcular las features de densidad
    logging.info("Calculando densidades económicas.")
    area_segura = gdf_ageb_con_features['ageb_area_km2'] + 1e-9
    gdf_ageb_con_features['densidad_negocios'] = gdf_ageb_con_features['num_negocios'] / area_segura
    gdf_ageb_con_features['densidad_diversidad'] = gdf_ageb_con_features['indice_diversidad'] / area_segura
    
    return gdf_ageb_con_features

def create_security_features_at_ageb_level(agebs_gdf: gpd.GeoDataFrame, crime_df: pd.DataFrame, target_crs: str, epsilon: float) -> gpd.GeoDataFrame:
    """Calcula la tasa de delitos de alto impacto por km² a nivel de AGEB."""
    logging.info("Agregando delitos de alto impacto a nivel de AGEB.")

    # Se asegura de que epsilon sea numérico para evitar TypeErrors.
    # Esta es la corrección clave que resuelve el bug persistente.
    try:
        epsilon_float = float(epsilon)
    except (ValueError, TypeError):
        logging.warning(f"Valor de epsilon no válido. Usando 1e-9 por defecto.")
        epsilon_float = 1e-9

    agebs_gdf_proj = agebs_gdf.to_crs(target_crs)
    if 'ageb_area_km2' not in agebs_gdf_proj.columns:
        agebs_gdf_proj['ageb_area_km2'] = agebs_gdf_proj['geometry'].area / 1_000_000
    
    gdf_crimen = gpd.GeoDataFrame(
        crime_df, 
        geometry=gpd.points_from_xy(crime_df.longitud, crime_df.latitud, crs="EPSG:4326")
    ).to_crs(target_crs)

    agebs_base = agebs_gdf_proj[['cvegeo', 'ageb_area_km2', 'geometry']]
    gdf_ageb_crimen_joined = gpd.sjoin(gdf_crimen, agebs_base, how="inner", predicate='intersects')
    
    conteo_delitos_ageb = gdf_ageb_crimen_joined.groupby('cvegeo')['index_right'].count()
    
    gdf_ageb_con_seguridad = agebs_base.merge(
        conteo_delitos_ageb.rename('conteo_delitos_alto_impacto'), 
        on='cvegeo', 
        how='left'
    ).fillna({'conteo_delitos_alto_impacto': 0})
    
    gdf_ageb_con_seguridad['ageb_area_km2'] = pd.to_numeric(
        gdf_ageb_con_seguridad['ageb_area_km2'], errors='coerce'
    ).fillna(0)

    # Se usa la variable epsilon convertida a float
    gdf_ageb_con_seguridad['tasa_delitos_km2'] = (
        gdf_ageb_con_seguridad['conteo_delitos_alto_impacto'] / (gdf_ageb_con_seguridad['ageb_area_km2'] + epsilon_float)
    )
    
    return gdf_ageb_con_seguridad