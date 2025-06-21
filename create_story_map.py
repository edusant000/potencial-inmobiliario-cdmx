# /create_story_map.py

import yaml
import logging
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import folium
from sklearn.preprocessing import MinMaxScaler

# --- Configuración ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_story_map():
    """
    Crea un mapa interactivo con múltiples capas, escalas de color por deciles
    y tooltips funcionales para explorar los hallazgos clave del dataset final.
    """
    logging.info("--- Iniciando Creación de Mapa Interactivo v2.1 (Corregido) ---")

    # --- 1. Carga de Datos y Configuración ---
    config_path = Path("config/params.yaml")
    if not config_path.exists():
        logging.error("No se encontró el archivo de configuración. Abortando.")
        return

    with open(config_path, "r") as f:
        params = yaml.safe_load(f)
    
    dataset_path = Path(params['paths']['output']['primary_dataset'])
    if not dataset_path.exists():
        logging.error(f"No se encontró el dataset final. Ejecute 'main.py' primero.")
        return
        
    logging.info(f"Cargando dataset desde: {dataset_path}")
    gdf = gpd.read_file(dataset_path)

    # --- FIX 1: Cálculo del centro del mapa con .union_all() para eliminar el DeprecationWarning ---
    map_center_proj = gdf.geometry.union_all().centroid
    map_center_gdf = gpd.GeoSeries([map_center_proj], crs=gdf.crs).to_crs("EPSG:4326")
    map_center = [map_center_gdf.y.iloc[0], map_center_gdf.x.iloc[0]]

    gdf_web = gdf.to_crs("EPSG:4326")

    # --- 2. Feature Engineering y Formateo para Visualización ---
    logging.info("Creando 'Índice de Equilibrio' y formateando columnas para tooltip...")
    scaler = MinMaxScaler()
    gdf_web['densidad_negocios_norm'] = scaler.fit_transform(gdf_web[['densidad_negocios']])
    gdf_web['tasa_delitos_km2_norm'] = scaler.fit_transform(gdf_web[['tasa_delitos_km2']])
    gdf_web['indice_equilibrio'] = gdf_web['densidad_negocios_norm'] / (gdf_web['tasa_delitos_km2_norm'] + 0.1)

    # --- FIX 2: Pre-formatear las columnas para el tooltip a strings ---
    # Se crean nuevas columnas con el formato deseado para evitar el error de serialización JSON.
    gdf_web['pob_total_str'] = gdf_web['pob_total'].apply(lambda x: f"{x:,.0f}")
    gdf_web['escolaridad_promedio_str'] = gdf_web['escolaridad_promedio'].apply(lambda x: f"{x:,.1f}")
    gdf_web['num_negocios_str'] = gdf_web['num_negocios'].apply(lambda x: f"{x:,.0f}")
    gdf_web['densidad_negocios_str'] = gdf_web['densidad_negocios'].apply(lambda x: f"{x:,.1f}")
    gdf_web['tasa_delitos_km2_str'] = gdf_web['tasa_delitos_km2'].apply(lambda x: f"{x:,.1f}")
    gdf_web['indice_equilibrio_str'] = gdf_web['indice_equilibrio'].apply(lambda x: f"{x:,.2f}")
    
    # --- 3. Creación del Mapa Base ---
    m = folium.Map(location=map_center, zoom_start=11, tiles=None)
    folium.TileLayer('CartoDB positron', name='Mapa Base (Claro)').add_to(m)
    folium.TileLayer('OpenStreetMap', name='Mapa Base (Calles)').add_to(m)

    # --- 4. Creación de Capas de Datos ---
    layers_to_create = [
        {'col': 'indice_equilibrio', 'name': 'Índice Equilibrio (Negocios vs. Seguridad)', 'cmap': 'PiYG'},
        {'col': 'densidad_negocios', 'name': 'Densidad de Negocios', 'cmap': 'YlOrRd'},
        {'col': 'tasa_delitos_km2', 'name': 'Tasa de Delitos de Alto Impacto', 'cmap': 'Blues'},
        {'col': 'escolaridad_promedio', 'name': 'Escolaridad Promedio', 'cmap': 'viridis'}
    ]

    for layer_info in layers_to_create:
        column = layer_info['col']
        bins = sorted(list(set(gdf_web[column].quantile(np.linspace(0, 1, 11)))))
        
        choropleth = folium.Choropleth(
            geo_data=gdf_web,
            name=layer_info['name'],
            data=gdf_web,
            columns=['cve_unidad_territorial', column],
            key_on='feature.properties.cve_unidad_territorial',
            fill_color=layer_info['cmap'],
            bins=bins,
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name=layer_info['name'] + " (por Deciles)",
            highlight=True,
            show=(column == 'indice_equilibrio')
        ).add_to(m)
        
        # --- FIX 3: Usar las nuevas columnas formateadas en el tooltip ---
        tooltip_fields = {
            'nombre_unidad_territorial': 'Colonia:',
            'pob_total_str': 'Población:',
            'escolaridad_promedio_str': 'Escolaridad (años):',
            'num_negocios_str': '# Negocios:',
            'densidad_negocios_str': 'Densidad Negocios/km²:',
            'tasa_delitos_km2_str': 'Tasa Delitos/km²:',
            'indice_equilibrio_str': 'Índice Equilibrio:'
        }
        choropleth.geojson.add_child(
            folium.features.GeoJsonTooltip(
                fields=list(tooltip_fields.keys()),
                aliases=list(tooltip_fields.values()),
                sticky=True,
                style="background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"
            )
        )

    # --- 5. Finalización del Mapa ---
    folium.LayerControl(collapsed=False).add_to(m)
    output_path = "hallazgos_clave_mapa.html"
    m.save(output_path)
    logging.info(f"Mapa interactivo de hallazgos (v2.1) guardado exitosamente en: '{output_path}'")

if __name__ == '__main__':
    create_story_map()