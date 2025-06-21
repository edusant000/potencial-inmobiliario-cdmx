# /src/visualization/visualize.py

import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from folium.plugins import Fullscreen
from pathlib import Path
import logging

def create_interactive_map(gdf: gpd.GeoDataFrame, output_path: Path, geo_crs: str):
    """Crea un mapa interactivo de Folium con múltiples capas y lo guarda como HTML."""
    logging.info("Creando mapa interactivo con Folium...")
    gdf_mapa = gdf.to_crs(geo_crs)
    gdf_mapa['cve_unidad_territorial'] = gdf_mapa['cve_unidad_territorial'].astype(str)
    
    m = folium.Map(location=[19.4326, -99.1332], zoom_start=10.5, tiles=None)
    
    # Capas de fondo
    folium.TileLayer('cartodbpositron', name='Mapa Base (Claro)').add_to(m)
    folium.TileLayer('OpenStreetMap', name='Mapa Base (Calles)').add_to(m)
    
    # Capas de datos (Choropleth)
    for col, name, legend, color in [
        ('escolaridad_promedio', 'Escolaridad Promedio', 'Escolaridad (Años)', 'YlGnBu'),
        ('porc_viv_con_internet', '% Viviendas con Internet', '% Viv. con Internet', 'YlOrRd'),
        ('pob_total', 'Población Total', 'Población (Est.)', 'PuBu')
    ]:
        choropleth = folium.Choropleth(
            geo_data=gdf_mapa,
            name=name,
            data=gdf_mapa,
            columns=['cve_unidad_territorial', col],
            key_on='feature.properties.cve_unidad_territorial',
            fill_color=color,
            fill_opacity=0.7,
            line_opacity=0.3,
            legend_name=legend,
            show=(col == 'escolaridad_promedio'), # Solo la primera es visible por defecto
            highlight=True,
            overlay=True
        ).add_to(m)
        
        choropleth.geojson.add_child(
            folium.features.GeoJsonTooltip(
                fields=['nombre_unidad_territorial', col],
                aliases=['Colonia:', f'{legend}:'],
                sticky=True
            )
        )

    # Controles finales
    Fullscreen().add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    
    m.save(str(output_path))
    logging.info(f"Mapa interactivo guardado exitosamente en: {output_path}")