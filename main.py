
import yaml
from pathlib import Path
import logging
import pandas as pd

from src.data.make_dataset import (
    load_ageb_polygons,
    load_and_process_census_data,
    load_territorial_units,
    process_historical_denue_zips,
    load_and_filter_crime_data
)
from src.features.build_features import (
    merge_agebs_with_census,
    perform_areal_interpolation,
    aggregate_to_territorial_units,
    assemble_final_gdf,
    clean_and_standardize_denue,
    create_economic_features_at_ageb_level,
    create_security_features_at_ageb_level
)
from src.visualization.visualize import create_interactive_map

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Orquesta el pipeline completo: Censo, DENUE, Seguridad, unión, interpolación y guardado."""
    logging.info("--- INICIANDO PIPELINE DE DATOS COMPLETO (CON SEGURIDAD) ---")
    
    with open("config/params.yaml", "r") as f:
        params = yaml.safe_load(f)

    # --- FASE 1: PROCESAMIENTO DE DATOS BASE (CENSO Y GEOMETRÍAS) ---
    logging.info("--- FASE 1: Procesando Censo y Geometrías ---")
    agebs_gdf = load_ageb_polygons(Path(params['paths']['input']['agebs']), params['crs_proyectado'])
    censo_df = load_and_process_census_data(
        Path(params['paths']['input']['censo']),
        params['census_variables']['raw_mapping'],
        params['census_variables']['numerical_cols'],
        params['census_variables']['aggregation']
    )
    units_gdf = load_territorial_units(Path(params['paths']['input']['unidades_territoriales']), params['crs_proyectado'])
    agebs_con_censo_gdf = merge_agebs_with_census(agebs_gdf, censo_df)

    # --- FASE 2: PROCESAMIENTO DE DATOS ECONÓMICOS (DENUE) ---
    logging.info("--- FASE 2: Procesando Datos Históricos del DENUE ---")
    denue_params = params['denue']
    process_historical_denue_zips(
        Path(denue_params['paths']['zip_directory']),
        denue_params['column_mapping'],
        denue_params['filename_date_exceptions'],
        Path(denue_params['paths']['historical_consolidated'])
    )

    # --- INICIO DE LA CORRECCIÓN ---
    #
    # La llamada a la función ahora incluye los 3 argumentos requeridos en el orden correcto:
    # 1. Ruta del archivo de entrada (historical_consolidated)
    # 2. Ruta del archivo de salida (cleaned_output)
    # 3. El diccionario de mapeo (estrato_map)
    #
    clean_and_standardize_denue(
        input_path=Path(denue_params['paths']['historical_consolidated']),
        output_path=Path(denue_params['paths']['cleaned_output']),
        estrato_map=denue_params['estrato_map']
    )
    #
    # --- FIN DE LA CORRECCIÓN ---
    
    # --- FASE 3: PROCESAMIENTO DE DATOS DE SEGURIDAD ---
    logging.info("--- FASE 3: Procesando Datos de Seguridad ---")
    security_params = params['security']
    crime_df = load_and_filter_crime_data(
        Path(security_params['paths']['crime_csv']),
        security_params['high_impact_categories']
    )
    
    # --- FASE 4: GENERACIÓN Y UNIFICACIÓN DE FEATURES A NIVEL AGEB ---
    logging.info("--- FASE 4: Creando y Uniendo Todos los Features a Nivel AGEB ---")
    denue_limpio_df = pd.read_parquet(denue_params['paths']['cleaned_output'])
    
    agebs_con_features_economicos_gdf = create_economic_features_at_ageb_level(agebs_gdf, denue_limpio_df, params['crs_proyectado'])
    agebs_con_features_seguridad_gdf = create_security_features_at_ageb_level(agebs_gdf, crime_df, params['crs_proyectado'], params['epsilon'])

    agebs_intermedio_gdf = agebs_con_censo_gdf.merge(
        agebs_con_features_economicos_gdf[['cvegeo', 'num_negocios', 'indice_diversidad', 'densidad_negocios', 'densidad_diversidad']],
        on='cvegeo', how='left'
    )
    agebs_final_gdf = agebs_intermedio_gdf.merge(
        agebs_con_features_seguridad_gdf[['cvegeo', 'tasa_delitos_km2']],
        on='cvegeo', how='left'
    ).fillna(0)

    # --- FASE 5: INTERPOLACIÓN Y ENSAMBLAJE FINAL ---
    logging.info("--- FASE 5: Interpolando Todos los Features a Unidades Territoriales ---")
    intersection_gdf = perform_areal_interpolation(units_gdf, agebs_final_gdf)
    aggregated_data = aggregate_to_territorial_units(intersection_gdf, params['epsilon'])
    final_gdf = assemble_final_gdf(units_gdf, aggregated_data)

    # --- FASE 6: GUARDADO Y VALIDACIÓN ---
    final_output_path = Path(params['paths']['output']['primary_dataset'])
    final_gdf.to_file(final_output_path, driver='GPKG')
    logging.info(f"Dataset final guardado en: {final_output_path}")

    poblacion_calculada = final_gdf['pob_total'].sum()
    logging.info(f"Verificación final: Población Total Calculada en el dataset: {int(poblacion_calculada):,}")
    
    # --- FASE 7: VISUALIZACIÓN ---
    logging.info("--- FASE 7: Generando Visualización Interactiva Final ---")
    create_interactive_map(final_gdf, Path(params['paths']['output']['interactive_map']), params['crs_geografico'])
    
    logging.info("--- PIPELINE COMPLETADO EXITOSAMENTE ---")

if __name__ == "__main__":
    main()