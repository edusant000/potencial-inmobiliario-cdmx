# /query_stats.py

import yaml
import logging
import argparse
from pathlib import Path
import geopandas as gpd

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_params_and_path():
    """Carga los parámetros y la ruta al dataset final."""
    config_path = Path("config/params.yaml")
    if not config_path.exists():
        logging.error(f"No se encontró el archivo de configuración en '{config_path}'. Abortando.")
        return None, None

    with open(config_path, "r") as f:
        params = yaml.safe_load(f)
    
    dataset_path = Path(params['paths']['output']['primary_dataset'])
    if not dataset_path.exists():
        logging.error(f"No se encontró el dataset final en '{dataset_path}'.")
        logging.error("Por favor, ejecute 'python main.py' primero para generarlo.")
        return None, None
        
    return params, dataset_path

def query_population_stats():
    """Carga el dataset final y calcula estadísticas de población."""
    logging.info("--- Ejecutando consulta de POBLACIÓN ---")
    params, dataset_path = get_params_and_path()
    if not dataset_path:
        return

    final_gdf = gpd.read_file(dataset_path)

    if 'pob_total' not in final_gdf.columns:
        logging.error("La columna 'pob_total' no se encontró en el dataset final.")
        return

    poblacion_calculada = final_gdf['pob_total'].sum()
    POBLACION_OFICIAL_CDMX_2020 = 9209944
    
    diferencia_abs = poblacion_calculada - POBLACION_OFICIAL_CDMX_2020
    diferencia_porcentual = 100 * diferencia_abs / POBLACION_OFICIAL_CDMX_2020

    print("\n" + "="*50)
    print("      VERIFICACIÓN DE POBLACIÓN TOTAL CALCULADA")
    print("="*50)
    print(f"Población Total (dataset final):   {int(poblacion_calculada):,}")
    print(f"Población Oficial (Censo 2020):    {POBLACION_OFICIAL_CDMX_2020:,}")
    print(f"Diferencia Porcentual:             {diferencia_porcentual:.2f}%")
    print("="*50 + "\n")

def query_denue_stats():
    """Carga el dataset final y calcula estadísticas del DENUE."""
    logging.info("--- Ejecutando consulta de DENUE ---")
    params, dataset_path = get_params_and_path()
    if not dataset_path:
        return

    final_gdf = gpd.read_file(dataset_path)
    
    required_cols = ['num_negocios', 'indice_diversidad', 'densidad_negocios', 'nombre_unidad_territorial']
    if not all(col in final_gdf.columns for col in required_cols):
        logging.error(f"Faltan columnas del DENUE en el dataset final. Columnas requeridas: {required_cols}")
        return

    total_negocios = final_gdf['num_negocios'].sum()
    avg_diversidad = final_gdf['indice_diversidad'].mean()
    top_5_colonias = final_gdf.sort_values('num_negocios', ascending=False).head(5)

    print("\n" + "="*60)
    print("      RESUMEN DE CARACTERÍSTICAS ECONÓMICAS (DENUE)")
    print("="*60)
    print(f"Total de Negocios (estimado en Unidades Territoriales): {int(total_negocios):,}")
    print(f"Promedio de Diversidad Comercial por Unidad Territorial: {avg_diversidad:.1f} (tipos de negocio únicos)")
    print("\n--- Top 5 Unidades Territoriales por # de Negocios ---")
    print(top_5_colonias[['nombre_unidad_territorial', 'num_negocios']].round(0).to_string(index=False))
    print("="*60 + "\n")


if __name__ == "__main__":
    # --- Configuración del argparse para una herramienta de consola flexible ---
    parser = argparse.ArgumentParser(
        description="Herramienta de consulta para estadísticas del dataset inmobiliario final.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "query_type",
        nargs='?', # El argumento es opcional
        default="population", # Valor por defecto si no se especifica
        choices=["population", "denue"],
        help=(
            "Especifica el tipo de consulta a realizar:\n"
            " 'population' - Muestra el resumen de población total (default).\n"
            " 'denue' - Muestra el resumen de características del DENUE."
        )
    )
    args = parser.parse_args()

    # Ejecutar la función correspondiente según el argumento
    if args.query_type == "population":
        query_population_stats()
    elif args.query_type == "denue":
        query_denue_stats()