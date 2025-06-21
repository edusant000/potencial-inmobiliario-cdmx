# /src/data/make_dataset.py

import pandas as pd
import geopandas as gpd
from pathlib import Path
import logging
import re
import zipfile
import tempfile
from unidecode import unidecode
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_ageb_polygons(path: Path, target_crs: str) -> gpd.GeoDataFrame:
    """Carga los polígonos de AGEB, crea claves únicas y calcula el área."""
    logging.info(f"Cargando polígonos de AGEB desde: {path}")
    gdf = gpd.read_file(path)
    
    logging.info("Creando clave única 'cvegeo' de 15 dígitos.")
    gdf['cvegeo'] = (
        gdf['CVE_ENT'].astype(str).str.zfill(2) +
        gdf['CVE_MUN'].astype(str).str.zfill(3) +
        gdf['CVE_LOC'].astype(str).str.zfill(4) +
        gdf['CVE_AGEB'].astype(str).str.zfill(4)
    )

    # --- INICIO DE LA CORRECCIÓN ---
    #
    # Se añade la creación explícita de 'cve_ageb' (4 dígitos) para asegurar
    # su disponibilidad en uniones posteriores, como con el DENUE.
    #
    logging.info("Creando clave 'cve_ageb' de 4 dígitos.")
    gdf['cve_ageb'] = gdf['CVE_AGEB'].astype(str).str.zfill(4)
    #
    # --- FIN DE LA CORRECCIÓN ---
    
    logging.info(f"Proyectando a CRS: {target_crs} para cálculo de área.")
    gdf = gdf.to_crs(target_crs)
    gdf['ageb_area_total'] = gdf.geometry.area
    
    # Se añade 'cve_ageb' a las columnas de salida.
    return gdf[['cvegeo', 'cve_ageb', 'ageb_area_total', 'geometry']]

def load_and_process_census_data(path: Path, var_map: dict, num_cols: list, agg_map: dict) -> pd.DataFrame:
    """Carga y procesa los datos del censo, agregando por AGEB desde el nivel de manzana."""
    logging.info(f"Cargando datos del censo desde: {path}")
    try:
        df = pd.read_csv(path)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding='latin-1')
        
    logging.info("Filtrando para usar solo datos de manzanas (MZA != '000').")
    df['MZA'] = df['MZA'].astype(str).str.zfill(3)
    df_manzanas = df[df['MZA'] != '000'].copy()

    logging.info("Creando clave 'cvegeo' a nivel de AGEB.")
    df_manzanas['cvegeo'] = (
        df_manzanas['ENTIDAD'].astype(str).str.zfill(2) +
        df_manzanas['MUN'].astype(str).str.zfill(3) +
        df_manzanas['LOC'].astype(str).str.zfill(4) +
        df_manzanas['AGEB'].astype(str).str.zfill(4)
    )

    logging.info("Seleccionando, renombrando y limpiando variables de interés.")
    df_seleccion = df_manzanas[list(var_map.keys())].rename(columns=var_map)
    for col in num_cols:
        df_seleccion[col] = pd.to_numeric(df_seleccion[col], errors='coerce')

    logging.info("Agrupando manzanas para calcular totales por AGEB.")
    df_agrupado = df_seleccion.groupby('cvegeo').agg(agg_map).reset_index()
    
    return df_agrupado

def load_territorial_units(path: Path, target_crs: str) -> gpd.GeoDataFrame:
    """Carga la capa de Unidades Territoriales y la prepara."""
    logging.info(f"Cargando Unidades Territoriales desde: {path}")
    gdf = gpd.read_file(path)
    gdf.rename(columns={'CVEUT': 'cve_unidad_territorial', 'NOMUT': 'nombre_unidad_territorial'}, inplace=True)
    gdf = gdf.to_crs(target_crs)
    return gdf


def _parse_date_from_filename(filepath: Path, exception_map: dict) -> str | None:
    """Extrae la fecha (YYYY-MM) del nombre de archivo."""
    stem = filepath.stem
    key = re.sub(r'\s*\(\d+\)$', '', stem)

    if key in exception_map:
        return exception_map[key]
    
    match = re.search(r'(\d{2})(\d{2})', key)
    if match:
        month, year_short = match.groups()
        if 1 <= int(month) <= 12 and 15 <= int(year_short) <= 99:
            return f'20{year_short}-{month}'

    match = re.search(r'(\d{4})', key)
    if match:
        year = match.group(1)
        if 2010 <= int(year) <= 2025:
            return f'{year}-01'
    
    logging.warning(f"No se pudo determinar una fecha para el archivo: {filepath.name}.")
    return None

def _standardize_columns(df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """Normaliza los nombres de las columnas usando unidecode y el mapeo."""
    df.columns = [unidecode(col.lower().strip().lstrip('\ufeff')) for col in df.columns]
    rename_dict = {}
    for standard_name, possible_names in column_mapping.items():
        normalized_variants = [unidecode(p.lower().strip()) for p in possible_names]
        for col in df.columns:
            if col in normalized_variants:
                rename_dict[col] = standard_name
                break
    return df.rename(columns=rename_dict)

def process_historical_denue_zips(zip_dir: Path, column_mapping: dict, exception_map: dict, output_path: Path):
    """Procesa archivos ZIP del DENUE, los consolida y guarda como Parquet."""
    logging.info("Iniciando consolidación de datos históricos del DENUE.")
    if not zip_dir.exists():
        logging.error(f"El directorio de ZIPs '{zip_dir}' no existe.")
        return

    zip_files = sorted(list(zip_dir.glob('denue_*.zip')))
    if not zip_files:
        logging.warning(f"No se encontraron archivos .zip en '{zip_dir}'.")
        return

    all_denue_dfs = []
    for zip_path in tqdm(zip_files, desc="Procesando archivos ZIP del DENUE"):
        date_key = _parse_date_from_filename(zip_path, exception_map)
        if not date_key:
            continue

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                csv_candidates = list(Path(temp_dir).rglob('*.csv'))
                if not csv_candidates:
                    continue

                main_csv_path = max(csv_candidates, key=lambda p: p.stat().st_size)
                df = pd.read_csv(main_csv_path, encoding='latin1', low_memory=False, dtype=str)
                df = _standardize_columns(df, column_mapping)
                df['timestamp'] = date_key
                
                cols_to_keep = list(column_mapping.keys())
                existing_cols = [col for col in cols_to_keep if col in df.columns]
                all_denue_dfs.append(df[existing_cols + ['timestamp']])
        except Exception as e:
            logging.error(f"Error procesando {zip_path.name}: {e}", exc_info=False)

    if not all_denue_dfs:
        logging.info("No se procesaron datos del DENUE.")
        return

    logging.info("Concatenando y limpiando DataFrames del DENUE...")
    denue_historico_df = pd.concat(all_denue_dfs, ignore_index=True)
    
    # Limpieza final y conversión de tipos
    denue_historico_df['timestamp'] = pd.to_datetime(denue_historico_df['timestamp'], errors='coerce')
    for col in ['latitud', 'longitud']:
        if col in denue_historico_df.columns:
            denue_historico_df[col] = pd.to_numeric(denue_historico_df[col], errors='coerce')
    
    denue_historico_df.dropna(subset=['timestamp', 'latitud', 'longitud'], inplace=True)
    denue_historico_df = denue_historico_df[
        (denue_historico_df['latitud'].between(-90, 90)) &
        (denue_historico_df['longitud'].between(-180, 180))
    ]

    logging.info(f"Guardando DENUE histórico consolidado en '{output_path}'...")
    denue_historico_df.to_parquet(output_path, index=False)

def load_and_filter_crime_data(path: Path, high_impact_categories: list) -> pd.DataFrame:
    """Carga los datos de crimen, seleccionando columnas clave y filtrando por alto impacto."""
    logging.info(f"Cargando datos de crimen desde: {path}")
    try:
        df_crimen_raw = pd.read_csv(
            path, 
            usecols=['categoria_delito', 'latitud', 'longitud'],
            dtype={'categoria_delito': 'category', 'latitud': 'float64', 'longitud': 'float64'}
        )
        df_crimen_filtrado = df_crimen_raw[df_crimen_raw['categoria_delito'].isin(high_impact_categories)].copy()
        df_crimen_filtrado.dropna(subset=['latitud', 'longitud'], inplace=True)
        logging.info(f"Se conservaron {len(df_crimen_filtrado):,} registros de delitos de alto impacto.")
        return df_crimen_filtrado
    except Exception as e:
        logging.exception(f"No se pudo leer o procesar el archivo de crimen en '{path}'. Error: {e}")
        # Devolvemos un DataFrame vacío para no romper el pipeline
        return pd.DataFrame(columns=['categoria_delito', 'latitud', 'longitud'])