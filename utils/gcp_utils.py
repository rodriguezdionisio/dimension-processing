from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import gcsfs
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

# Cliente singleton
_storage_client = None

def get_storage_client():
    """Inicializa (una vez) y devuelve el cliente de Google Cloud Storage."""
    global _storage_client
    if _storage_client:
        return _storage_client

    try:
        if config.GOOGLE_APPLICATION_CREDENTIALS:
            credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
            _storage_client = storage.Client(credentials=credentials, project=config.GCP_PROJECT_NAME)
            logger.info("Cliente de Storage inicializado con archivo de credenciales.")
        else:
            _storage_client = storage.Client(project=config.GCP_PROJECT_NAME)
            logger.info("Cliente de Storage inicializado con Application Default Credentials (ADC).")
    except Exception as e:
        logger.error(f"Error al inicializar cliente de GCS: {e}")
        _storage_client = None

    return _storage_client

def get_gcsfs():
    """
    Retorna una instancia de GCSFileSystem, usando el archivo de credenciales si está definido.
    """
    if config.GOOGLE_APPLICATION_CREDENTIALS:
        return gcsfs.GCSFileSystem(token=config.GOOGLE_APPLICATION_CREDENTIALS)
    return gcsfs.GCSFileSystem()

def list_gcs_files(bucket: str, prefix: str) -> list[str]:
    """
    Lista archivos en GCS con un prefijo determinado usando gcsfs.

    Args:
        bucket (str): Nombre del bucket (sin 'gs://').
        prefix (str): Prefijo de los archivos a listar.

    Returns:
        list[str]: Lista de rutas de archivos en GCS que coinciden con el prefijo.
    """
    fs = get_gcsfs()
    path = f"gs://{bucket}/{prefix}"
    try:
        return fs.ls(path)
    except Exception as e:
        logger.error(f"gcs_utils: Error al listar archivos en {path}: {e}")
        return []
    
def find_latest_dimension_path(dimension_name: str) -> str:
    """
    Encuentra la ruta con la fecha más reciente dentro de la carpeta raw de una dimensión.

    Args:
        dimension_name (str): Nombre de la dimensión (ej. 'customer').

    Returns:
        str: Ruta completa al archivo más reciente.
    """
    prefix = f"raw/dim_{dimension_name}/"
    files = list_gcs_files(config.GCS_BUCKET_NAME, prefix)

    # Filtrar solo paths con date partition
    dated_paths = [f for f in files if 'date=' in f]

    if not dated_paths:
        raise FileNotFoundError(f"No se encontraron archivos con particiones de fecha para {dimension_name}")

    # Extraer fecha y comparar
    def extract_date(path):
        try:
            date_str = [part for part in path.split('/') if part.startswith('date=')][0].replace('date=', '')
            return datetime.strptime(date_str, '%Y-%m-%d')
        except Exception:
            return datetime.min

    latest_path = max(dated_paths, key=extract_date)
    return latest_path

def read_csv_from_gcs(path: str) -> pd.DataFrame:
    """
    Lee un archivo CSV desde una ruta completa de GCS y devuelve un DataFrame.

    Args:
        path (str): Ruta GCS completa (ej. 'gs://bucket/raw/dim_customer/date=2024-06-01/data.csv').

    Returns:
        pd.DataFrame: DataFrame con los datos del archivo.
    """
    fs = get_gcsfs()
    try:
        with fs.open(path, 'r') as f:
            df = pd.read_csv(f)
            logger.info(f"CSV leído exitosamente desde {path} con {len(df)} registros.")
            return df
    except Exception as e:
        logger.error(f"Error al leer CSV desde GCS: {path} - {e}", exc_info=True)
        return pd.DataFrame()

def write_parquet_to_gcs(df: pd.DataFrame, destination_path: str):
    """
    Escribe un DataFrame como archivo Parquet en GCS.

    Args:
        df (pd.DataFrame): DataFrame a guardar.
        destination_path (str): Ruta GCS de destino (ej. 'gs://bucket/clean/dim_customer/date=2024-06-01/data.parquet').
    """
    fs = get_gcsfs()
    try:
        with fs.open(destination_path, 'wb') as f:
            df.to_parquet(f, index=False)
            logger.info(f"Archivo Parquet guardado exitosamente en {destination_path}")
    except Exception as e:
        logger.error(f"Error al escribir Parquet en {destination_path}: {e}", exc_info=True)
        raise
