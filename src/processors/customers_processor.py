import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

def process(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las transformaciones de limpieza y procesamiento
    específicas para la dimensión customer.

    - Elimina los prefijos 'attributes.' y 'relationships.' de las columnas.
    - Renombra la columna 'id' a 'customer_key'.

    Args:
        df (pd.DataFrame): El DataFrame de clientes leído desde la zona raw.

    Returns:
        pd.DataFrame: El DataFrame procesado y listo para ser guardado.
    """
    logger.info("Iniciando procesamiento de la dimensión de clientes...")
    
    # Copio el DataFrame para evitar advertencias de SettingWithCopyWarning
    df_processed = df.copy()

    # 1. Eliminar prefijos de los nombres de las columnas
    df_processed.columns = df_processed.columns.str.replace("attributes.", "", regex=False)
    df_processed.columns = df_processed.columns.str.replace("relationships.", "", regex=False)
    logger.debug("Prefijos 'attributes.' y 'relationships.' eliminados de las columnas.")

    # 2. Renombrar columnas
    new_column_names = {'id': 'customer_key'}
    df_processed = df_processed.rename(columns=new_column_names)
    logger.debug(f"Columnas renombradas: {new_column_names}")
    
    logger.info("Procesamiento de clientes finalizado.")
    
    return df_processed