import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

def process(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las transformaciones de limpieza y procesamiento
    específicas para la dimensión table.

    - Elimina los prefijos 'attributes.' y 'relationships.' de las columnas.
    - Renombra columnas específicas.
    - Elimina columnas innecesarias.
    - Traduce valores de las columnas 'table_size', 'table_location' y 'table_shape'.

    Args:
        df (pd.DataFrame): El DataFrame de mesas leído desde la zona raw.

    Returns:
        pd.DataFrame: El DataFrame procesado y listo para ser guardado.
    """
    logger.info("Iniciando procesamiento de la dimensión de mesas...")

    # Copia del DataFrame para evitar modificar el original
    df_processed = df.copy()

    # 1. Eliminar prefijos
    df_processed.columns = df_processed.columns.str.replace("attributes.", "table_", regex=False)
    df_processed.columns = df_processed.columns.str.replace("relationships.", "", regex=False)
    logger.debug("Prefijos 'attributes.' y 'relationships.' eliminados de las columnas.")

    # 2. Renombrar columnas
    new_column_names = {
        'id': 'table_key',
        'room.data.id': 'table_location'
    }
    df_processed = df_processed.rename(columns=new_column_names)
    logger.debug(f"Columnas renombradas: {new_column_names}")

    # 3. Eliminar columnas innecesarias
    columns_to_delete = ['type', 'room.data.type']
    df_processed = df_processed.drop(columns=columns_to_delete, errors='ignore')
    logger.debug(f"Columnas eliminadas: {columns_to_delete}")

    # 4. Traducción de valores
    df_processed["table_size"] = df_processed["table_size"].replace({"s": "chica", "l": "grande"})
    df_processed["table_location"] = df_processed["table_location"].replace({1: "salon", 2: "vereda"})
    df_processed["table_shape"] = df_processed["table_shape"].replace({0: "cuadrada", 1: "redonda"})
    logger.debug("Valores traducidos en columnas categóricas.")

    logger.info("Procesamiento de mesas finalizado.")
    return df_processed
