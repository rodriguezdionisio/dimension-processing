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
    new_column_names = {
        'id': 'customer_key',
        'createdAt': 'created_date',
        'discountPercentage': 'discount_percentage',
        'houseAccountBalance': 'house_account_balance',
        'houseAccountEnabled': 'house_account_enabled'
    }
    df_processed = df_processed.rename(columns=new_column_names)
    logger.debug(f"Columnas renombradas: {new_column_names}")

    # 3. Forzar tipos de datos
    if 'customer_key' in df_processed.columns:
        df_processed['customer_key'] = pd.to_numeric(df_processed['customer_key'], errors='coerce').astype('Int64')
        logger.debug("Columna 'customer_key' convertida a int")
    
    if 'created_date' in df_processed.columns:
        df_processed['created_date'] = pd.to_datetime(df_processed['created_date'], utc=True).dt.tz_convert('America/Argentina/Buenos_Aires')
        logger.debug("Columna 'created_date' convertida a datetime con zona horaria de Argentina")
        
        # Extraer fecha en formato YYYYMMDD (manejar valores nulos)
        df_processed['created_date_key'] = df_processed['created_date'].dt.strftime('%Y%m%d')
        df_processed['created_date_key'] = pd.to_numeric(df_processed['created_date_key'], errors='coerce').astype('Int64')
        logger.debug("Columna 'created_date_key' extraída en formato YYYYMMDD como int")
        
        # Extraer hora como minutos desde medianoche (00:00 = 1, 00:10 = 10, 01:00 = 60)
        time_minutes = df_processed['created_date'].dt.hour * 60 + df_processed['created_date'].dt.minute
        df_processed['created_time_key'] = time_minutes.replace(0, 1).astype('Int64')
        logger.debug("Columna 'created_time_key' extraída como minutos desde medianoche como int")
        
        # Eliminar la columna created_date original
        df_processed = df_processed.drop(columns=['created_date', 'paymentMethod.data'])
        logger.debug("Columna 'created_date' y 'paymentMethod.data' eliminadas del DataFrame")
    
    if 'discount_percentage' in df_processed.columns:
        df_processed['discount_percentage'] = pd.to_numeric(df_processed['discount_percentage'], errors='coerce')
        logger.debug("Columna 'discount_percentage' convertida a numeric")
    
    if 'house_account_balance' in df_processed.columns:
        df_processed['house_account_balance'] = pd.to_numeric(df_processed['house_account_balance'], errors='coerce')
        logger.debug("Columna 'house_account_balance' convertida a numeric")
    
    if 'active' in df_processed.columns:
        df_processed['active'] = df_processed['active'].astype('boolean')
        logger.debug("Columna 'active' convertida a boolean")
    
    if 'house_account_enabled' in df_processed.columns:
        df_processed['house_account_enabled'] = df_processed['house_account_enabled'].astype('boolean')
        logger.debug("Columna 'house_account_enabled' convertida a boolean")
    
    # Convertir el resto de columnas a string (excluyendo las ya procesadas)
    processed_columns = {'customer_key', 'created_date_key', 'created_time_key', 'discount_percentage', 'house_account_balance', 'active', 'house_account_enabled'}
    for col in df_processed.columns:
        if col not in processed_columns:
            df_processed[col] = df_processed[col].astype('string')
    logger.debug("Resto de columnas convertidas a string")

    logger.info("Procesamiento de clientes finalizado.")
    
    return df_processed