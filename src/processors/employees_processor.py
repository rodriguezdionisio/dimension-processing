import pandas as pd
from utils.logger import get_logger
from utils import gcp_utils

logger = get_logger(__name__)

def process(df_users: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las transformaciones de limpieza y procesamiento
    específicas para la dimensión users.

    - Elimina los prefijos 'attributes.' y 'relationships.' de las columnas.
    - Renombra columnas clave.
    - Elimina columnas innecesarias.
    - Agrega la columna 'role_name' uniendo con la tabla de roles.

    Args:
        df_users (pd.DataFrame): DataFrame de empleados crudo.

    Returns:
        pd.DataFrame: DataFrame procesado y listo para ser guardado.
    """
    logger.info("Iniciando procesamiento de la dimensión de empleados...")


    latest_path = gcp_utils.find_latest_dimension_path("user_roles")
    df_roles_processed = gcp_utils.read_csv_from_gcs(latest_path + "/dim_user_roles.csv")
    logger.debug(f"Se obtuvieron los roles desde {latest_path}.")
    
    df_users_processed = df_users.copy()

    # --- Limpiar columnas de usuarios ---
    df_users_processed.columns = df_users_processed.columns.str.replace("attributes.", "", regex=False)
    df_users_processed.columns = df_users_processed.columns.str.replace("relationships.", "", regex=False)
    logger.debug("Prefijos 'attributes.' y 'relationships.' eliminados de columnas de usuarios.")

    # Renombrar columnas
    user_column_renames = {
        'id': 'employee_key',
        'role.data.id': 'role_key'
    }
    df_users_processed = df_users_processed.rename(columns=user_column_renames)
    logger.debug(f"Columnas de usuarios renombradas: {user_column_renames}")

    # Eliminar columnas innecesarias
    columns_to_delete = ['type', 'admin', 'promotionalCode', 'role.data.type']
    df_users_processed = df_users_processed.drop(columns=columns_to_delete, errors='ignore')
    logger.debug(f"Columnas eliminadas de usuarios: {columns_to_delete}")

    # --- Limpiar columnas de roles ---
    df_roles_processed.columns = df_roles_processed.columns.str.replace("attributes.", "", regex=False)
    role_column_renames = {
        'id': 'role_key',
        'name': 'role_name'
    }
    df_roles_processed = df_roles_processed.rename(columns=role_column_renames)
    logger.debug(f"Columnas de roles renombradas: {role_column_renames}")

    # --- Merge roles con usuarios ---
    df_roles_selection = df_roles_processed[['role_key', 'role_name']]
    df_final = pd.merge(df_users_processed, df_roles_selection, on='role_key', how='left')
    df_final = df_final.drop(columns=['role_key'])
    logger.debug("Merge entre usuarios y roles realizado. Columna 'role_key' eliminada.")

    logger.info("Procesamiento de empleados finalizado.")
    return df_final
