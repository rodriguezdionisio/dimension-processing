import os
from utils import gcp_utils
from utils.env_config import config
from utils.logger import get_logger

from src.processors import customers_processor, employees_processor, tables_processor

logger = get_logger(__name__)

# --------------------------------------------------------------------------------
# 1. CENTRALIZACIÓN DE TAREAS
#    Aquí definimos cada tarea de procesamiento.
#    Mapeamos el nombre de la dimensión a la función de procesamiento específica.
# --------------------------------------------------------------------------------
PROCESSING_TASKS = [
    {"name": "customers", "processor_func": customers_processor.process},
    {"name": "users", "processor_func": employees_processor.process},
    {"name": "tables", "processor_func": tables_processor.process}
    ]

# --------------------------------------------------------------------------------
# 2. LÓGICA REUTILIZABLE
#    Esta función encapsula la lógica para procesar UNA dimensión.
# --------------------------------------------------------------------------------
def run_processing_task(dimension_name: str, process_function):
    """
    Ejecuta el pipeline completo (lectura, procesamiento, escritura) para una dimensión.
    """
    logger.info(f"Iniciando procesamiento para la dimensión: '{dimension_name}'")
    try:
        # Paso 1: Encontrar el archivo más reciente en la zona raw
        latest_path = gcp_utils.find_latest_dimension_path(dimension_name)
        date_partition = [part for part in latest_path.split('/') if 'date=' in part][0]
        logger.info(f"Última versión encontrada: {latest_path}")

        # Paso 2: Leer los datos CSV desde GCS
        raw_df = gcp_utils.read_csv_from_gcs(latest_path + "/dim_" + dimension_name + ".csv")
        
        # Paso 3: Procesar los datos usando la función específica
        logger.info(f"Aplicando la función de procesamiento: {process_function.__module__}")
        clean_df = process_function(raw_df)

        # Paso 4: Guardar los datos procesados en formato Parquet
        destination_path = f"gs://{config.GCS_BUCKET_NAME}/clean/dim_{dimension_name}/{date_partition}/dim_{dimension_name}.parquet"
        gcp_utils.write_parquet_to_gcs(clean_df, destination_path)
        
        logger.info(f"Dimensión '{dimension_name}' procesada y guardada exitosamente en {destination_path}.")
        return True

    except Exception as e:
        logger.error(f"ERROR al procesar la dimensión '{dimension_name}': {e}", exc_info=True)
        return False

# --------------------------------------------------------------------------------
# 3. ORQUESTADOR PRINCIPAL
#    Itera sobre la lista de tareas y ejecuta cada una.
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("--- INICIANDO PIPELINE DE PROCESAMIENTO DE DIMENSIONES ---")
    
    success_count = 0
    failure_count = 0

    for task in PROCESSING_TASKS:
        # Desempaquetamos los parámetros de la tarea
        dim_name = task["name"]
        processor = task["processor_func"]
        
        # Ejecutamos la función reutilizable
        if run_processing_task(dim_name, processor):
            success_count += 1
        else:
            failure_count += 1

    logger.info("--- PIPELINE FINALIZADO ---")
    logger.info(f"Resumen: {success_count} tareas exitosas, {failure_count} tareas fallidas.")
