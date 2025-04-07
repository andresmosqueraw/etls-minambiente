import os
import shutil
import re
import json
import logging
from typing import Dict

def leer_configuracion(cfg) -> Dict:
    """
    Lee la configuración desde un archivo JSON.

    Returns:
        Un diccionario con la configuración leída.

    Raises:
        Exception: Si ocurre algún error al leer o parsear el archivo de configuración.
    """
    try:
        CONFIG_PATH = cfg["CONFIG_PATH"]
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
        logging.info("Configuración cargada correctamente.")
        return config
    except FileNotFoundError as e:
        logging.exception("Archivo no encontrado.")
        raise FileNotFoundError(f"Archivo no encontrado: {e}") from e
    except Exception as e:
        logging.exception("Error leyendo la configuración.")
        raise Exception(f"Error leyendo la configuración: {e}") from e

def limpiar_carpeta_temporal(cfg) -> None:
    """
    Limpia la carpeta temporal definida en TEMP_FOLDER:
      - Elimina archivos y subcarpetas existentes.
      - Crea la carpeta si no existe.

    Raises:
        Exception: Si ocurre algún problema al eliminar archivos o directorios.
    """
    TEMP_FOLDER = cfg["TEMP_FOLDER"]
    logging.info("Iniciando limpieza de la carpeta temporal...")
    if os.path.exists(TEMP_FOLDER):
        for item in os.listdir(TEMP_FOLDER):
            item_path = os.path.join(TEMP_FOLDER, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            except Exception as e:
                logging.exception(f"Error eliminando {item_path}.")
                raise Exception(f"Error eliminando {item_path}: {e}") from e

    os.makedirs(TEMP_FOLDER, exist_ok=True)
    logging.info("Carpeta temporal limpiada y (re)creada correctamente.")


def clean_sql_script(script: str) -> str:
    """
    Elimina comentarios de bloque (/* ... */) en el script SQL.

    Si queda un bloque desbalanceado (por ejemplo, un /* sin */), 
    se elimina todo el contenido a partir de ese /*.

    Args:
        script: Cadena que contiene el script SQL original.

    Returns:
        El script SQL sin los comentarios de bloque.
    """
    logging.info("Eliminando comentarios de bloque en el script SQL...")
    # Elimina todos los bloques /* ... */ bien formados
    cleaned = re.sub(r'/\*.*?\*/', '', script, flags=re.DOTALL)

    # Si queda algún /* sin cerrar, eliminamos desde ese punto hasta el final
    if '/*' in cleaned:
        cleaned = re.sub(r'/\*.*', '', cleaned, flags=re.DOTALL)

    return cleaned
