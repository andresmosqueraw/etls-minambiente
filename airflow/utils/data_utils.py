import os
import zipfile
import requests
import subprocess
import logging

from utils.utils import (
    leer_configuracion,
    limpiar_carpeta_temporal
)

def obtener_insumos_desde_web(cfg, **context):
    """
    Descarga los insumos definidos en la configuración.
    Si falla la descarga, intenta obtener el ZIP local mediante _validar_archivo_local.
    Si tampoco se encuentra un respaldo local, lanza una excepción.
    Retorna un diccionario con { key: zip_path }.
    """
    logging.info("Iniciando obtener_insumos_desde_web...")
    limpiar_carpeta_temporal(cfg)
    
    config = leer_configuracion(cfg)
    insumos_web = config.get("insumos_web", {})
    if not insumos_web:
        logging.error("No se encontraron 'insumos_web' en la configuración.")
        raise ValueError("No se encontraron 'insumos_web' en la configuración.")
    
    insumos_local = config.get("insumos_local", {})
    base_local = "/opt/airflow/OTL/ETL_RL2"
    TEMP_FOLDER = cfg["TEMP_FOLDER"]
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    resultado = {}
    errores = []

    for key, url in insumos_web.items():
        zip_path = os.path.join(TEMP_FOLDER, f"{key}.zip")
        try:
            logging.info(f"Intentando descargar insumo '{key}' desde {url}...")
            response = requests.get(url, stream=True, timeout=10)
            response.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            # Verificar que el ZIP descargado no esté vacío
            if os.path.getsize(zip_path) == 0:
                logging.warning(f"El insumo '{key}' descargado desde la web está vacío. Se eliminará y se usará respaldo local.")
                os.remove(zip_path)
                errores.append({
                    "url": url,
                    "key": key,
                    "insumos_local": insumos_local,
                    "base_local": base_local,
                    "error": "Archivo descargado está vacío."
                })
                zip_path = None
            else:
                logging.info(f"Insumo '{key}' descargado exitosamente.")
        except Exception as e:
            logging.warning(f"No se pudo descargar el insumo '{key}', se intentará obtener desde el respaldo local.")
            errores.append({
                "url": url,
                "key": key,
                "insumos_local": insumos_local,
                "base_local": base_local,
                "error": str(e)
            })
            zip_path = None
        
        resultado[key] = zip_path

    context["ti"].xcom_push(key="errores", value=errores)
    context["ti"].xcom_push(key="insumos_web", value=resultado)
    logging.info("Descarga de insumos finalizada (incluyendo los que fallaron y serán manejados en otra tarea).")
    return resultado

def ejecutar_copia_insumo_local(**context):
    errores = context["ti"].xcom_pull(key="errores", task_ids="Obtener_Insumos_Web")
    if not errores:
        return

    recuperados = {}
    for err in errores:
        url, key, insumos_local, base_local, error_msg = err.values()
        try:
            zip_path = copia_insumo_local(url, key, insumos_local, base_local, error_msg)
            recuperados[key] = zip_path
        except Exception as exc:
            logging.error(f"Fallo al recuperar '{key}': {exc}")

    context["ti"].xcom_push(key="insumos_local", value=recuperados)

def copia_insumo_local(url, key, insumos_local, base_local, e):
    """
    Maneja el error al descargar un archivo desde una URL.
    Intenta buscar un respaldo local y lanza una excepción si no se encuentra.
    """
    logging.error(f"Error al descargar '{url}': {e}")
    logging.info("Intentando buscar respaldo local...")
    zip_path = _validar_archivo_local(key, insumos_local, base_local)
    if not zip_path or not os.path.exists(zip_path):
        logging.error(f"No se encontró respaldo local para '{key}'.")
        raise Exception(f"Respaldo local no encontrado para '{key}'.")
    return zip_path

def _validar_archivo_local(key, insumos_local, base_local):
    logging.info("Iniciando _validar_archivo_local...")
    if key in insumos_local:
        local_zip_path = os.path.join(base_local, insumos_local[key].lstrip("/"))
        if os.path.exists(local_zip_path):
            logging.info(f"Usando archivo local para '{key}': {local_zip_path}")
            logging.info("\033[92m✔ _validar_archivo_local finalizó sin errores (archivo local existe).\033[0m")
            return local_zip_path
        else:
            msg = f"Archivo local para '{key}' no encontrado en {local_zip_path}."
            logging.error(msg)
            logging.error("\033[91m❌ _validar_archivo_local falló (no existe local).\033[0m")
            raise FileNotFoundError(msg)
    msg = f"No se encontró entrada local para '{key}' en la configuración."
    logging.error(msg)
    logging.error("\033[91m❌ _validar_archivo_local falló (sin entrada local).\033[0m")
    raise FileNotFoundError(msg)

def procesar_insumos_descargados(cfg, **context):
    """
    Descomprime los archivos ZIP obtenidos en la tarea 'Obtener_Insumos_Web'.
    Se espera recibir un diccionario con { key: zip_path }.
    """
    logging.info("Iniciando procesar_insumos_descargados...")
    ti = context["ti"]
    
    TEMP_FOLDER = cfg["TEMP_FOLDER"]
    
    insumos_web = ti.xcom_pull(task_ids="Obtener_Insumos_Web", key="insumos_web") or {}
    insumos_local = ti.xcom_pull(task_ids="copia_insumo_local_task", key="insumos_local") or {}

    insumos_totales = {**insumos_web, **insumos_local}

    if not insumos_totales:
        raise Exception("No se encontraron insumos para procesar.")

    resultados = []
    for key, zip_path in insumos_totales.items():
        if not zip_path or not os.path.exists(zip_path):
            logging.error(f"El archivo ZIP para '{key}' no existe: {zip_path}")
            raise Exception(f"Archivo ZIP no encontrado para '{key}'")
        extract_folder = os.path.join(TEMP_FOLDER, key)
        os.makedirs(extract_folder, exist_ok=True)
        _extraer_zip(zip_path, extract_folder)
        resultados.append({"key": key, "zip_path": zip_path})

    logging.info(f"Insumos procesados y descomprimidos: {resultados}")
    ti.xcom_push(key="insumos_procesados", value=resultados)
    logging.info("\033[92m✔ Descompresión finalizada.\033[0m")
    
    return resultados

def _extraer_zip(zip_path, extract_folder):
    logging.info("Iniciando _extraer_zip...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        logging.info(f"Archivo extraído correctamente en: {extract_folder}")
        logging.info("\033[92m✔ _extraer_zip finalizó sin errores.\033[0m")
    except zipfile.BadZipFile as e:
        logging.error(f"El archivo '{zip_path}' no es un ZIP válido: {e}")
        logging.error("\033[91m❌ _extraer_zip falló (BadZipFile).\033[0m")
        raise Exception(f"El archivo '{zip_path}' no es un ZIP válido: {e}")
    except Exception as e:
        logging.error(f"Error extrayendo '{zip_path}': {e}")
        logging.error("\033[91m❌ _extraer_zip falló.\033[0m")
        raise Exception(f"Error extrayendo '{zip_path}': {e}")


def ejecutar_importar_shp_a_postgres(cfg, **kwargs):
    """
    Recupera la información de insumos procesados desde XCom, busca en cada carpeta
    un archivo SHP y lo importa a la base de datos en la tabla correspondiente.
    """
    TEMP_FOLDER = cfg["TEMP_FOLDER"]
    logging.info("Iniciando ejecutar_importar_shp_a_postgres...")
    ti = kwargs['ti']
    try:
        insumos_info = ti.xcom_pull(task_ids='Descomprimir_Insumos', key="insumos_procesados")
        logging.info(f"Datos recuperados de XCom en 'Importar_SHP_Postgres': {insumos_info}")
        if not insumos_info or not isinstance(insumos_info, list):
            logging.error("No se encontró información válida de insumos en XCom.")
            raise Exception("No se encontró información válida de insumos en XCom.")
        config = leer_configuracion(cfg)
        db_config = config["db"]
        for insumo in insumos_info:
            if not isinstance(insumo, dict) or "key" not in insumo:
                logging.error(f"Formato incorrecto en insumo: {insumo}")
                continue
            key = insumo["key"]
            extract_folder = os.path.join(TEMP_FOLDER, key)
            shp_file = _buscar_shp_en_carpeta(extract_folder)
            if not shp_file:
                error_msg = f"No se encontró archivo SHP en {extract_folder} para '{key}'."
                logging.error(error_msg)
                raise FileNotFoundError(error_msg)
            table_name = f"insumos.{key}"
            logging.info(f"Ejecutando importación del SHP '{shp_file}' en la tabla '{table_name}'...")
            _importar_shp_a_postgres(db_config, shp_file, table_name)
        logging.info("\033[92m✔ ejecutar_importar_shp_a_postgres finalizó sin errores.\033[0m")
    except Exception as e:
        logging.error(f"Error en ejecutar_importar_shp_a_postgres: {e}")
        raise e

def _buscar_shp_en_carpeta(folder):
    logging.info("Iniciando _buscar_shp_en_carpeta...")
    for root, dirs, files in os.walk(folder):
        for file_name in files:
            if file_name.lower().endswith(".shp"):
                shp_path = os.path.join(root, file_name)
                # Verificar que el archivo exista y tenga contenido
                if os.path.exists(shp_path) and os.path.getsize(shp_path) > 0:
                    logging.info("\033[92m✔ _buscar_shp_en_carpeta encontró un SHP válido.\033[0m")
                    return shp_path
    logging.info("\033[91m❌ _buscar_shp_en_carpeta no encontró ningún SHP válido.\033[0m")
    return None

def _importar_shp_a_postgres(db_config, shp_file, table_name):
    logging.info(f"Importando '{shp_file}' en la tabla '{table_name}'...")
    command = [
        "ogr2ogr", "-f", "PostgreSQL",
        f"PG:host={db_config['host']} port={db_config['port']} dbname={db_config['db_name']} user={db_config['user']} password={db_config['password']}",
        shp_file,
        "-nln", table_name,
        "-overwrite",
        "-progress",
        "-lco", "GEOMETRY_NAME=geom",
        "-lco", "FID=gid",
        "-nlt", "PROMOTE_TO_MULTI",
        "-t_srs", "EPSG:9377"
    ]
    try:
        subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"Archivo '{shp_file}' importado correctamente en '{table_name}'.")
        logging.info("\033[92m✔ _importar_shp_a_postgres finalizó sin errores.\033[0m")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error importando '{table_name}': {e.stderr}")
        logging.error("\033[91m❌ _importar_shp_a_postgres falló.\033[0m")
        raise Exception(f"Error importando '{table_name}': {e.stderr}")