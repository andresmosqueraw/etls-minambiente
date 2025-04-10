import os
import zipfile
import requests
import subprocess
import logging
import pandas as pd
import shutil
import re
import sqlalchemy

from utils.utils import (
    leer_configuracion,
    limpiar_carpeta_temporal
)

from utils.gx_utils import (
    _obtener_engine_sqlalchemy
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
    base_local = cfg["ETL_DIR"]
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
    Procesa insumos descargados o copiados localmente:
    - Si el insumo es un archivo ZIP, se revisa su contenido:
         * Si contiene la estructura propia de un Excel (por ejemplo, "[Content_Types].xml" o la carpeta "xl/"),
           se copia el archivo y se renombra a .xlsx, tratándolo como Excel.
         * Si no es un Excel, se descomprime en TEMP_FOLDER/<key>.
    - Si el insumo es Excel (.xls o .xlsx) o GeoJSON (.geojson), se copia a TEMP_FOLDER/<key>.
    - Se ignoran otros formatos.
    """
    logging.info("Iniciando procesar_insumos_descargados...")
    ti = context["ti"]

    # Se obtiene la carpeta temporal desde la configuración.
    TEMP_FOLDER = cfg["TEMP_FOLDER"]

    # Recuperamos los insumos (descargados y/o copiados localmente) desde XCom.
    insumos_web = ti.xcom_pull(task_ids="Obtener_Insumos_Web", key="insumos_web") or {}
    insumos_local = ti.xcom_pull(task_ids="copia_insumo_local_task", key="insumos_local") or {}
    insumos_totales = {**insumos_web, **insumos_local}

    if not insumos_totales:
        raise Exception("❌ No se encontraron insumos para procesar.")

    resultados = []

    for key, file_path in insumos_totales.items():
        if not file_path or not os.path.exists(file_path):
            msg = f"❌ Archivo para '{key}' no encontrado: {file_path}"
            logging.error(msg)
            raise Exception(msg)

        # Se crea la carpeta de destino para este insumo.
        extract_folder = os.path.join(TEMP_FOLDER, key)
        os.makedirs(extract_folder, exist_ok=True)
        ext = os.path.splitext(file_path)[1].lower()

        if ext == ".zip":
            try:
                # Abrimos el ZIP para inspeccionar su contenido.
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    namelist = zip_ref.namelist()

                # Si se detecta estructura típica de un Excel, se tratará como Excel.
                if "[Content_Types].xml" in namelist or any(n.startswith("xl/") for n in namelist):
                    destino = os.path.join(extract_folder, key + ".xlsx")
                    shutil.copy(file_path, destino)
                    logging.info(f"Se detectó un Excel empaquetado en ZIP. Archivo copiado y renombrado a '{destino}'")
                    resultados.append({
                        "key": key,
                        "file": destino,
                        "folder": extract_folder,
                        "type": "excel"
                    })
                else:
                    logging.info(f"Descomprimiendo ZIP '{file_path}' en: {extract_folder}")
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_folder)
                    logging.info(f"✔ ZIP extraído correctamente en: {extract_folder}")
                    resultados.append({
                        "key": key,
                        "file": file_path,
                        "folder": extract_folder,
                        "type": "zip"
                    })
            except zipfile.BadZipFile as e:
                msg = f"❌ Archivo ZIP inválido '{file_path}': {e}"
                logging.error(msg)
                raise Exception(msg)
            except Exception as e:
                msg = f"❌ Error extrayendo '{file_path}': {e}"
                logging.error(msg)
                raise Exception(msg)
        elif ext in [".xls", ".xlsx", ".geojson"]:
            destino = os.path.join(extract_folder, os.path.basename(file_path))
            try:
                shutil.copy(file_path, destino)
                logging.info(f"✔ Archivo '{file_path}' copiado a '{destino}'")
                resultados.append({
                    "key": key,
                    "file": destino,
                    "folder": extract_folder,
                    "type": "file"
                })
            except Exception as e:
                msg = f"❌ Error copiando el archivo '{file_path}' a '{destino}': {e}"
                logging.error(msg)
                raise Exception(msg)
        else:
            logging.warning(f"⚠ Formato '{ext}' no soportado para '{file_path}', se omite.")
            continue

    ti.xcom_push(key="insumos_procesados", value=resultados)
    logging.info(f"✔ Insumos procesados correctamente: {resultados}")
    return resultados


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
            shp_file = _buscar_archivos_en_carpeta(extract_folder)
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
    
def ejecutar_importacion_general_a_postgres(cfg, **kwargs):
    """
    Importa archivos SHP, GeoJSON y Excel a PostgreSQL. Soporta dos modos:
    - Con `cfg` (modo TEMP_FOLDER): busca los archivos en TEMP_FOLDER + key.
    - Sin `cfg`: usa las rutas de carpetas directamente desde los insumos.
    """
    logging.info("Iniciando ejecutar_importacion_general_a_postgres...")
    ti = kwargs['ti']
    try:
        insumos_info = ti.xcom_pull(task_ids='Descomprimir_Insumos', key="insumos_procesados")
        logging.info(f"Datos recuperados de XCom: {insumos_info}")
        if not insumos_info or not isinstance(insumos_info, list):
            logging.error("❌ No se encontró información válida de insumos en XCom.")
            raise Exception("No se encontró información válida de insumos en XCom.")

        engine = _obtener_engine_sqlalchemy(cfg)
        config = leer_configuracion(cfg)
        db_config = config["db"]

        for insumo in insumos_info:
            if not isinstance(insumo, dict) or "key" not in insumo:
                logging.error(f"Formato incorrecto en insumo: {insumo}")
                continue

            key = insumo["key"]
            if cfg and "TEMP_FOLDER" in cfg:
                folder = os.path.join(cfg["TEMP_FOLDER"], key)
                # 👇 LOGS DE DIAGNÓSTICO
                logging.info(f"[DEBUG] Carpeta esperada para '{key}': {folder}")
                if os.path.exists(folder):
                    logging.info(f"[DEBUG] Contenido de la carpeta '{folder}': {os.listdir(folder)}")
                else:
                    logging.warning(f"[DEBUG] La carpeta '{folder}' no existe.")
                
                if not os.path.exists(folder):
                    # Tal vez el archivo ya es .xlsx directamente
                    archivo_directo = insumo.get("zip_path")  # ya es ruta absoluta
                    if archivo_directo and archivo_directo.lower().endswith(".xlsx") and os.path.exists(archivo_directo):
                        logging.info(f"Procesando Excel directo para '{key}': {archivo_directo}")
                        _importar_excel_a_postgres(cfg, engine, archivo_directo, table_name=key, schema="insumos")
                        continue  # saltar búsqueda de carpetas

            if not folder:
                logging.error(f"No se pudo determinar la carpeta para el insumo '{key}'.")
                continue

            shp_files = _buscar_archivos_en_carpeta(folder, [".shp"])
            geojson_files = _buscar_archivos_en_carpeta(folder, [".geojson"])
            xlsx_files = _buscar_archivos_en_carpeta(folder, [".xlsx"])
            
            print(f"Archivos SHP encontrados: {shp_files}")
            print(f"Archivos GeoJSON encontrados: {geojson_files}")
            print(f"Archivos Excel encontrados: {xlsx_files}")

            if not shp_files and not geojson_files and not xlsx_files:
                msg = f"❌ El insumo '{key}' no contiene archivos compatibles."
                logging.error(msg)
                raise Exception(msg)

            for shp_file in shp_files:
                tname = f"insumos.{key}"
                logging.info(f"Importando SHP: {shp_file} -> {tname}")
                _importar_shp_a_postgres(db_config, shp_file, tname)

            for geojson_file in geojson_files:
                tname = f"insumos.{key}"
                logging.info(f"Importando GeoJSON: {geojson_file} -> {tname}")
                _importar_geojson_a_postgres(db_config, geojson_file, tname)

            for xlsx_file in xlsx_files:
                logging.info(f"Importando Excel: {xlsx_file} -> insumos.{key}")
                _importar_excel_a_postgres(cfg, engine, xlsx_file, table_name=key, schema="insumos")

        logging.info("✔ Importación a PostgreSQL completada correctamente.")
    except Exception as e:
        logging.error(f"❌ Error en ejecutar_importacion_general_a_postgres: {e}")
        raise

def _buscar_archivos_en_carpeta(folder, extensiones):
    encontrados = []
    for root, _, files in os.walk(folder):
        for file in files:
            if any(file.lower().endswith(ext) for ext in extensiones):
                full_path = os.path.join(root, file)
                if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                    encontrados.append(full_path)
    return encontrados

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
    
def _importar_geojson_a_postgres(db_config, geojson_file, table_name):
    logging.info(f"Iniciando _importar_geojson_a_postgres... Importando '{geojson_file}' en '{table_name}'...")
    command = [
        "ogr2ogr", "-f", "PostgreSQL",
        f"PG:host={db_config['host']} port={db_config['port']} dbname={db_config['db_name']} user={db_config['user']} password={db_config['password']}",
        geojson_file,
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
        logging.info(f"\033[92m✔ GeoJSON '{geojson_file}' importado correctamente en '{table_name}'.\033[0m")
    except subprocess.CalledProcessError as e:
        logging.error(f"\033[91m❌ _importar_geojson_a_postgres falló: {e.stderr}\033[0m")
        raise Exception(f"Error importando GeoJSON en '{table_name}': {e.stderr}")

def _importar_excel_a_postgres(cfg, engine, xlsx_file, table_name, schema="insumos", ): 
    if "ETL_AP" in cfg["ETL_DIR"]: 
        config = leer_configuracion(cfg)
        db_config = config["db"] 
        print("entre")
        return import_excel_to_db(db_config, xlsx_file, table_name)
    print("no entra")
    logging.info(f"Iniciando _importar_excel_a_postgres... Importando '{xlsx_file}' en '{schema}.{table_name}'...")
    try:
        df = pd.read_excel(xlsx_file)
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists='replace', index=False)
        logging.info(f"\033[92m✔ Excel '{xlsx_file}' importado correctamente en {schema}.{table_name}.\033[0m")
    except Exception as e:
        raise Exception(f"Error importando Excel '{xlsx_file}': {e}")
    
def import_excel_to_db(db_config, excel_file, key):
    """
    Lee las hojas "General" y "Actos" del Excel.
    
    La hoja "General" es la base (por ejemplo, 30 filas). Para cada "Id del área protegida" en 
    la hoja "Actos" se selecciona la primera fila que cumpla que "Objeto del acto" esté en la 
    lista de valores válidos; si ninguna cumple, se toma la primera fila del grupo.
    
    Se realiza un merge LEFT usando "Id del área protegida" como llave, de modo que se 
    conservan exactamente las filas de la hoja "General". El resultado se inserta en la 
    tabla insumos.<key>_excel.
    """
    logging.info(f"Iniciando import_excel_to_db para {excel_file} en insumo '{key}'...")

    # Leer la hoja "General" sin usar ninguna columna como índice y reiniciar índice.
    try:
        df_general = pd.read_excel(excel_file, sheet_name="General", index_col=None)
        df_general = df_general.reset_index(drop=True)
        logging.info(f"Hoja 'General' leída con {len(df_general)} filas.")
    except Exception as e:
        logging.error(f"Error leyendo la hoja 'General' de {excel_file}: {e}")
        raise Exception(f"Error leyendo Excel (General): {e}")

    # Leer la hoja "Actos" de la misma forma.
    try:
        df_actos = pd.read_excel(excel_file, sheet_name="Actos", index_col=None)
        df_actos = df_actos.reset_index(drop=True)
        logging.info(f"Hoja 'Actos' leída con {len(df_actos)} filas.")
    except Exception as e:
        logging.error(f"Error leyendo la hoja 'Actos' de {excel_file}: {e}")
        raise Exception(f"Error leyendo Excel (Actos): {e}")

    # Lista de valores válidos para "Objeto del acto"
    valid_objetos = ["Declaratoria", "Registro RNSC", "Declaratoria y adopcion de plan de manejo"]

    # Función para seleccionar la fila adecuada en cada grupo
    def choose_row(group):
        matching = group[group["Objeto del acto"].isin(valid_objetos)]
        if not matching.empty:
            return matching.iloc[0]
        else:
            return group.iloc[0]

    try:
        # Agrupar por "Id del área protegida" y aplicar la función para elegir la fila deseada
        df_actos_reducido = df_actos.groupby("Id del área protegida", group_keys=False).apply(choose_row)
        # Resetear el índice para evitar que "Id del área protegida" esté tanto en el índice como en la columna
        df_actos_reducido = df_actos_reducido.reset_index(drop=True)
        logging.info(f"Reducción de 'Actos': {len(df_actos_reducido)} filas resultantes (1 por cada id).")
    except Exception as e:
        logging.error(f"Error reduciendo 'Actos': {e}")
        raise Exception(f"Error reduciendo 'Actos': {e}")

    try:
        # Realizar un merge LEFT usando la hoja General como base.
        df_merged = pd.merge(df_general, df_actos_reducido, on="Id del área protegida", how="left")
        logging.info(f"Merge realizado: {len(df_merged)} filas resultantes (deberían ser {len(df_general)}).")
    except Exception as e:
        logging.error(f"Error realizando merge: {e}")
        raise Exception(f"Error en merge: {e}")

    # Insertar el DataFrame resultante en la base de datos
    table_name = f"insumos.{shorten_identifier(key)}_excel"
    engine = sqlalchemy.create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['db_name']}"
    )
    try:
        df_merged.to_sql(table_name.split('.')[-1], engine, schema="insumos", if_exists="replace", index=False)
        logging.info(f"Excel importado en la tabla {table_name}.")
    except Exception as e:
        logging.error(f"Error insertando Excel en {table_name}: {e}")
        raise Exception(f"Error insertando Excel a DB: {e}")
    
def shorten_identifier(identifier):
    return re.sub(r'\W+', '_', identifier).lower()    