# /opt/cpi/test/ETL_RL2/airflow/utils/interlis_utils.py
import os
import subprocess
import logging

from utils.utils import leer_configuracion

def exportar_datos_ladm(cfg):
    """
    Exporta datos del esquema 'ladm' a XTF utilizando la configuración dinámica.
    """
    model_dir = cfg["MODEL_DIR"]
    xtf_folder = cfg["XTF_DIR"]
    ili2db_path = cfg["ILI2DB_JAR_PATH"]
    config = leer_configuracion(cfg)
    db_config = config["db"]
    logs = config["logs"]
    interlis = config["interlis"]
    logging.info(f"Iniciando exportar_datos_ladm para {logs["nombre_etl"]}...")
    logging.info("Exportando datos del esquema 'ladm' a XTF (ili2db) ...")
    try:        
        if not os.path.exists(xtf_folder):
            os.makedirs(xtf_folder)
        xtf_path = os.path.join(xtf_folder, interlis["nombre_archivo_xtf"])
        command = [
            "java",
            "-jar",
            ili2db_path,
            "--dbhost", db_config["host"],
            "--dbport", str(db_config["port"]),
            "--dbusr", db_config["user"],
            "--dbpwd", db_config["password"],
            "--dbdatabase", db_config["db_name"],
            "--dbschema", "ladm",
            "--export",
            "--exportTid",
            "--disableValidation",
            "--strokeArcs",
            "--modeldir", model_dir,
            "--models", interlis["nombre_modelo"],
            "--iliMetaAttrs", "NULL",
            "--defaultSrsAuth", "EPSG",
            "--defaultSrsCode", "9377",
            xtf_path
        ]
        logging.info("Ejecutando exportación a XTF:")
        logging.info(" ".join(command))
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"Exportación a XTF completada: {result.stderr.strip()}")
        logging.info(f"\033[92m✔ exportar_datos_ladm para {logs["nombre_etl"]}finalizó sin errores.\033[0m")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error exportando XTF: {e.stderr}")
        logging.error(f"\033[91m❌ exportar_datos_ladm para {logs["nombre_etl"]}falló.\033[0m")
        raise Exception(f"Error exportando XTF: {e.stderr}")
    except Exception as ex:
        logging.error(f"Error en exportar_datos_ladm: {ex}")
        logging.error(f"\033[91m❌ exportar_datos_ladm para {logs["nombre_etl"]}falló.\033[0m")
        raise Exception(f"Error exportando XTF: {ex}")

def importar_esquema_ladm(cfg):
    """
    Importa el esquema LADM usando la configuración dinámica.
    """
    MODEL_DIR = cfg["MODEL_DIR"]
    ILI2DB_JAR_PATH = cfg["ILI2DB_JAR_PATH"]
    EPSG_SCRIPT = cfg["EPSG_SCRIPT"]
    config = leer_configuracion(cfg)
    db_config = config["db"]
    logs = config["logs"]
    interlis = config["interlis"]
    logging.info(f"Iniciando importar_esquema_ladm para {logs["nombre_etl"]}...")
    logging.info(f"Importando esquema {logs["nombre_etl"]}...")
    try:
        if not os.path.exists(ILI2DB_JAR_PATH):
            logging.error("\033[91m❌ importar_esquema_ladm falló. JAR no encontrado.\033[0m")
            raise FileNotFoundError(f"Archivo JAR no encontrado: {ILI2DB_JAR_PATH}")
        command = [
            "java", "-Duser.language=es", "-Duser.country=ES", "-jar", ILI2DB_JAR_PATH,
            "--schemaimport", "--setupPgExt",
            "--dbhost", db_config["host"],
            "--dbport", str(db_config["port"]),
            "--dbusr", db_config["user"],
            "--dbpwd", db_config["password"],
            "--dbdatabase", db_config["db_name"],
            "--dbschema", "ladm",
            "--coalesceCatalogueRef", "--createNumChecks", "--createUnique",
            "--createFk", "--createFkIdx", "--coalesceMultiSurface",
            "--coalesceMultiLine", "--coalesceMultiPoint", "--coalesceArray",
            "--beautifyEnumDispName", "--createGeomIdx", "--createMetaInfo",
            "--expandMultilingual", "--createTypeConstraint",
            "--createEnumTabsWithId", "--createTidCol", "--smart2Inheritance",
            "--strokeArcs", "--createBasketCol",
            "--defaultSrsAuth", "EPSG",
            "--defaultSrsCode", "9377",
            "--preScript", EPSG_SCRIPT,
            "--postScript", "NULL",
            "--modeldir", MODEL_DIR,
            "--models", interlis["nombre_modelo"],
            "--iliMetaAttrs", "NULL"
        ]
        logging.info(f"Ejecutando ili2pg para importar {logs["nombre_etl"]}...")
        logging.info(" ".join(command))
        subprocess.run(command, check=True)
        logging.info(f"Esquema {logs["nombre_etl"]} importado correctamente.")
        logging.info(f"\033[92m✔ importar_esquema_ladm para {logs["nombre_etl"]} finalizó sin errores.\033[0m")
    except subprocess.CalledProcessError as e:
        logging.error(f"\033[91m❌ importar_esquema_ladm para {logs["nombre_etl"]} falló.\033[0m")
        raise Exception(f"Error importando {logs["nombre_etl"]}: {e}")
    except Exception as ex:
        logging.error(f"Error: {ex}")
        logging.error(f"\033[91m❌ importar_esquema_ladm para {logs["nombre_etl"]} falló.\033[0m")
        raise Exception(f"Error importando {logs["nombre_etl"]}: {ex}")