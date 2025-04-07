import os
import subprocess
import logging

from utils.rl2.utils import leer_configuracion
from config.general_config import (
    MODEL_DIR,
    XTF_DIR,
    ILI2DB_JAR_PATH,
    EPSG_SCRIPT
)

def exportar_datos_ladm_rl2():
    logging.info("Iniciando exportar_datos_ladm_rl2...")
    logging.info("Exportando datos del esquema 'ladm' a XTF (ili2db) ...")
    try:
        config = leer_configuracion()
        db_config = config["db"]
        ili2db_path = ILI2DB_JAR_PATH
        model_dir = MODEL_DIR
        xtf_folder = "/opt/airflow/OTL/ETL_RL2/xtf"
        if not os.path.exists(xtf_folder):
            os.makedirs(xtf_folder)
        xtf_path = os.path.join(xtf_folder, "rl2.xtf")
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
            "--models", "LADM_COL_v_1_0_0_Ext_RL2",
            "--iliMetaAttrs", "NULL",
            "--defaultSrsAuth", "EPSG",
            "--defaultSrsCode", "9377",
            xtf_path
        ]
        logging.info("Ejecutando exportación a XTF:")
        logging.info(" ".join(command))
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"Exportación a XTF completada: {result.stderr.strip()}")
        logging.info("\033[92m✔ exportar_datos_ladm_rl2 finalizó sin errores.\033[0m")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error exportando XTF: {e.stderr}")
        logging.error("\033[91m❌ exportar_datos_ladm_rl2 falló.\033[0m")
        raise Exception(f"Error exportando XTF: {e.stderr}")
    except Exception as ex:
        logging.error(f"Error en exportar_datos_ladm_rl2: {ex}")
        logging.error("\033[91m❌ exportar_datos_ladm_rl2 falló.\033[0m")
        raise Exception(f"Error exportando XTF: {e.stderr}")
    
def importar_esquema_ladm_rl2():
    logging.info("Iniciando importar_esquema_ladm_rl2...")
    logging.info("Importando esquema LADM-RL2...")
    try:
        config = leer_configuracion()
        db_config = config["db"]
        if not os.path.exists(ILI2DB_JAR_PATH):
            logging.error("\033[91m❌ importar_esquema_ladm_rl2 falló. JAR no encontrado.\033[0m")
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
            "--models", "LADM_COL_v_1_0_0_Ext_RL2",
            "--iliMetaAttrs", "NULL"
        ]
        logging.info("Ejecutando ili2pg para importar LADM-RL2...")
        logging.info(" ".join(command))
        subprocess.run(command, check=True)
        logging.info("Esquema LADM-RL2 importado correctamente.")
        logging.info("\033[92m✔ importar_esquema_ladm_rl2 finalizó sin errores.\033[0m")
    except subprocess.CalledProcessError as e:
        logging.error("\033[91m❌ importar_esquema_ladm_rl2 falló.\033[0m")
        raise Exception(f"Error importando LADM-RL2: {e}")
    except Exception as ex:
        logging.error(f"Error: {ex}")
        logging.error("\033[91m❌ importar_esquema_ladm_rl2 falló.\033[0m")
        raise Exception(f"Error importando LADM-RL2: {e}")