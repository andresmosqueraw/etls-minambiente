import logging
import importlib

from utils.db_utils import ejecutar_sql
from utils.utils import clean_sql_script

# Función para importar dinámicamente el módulo SQL según ETL_DIR en la configuración
def get_etl_sql_module(cfg):
    etl_dir = cfg.get("ETL_DIR", "").upper()
    if "RFPP" in etl_dir:
        module_path = "dags.rfpp.etl_rfpp_sql"
    elif "RL2" in etl_dir:
        module_path = "dags.rl2.etl_rl2_sql"
    elif "PRM" in etl_dir:
        module_path = "dags.prm.etl_prm_sql"
    else:
        raise Exception("No se encontró un módulo SQL adecuado para ETL_DIR: " + cfg.get("ETL_DIR", ""))
    print(f"Importando módulo SQL desde: {module_path}")
    return importlib.import_module(module_path)

# Función auxiliar que carga y retorna las funciones SQL requeridas
def load_sql_functions(cfg):
    sql_module = get_etl_sql_module(cfg)
    return (
        sql_module.estructura_intermedia,
        sql_module.transformacion_datos,
        sql_module.validar_estructura,
        sql_module.importar_al_modelo
    )

def ejecutar_importar_estructura_intermedia(cfg):
    logging.info("Importando estructura_intermedia...")
    try:
        estructura_intermedia, _, _, _ = load_sql_functions(cfg)
        script_sql = estructura_intermedia()
        if isinstance(script_sql, str):
            script_sql = clean_sql_script(script_sql)
            ejecutar_sql(cfg, script_sql)
            logging.info("Estructura_intermedia importada correctamente.")
        else:
            logging.info("Estructura_intermedia importada por función interna.")
    except Exception as e:
        logging.error(f"Error importando estructura_intermedia: {e}")
        raise Exception(f"Error importando estructura_intermedia: {e}")

def ejecutar_migracion_datos_estructura_intermedia(cfg):
    logging.info("Migrando datos a estructura_intermedia...")
    try:
        _, transformacion_datos, _, _ = load_sql_functions(cfg)
        script_sql = transformacion_datos()
        if isinstance(script_sql, str):
            ejecutar_sql(cfg, script_sql)
            logging.info("Migración a estructura_intermedia completada.")
        else:
            logging.info("Migración a estructura_intermedia completada por función interna.")
    except Exception as e:
        logging.error(f"Error migrando a estructura_intermedia: {e}")
        raise Exception(f"Error migrando a estructura_intermedia: {e}")

def ejecutar_validacion_datos(cfg):
    logging.info("Validando datos en la estructura intermedia...")
    try:
        _, _, validar_estructura, _ = load_sql_functions(cfg)
        resultado = validar_estructura()
        if resultado is not None:
            if isinstance(resultado, bool) and not resultado:
                raise Exception("Validación de datos falló (retornó False).")
            elif isinstance(resultado, str):
                ejecutar_sql(cfg, resultado)
        logging.info("Validación de datos completada.")
    except Exception as e:
        logging.error(f"Error validando datos: {e}")
        raise Exception(f"Error validando datos: {e}")

def ejecutar_migracion_datos_ladm(cfg):
    logging.info("Migrando datos al modelo LADM...")
    try:
        _, _, _, importar_al_modelo = load_sql_functions(cfg)
        script_sql = importar_al_modelo()
        if isinstance(script_sql, str):
            ejecutar_sql(cfg, script_sql)
            logging.info("Migración a LADM completada.")
        else:
            logging.info("Migración a LADM completada por función interna.")
    except Exception as e:
        logging.error(f"Error migrando a LADM: {e}")
        raise Exception(f"Error migrando a LADM: {e}")