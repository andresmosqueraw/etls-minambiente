import logging

from utils.db_utils import ejecutar_sql
from utils.utils import clean_sql_script

from dags.rl2.etl_rl2_sql import (
    estructura_intermedia,
    transformacion_datos,
    validar_estructura,
    importar_al_modelo
)

def ejecutar_importar_estructura_intermedia(cfg):
    logging.info("Importando estructura_intermedia...")
    try:
        script_sql = estructura_intermedia()
        if isinstance(script_sql, str):
            script_sql = clean_sql_script(script_sql)
            ejecutar_sql(cfg, script_sql)
            logging.info("Estructura_intermedia importada correctamente.")
        else:
            # Si la función maneja la importación internamente
            logging.info("Estructura_intermedia importada por función interna.")
    except Exception as e:
        logging.error(f"Error importando estructura_intermedia: {e}")
        raise Exception(f"Error importando estructura_intermedia: {e}")


def ejecutar_migracion_datos_estructura_intermedia(cfg):
    logging.info("Migrando datos a estructura_intermedia...")
    try:
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
        resultado = validar_estructura()
        if resultado is not None:
            if isinstance(resultado, bool) and not resultado:
                # Si la función de validación retorna False, marcamos error
                raise Exception("Validación de datos falló (retornó False).")
            elif isinstance(resultado, str):
                # Si retorna un script, lo ejecutamos
                ejecutar_sql(cfg, resultado)
        logging.info("Validación de datos completada.")
    except Exception as e:
        logging.error(f"Error validando datos: {e}")
        raise Exception(f"Error validando datos: {e}")


def ejecutar_migracion_datos_ladm(cfg):
    logging.info("Migrando datos al modelo LADM...")
    try:
        script_sql = importar_al_modelo()
        if isinstance(script_sql, str):
            ejecutar_sql(cfg, script_sql)
            logging.info("Migración a LADM completada.")
        else:
            logging.info("Migración a LADM completada por función interna.")
    except Exception as e:
        logging.error(f"Error migrando a LADM: {e}")
        raise Exception(f"Error migrando a LADM: {e}")
