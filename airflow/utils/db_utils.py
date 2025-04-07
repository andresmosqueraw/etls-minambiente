import os
import psycopg2
import logging

from utils.utils import leer_configuracion


def ejecutar_sql(cfg, sql, params=None):
    """Ejecuta un script SQL completo en la base de datos."""
    config = leer_configuracion(cfg)
    db_config = config["db"]
    logging.info(f"Ejecutando SQL:\n{sql[:300]}...")  # Muestra solo primeros 300 caracteres
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["db_name"]
    )
    try:
        with conn.cursor() as cursor:
            if params:
                logging.info(f"Parámetros: {params}")
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
        conn.commit()
        logging.info("Script SQL ejecutado correctamente.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error ejecutando SQL: {e}")
        # Lanzamos excepción para marcar tarea fallida
        raise Exception(f"Error ejecutando SQL: {e}")
    finally:
        conn.close()


def validar_conexion_postgres(cfg):
    """Valida la conexión a PostgreSQL."""
    logging.info("Iniciando validar_conexion_postgres...")
    try:
        config = leer_configuracion(cfg)
        db_config = config["db"]
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database="postgres"
        )
        conn.close()
        logging.info("Conexión exitosa a PostgreSQL.")
        logging.info("\033[92m✔ validar_conexion_postgres finalizó sin errores.\033[0m")
        return True
    except FileNotFoundError:
        raise
    except psycopg2.Error as e:
        logging.error(f"Error en la conexión: {e}")
        logging.error("\033[91m❌ validar_conexion_postgres falló.\033[0m")
        raise Exception(f"Error en la conexión: {e}")
    except Exception as ex:
        logging.error(f"Error: {ex}")
        logging.error("\033[91m❌ validar_conexion_postgres falló.\033[0m")
        raise Exception(f"Error validando la conexión: {ex}")
    
def revisar_existencia_db(cfg):
    """Verifica si la base de datos ya existe y define el flujo de ejecución en Airflow."""
    logging.info("Iniciando revisar_existencia_db...")
    logging.info("Revisando si la base de datos existe...")
    try:
        config = leer_configuracion(cfg)
        db_config = config["db"]
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database="postgres"
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_config["db_name"],))
            existe = cursor.fetchone() is not None
        conn.close()
        if existe:
            logging.info(f"La base de datos '{db_config['db_name']}' ya existe. Se omite la creación.")
            logging.info("\033[92m✔ revisar_existencia_db finalizó (DB existe).\033[0m")
            return ["Adicionar_Extensiones"]
        else:
            logging.info(f"La base de datos '{db_config['db_name']}' no existe. Se creará.")
            logging.info("\033[92m✔ revisar_existencia_db finalizó (DB no existe).\033[0m")
            return ["Crear_Base_Datos"]
    except Exception as e:
        logging.error(f"Error revisando existencia de la base de datos: {e}")
        logging.error("\033[91m❌ revisar_existencia_db falló.\033[0m")
        raise Exception(f"Error revisando existencia de la base de datos: {e}")

def crear_base_datos(cfg):
    """Crea la base de datos si no existe."""
    logging.info("Iniciando crear_base_datos...")
    logging.info("Creando base de datos...")
    try:
        config = leer_configuracion(cfg)
        db_config = config["db"]
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database="postgres"
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {db_config['db_name']};")
        conn.close()
        logging.info(f"Base de datos '{db_config['db_name']}' creada exitosamente.")
        logging.info("\033[92m✔ crear_base_datos finalizó sin errores.\033[0m")
    except Exception as e:
        logging.error(f"Error creando base de datos: {e}")
        logging.error("\033[91m❌ crear_base_datos falló.\033[0m")
        raise Exception(f"Error creando base de datos: {e}")

def adicionar_extensiones(cfg):
    """Adiciona las extensiones PostGIS y UUID a la base de datos."""
    logging.info("Iniciando adicionar_extensiones...")
    logging.info("Adicionando extensiones PostGIS y UUID...")
    try:
        config = leer_configuracion(cfg)
        db_config = config["db"]
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["db_name"]
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS plpgsql;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        conn.close()
        logging.info("Extensiones añadidas correctamente.")
        logging.info("\033[92m✔ adicionar_extensiones finalizó sin errores.\033[0m")
    except Exception as e:
        logging.error(f"Error adicionando extensiones: {e}")
        logging.error("\033[91m❌ adicionar_extensiones falló.\033[0m")
        raise Exception(f"Error adicionando extensiones: {e}")
       
def restablecer_esquema_insumos(cfg):
    logging.info("Restableciendo esquema 'insumos'...")
    try:
        ejecutar_sql(cfg, "DROP SCHEMA IF EXISTS insumos CASCADE; CREATE SCHEMA insumos;")
        logging.info("Esquema 'insumos' restablecido correctamente.")
    except Exception as e:
        logging.error(f"Error restableciendo esquema 'insumos': {e}")
        raise Exception(f"Error restableciendo esquema 'insumos': {e}")


def restablecer_esquema_estructura_intermedia(cfg):
    logging.info("Restableciendo esquema 'estructura_intermedia'...")
    try:
        ejecutar_sql(cfg, "DROP SCHEMA IF EXISTS estructura_intermedia CASCADE; CREATE SCHEMA estructura_intermedia;")
        logging.info("Esquema 'estructura_intermedia' restablecido correctamente.")
    except Exception as e:
        logging.error(f"Error restableciendo esquema 'estructura_intermedia': {e}")
        raise Exception(f"Error restableciendo esquema 'estructura_intermedia': {e}")


def restablecer_esquema_ladm(cfg):
    logging.info("Restableciendo esquema 'ladm'...")
    try:
        ejecutar_sql(cfg, "DROP SCHEMA IF EXISTS ladm CASCADE; CREATE SCHEMA ladm;")
        logging.info("Esquema 'ladm' restablecido correctamente.")
    except Exception as e:
        logging.error(f"Error restableciendo esquema 'ladm': {e}")
        raise Exception(f"Error restableciendo esquema 'ladm': {e}")