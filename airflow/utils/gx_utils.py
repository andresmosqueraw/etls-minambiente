import os
import sqlalchemy
import pandas as pd
import yaml  # Requiere PyYAML para leer archivos YAML
import logging

from utils.utils import leer_configuracion


def _cargar_expectativas_desde_yaml(yaml_filename, cfg):
    logging.info("Iniciando _cargar_expectativas_desde_yaml...")
    try:
        GX_DIR = cfg["GX_DIR"]
        yaml_path = os.path.join(GX_DIR, yaml_filename)
        if not os.path.exists(yaml_path):
            msg = f"Archivo de expectativas {yaml_path} no existe."
            logging.error(msg)
            logging.error("\033[91m❌ _cargar_expectativas_desde_yaml falló (no existe YAML).\033[0m")
            raise FileNotFoundError(msg)
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if "expectations" not in data:
            msg = "El archivo de expectativas no contiene 'expectations'."
            logging.error(msg)
            logging.error("\033[91m❌ _cargar_expectativas_desde_yaml falló (no 'expectations').\033[0m")
            raise ValueError(msg)
        logging.info("\033[92m✔ _cargar_expectativas_desde_yaml finalizó sin errores.\033[0m")
        return data
    except Exception as e:
        raise e

def _obtener_tablas_esperadas(data):
    logging.info("Iniciando _obtener_tablas_esperadas...")
    expected_tables = {}
    for exp in data["expectations"]:
        if exp.get("expectation_type") == "expect_table_columns_to_match_set":
            table = exp.get("meta", {}).get("table")
            if table:
                expected_tables[table] = {
                    "expected_columns": set(exp.get("kwargs", {}).get("column_set", [])),
                    "exact_match": exp.get("kwargs", {}).get("exact_match", False)
                }
    logging.info("\033[92m✔ _obtener_tablas_esperadas finalizó sin errores.\033[0m")
    return expected_tables

def _obtener_engine_sqlalchemy(cfg):
    logging.info("Iniciando _obtener_engine_sqlalchemy...")
    config = leer_configuracion(cfg)
    db_config = config["db"]
    try:
        url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['db_name']}"
        engine = sqlalchemy.create_engine(url)
        return engine
    except Exception as e:
        raise Exception(f"Error creando engine SQLAlchemy: {e}")

def _obtener_tablas_esquema(engine, schema):
    logging.info("Iniciando _obtener_tablas_esquema...")
    try:
        query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}';
        """
        df_tables = pd.read_sql(query, engine)
        logging.info("\033[92m✔ _obtener_tablas_esquema finalizó sin errores.\033[0m")
        return set(df_tables["table_name"].tolist())
    except Exception as e:
        logging.error(f"Error consultando tablas del esquema '{schema}': {e}")
        logging.error("\033[91m❌ _obtener_tablas_esquema falló.\033[0m")
        raise

def _revisar_tablas_encontradas(schema, expected_tables, actual_tables):
    logging.info("Iniciando _revisar_tablas_encontradas...")
    report_lines = [f"REPORTE DE ESTRUCTURA PARA EL ESQUEMA '{schema}':\n"]
    report_lines.append("Tablas esperadas vs. encontradas:")
    for table in expected_tables.keys():
        if table in actual_tables:
            report_lines.append(f"  {table}: ✔️")
        else:
            report_lines.append(f"  {table}: ❌ (Falta)")
    report_lines.append("\nDetalle de columnas para cada tabla:")
    logging.info("\033[92m✔ _revisar_tablas_encontradas finalizó sin errores.\033[0m")
    return report_lines

def _revisar_columnas_tabla(engine, schema, expected_tables, actual_tables):
    logging.info("Iniciando _revisar_columnas_tabla...")
    report = []
    for table, exp_details in expected_tables.items():
        if table not in actual_tables:
            continue
        expected_cols = exp_details["expected_columns"]
        exact = exp_details["exact_match"]
        query_cols = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
              AND table_name = '{table}';
        """
        df_cols = pd.read_sql(query_cols, engine)
        actual_cols = set(df_cols["column_name"].tolist())
        if exact:
            report.append(_comparar_columnas_exactas(table, expected_cols, actual_cols))
        else:
            report.append(_comparar_columnas_subconjunto(table, expected_cols, actual_cols))
    logging.info("\033[92m✔ _revisar_columnas_tabla finalizó sin errores.\033[0m")
    return report

def _comparar_columnas_exactas(table, expected_cols, actual_cols):
    logging.info(f"Iniciando _comparar_columnas_exactas para {table}...")
    if actual_cols == expected_cols:
        logging.info("\033[92m✔ Coincidencia exacta de columnas.\033[0m")
        return f"  {table}: Se esperaban {sorted(expected_cols)} y se encontraron exactamente. ✔️"
    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols
    resultado = [f"  {table}: ❌ Diferencias en columnas:"]
    if missing:
        resultado.append(f"    Faltantes: {sorted(missing)}")
    if extra:
        resultado.append(f"    Extras: {sorted(extra)}")
    logging.info("\033[91m❌ Diferencia de columnas detectada en _comparar_columnas_exactas.\033[0m")
    return "\n".join(resultado)

def _comparar_columnas_subconjunto(table, expected_cols, actual_cols):
    logging.info(f"Iniciando _comparar_columnas_subconjunto para {table}...")
    if expected_cols.issubset(actual_cols):
        logging.info("\033[92m✔ Subconjunto de columnas encontrado.\033[0m")
        return f"  {table}: Se esperaba (subconjunto) {sorted(expected_cols)} y se encontraron. ✔️"
    missing = expected_cols - actual_cols
    logging.info("\033[91m❌ Faltan columnas en _comparar_columnas_subconjunto.\033[0m")
    return f"  {table}: ❌ Faltan columnas: {sorted(missing)}"
