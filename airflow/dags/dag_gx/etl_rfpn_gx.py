import os
import yaml
import json
import sqlalchemy
import logging
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# ------------------- CONFIGURACIÓN -------------------

CONFIG_PATH = "/opt/airflow/otl/etl/etl_rfpn/config.json"
OUTPUT_YML_FOLDER = "/opt/airflow/otl/etl/etl_rfpn/dags_rfpn/gx/"
SCHEMAS = ["insumos", "estructura_intermedia", "ladm"]

# Mapeo simple de Postgres -> GE
POSTGRES_TO_GE_TYPE = {
    "integer": "int",
    "bigint": "int",
    "smallint": "int",
    "numeric": "float",
    "double precision": "float",
    "real": "float",
    "character varying": "str",
    "text": "str",
    "date": "datetime",
    "timestamp without time zone": "datetime",
    "timestamp with time zone": "datetime",
    "boolean": "bool",
    # etc.
}

def map_postgres_type_to_ge(data_type: str) -> str:
    """Traduce data_type de Postgres a un tipo que Great Expectations reconozca."""
    return POSTGRES_TO_GE_TYPE.get(data_type.lower(), "str")

# ------------------------- FUNCIONES UTILITARIAS -------------------------
# hola
def leer_configuracion():
    """Lee la configuración desde config.json."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
        logging.info("Configuración cargada correctamente.")
        return config
    except Exception as e:
        logging.error(f"Error leyendo la configuración: {e}")
        raise

# ------------------- FUNCIONES -------------------

def generar_suite_ge_por_esquema(schema):
    """
    1) Obtiene tablas y columnas del esquema.
    2) Construye una 'suite' con expect_column_values_to_be_of_type
       para cada columna.
    3) Exporta la suite a un archivo YAML: gx_{schema}.yml
    """
    config = leer_configuracion()
    db_config = config["db"]
    db_user = db_config["user"]
    db_password = db_config["password"]
    db_host = db_config["host"]
    db_port = db_config["port"]
    db_name="arfw_etl_rfpn"

    DB_CONNECTION_STRING_GE = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    engine = sqlalchemy.create_engine(DB_CONNECTION_STRING_GE)

    # 1. Listar tablas del esquema
    query_tables = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE lower(table_schema) = lower('{schema}');
    """
    tables_df = pd.read_sql(query_tables, engine)
    tables = tables_df["table_name"].tolist()

    # Estructura base de la suite
    suite_dict = {
        "data_asset_type": "Dataset",
        "expectations": [],
        "meta": {
            "great_expectations_version": "0.15.50",  # ajusta según tu versión
            "notes": {
                "format": "markdown",
                "content": f"Expectations generadas automáticamente para el esquema {schema}"
            }
        }
    }

    # 2. Para cada tabla, obtener columnas y crear expectativas
    for table in tables:
        query_columns = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE lower(table_schema) = lower('{schema}')
              AND lower(table_name) = lower('{table}')
            ORDER BY ordinal_position;
        """
        columns_df = pd.read_sql(query_columns, engine)

        # Añadimos una expectativa de "expect_table_columns_to_match_set" (opcional)
        # para asegurar que la tabla contenga las columnas exactas que se esperan
        # (puedes omitirlo si no deseas forzar la lista exacta de columnas).
        all_columns = columns_df["column_name"].tolist()
        suite_dict["expectations"].append({
            "expectation_type": "expect_table_columns_to_match_set",
            "kwargs": {
                "column_set": all_columns,
                "exact_match": True  # True => deben coincidir exactamente
            },
            "meta": {
                "table": table
            }
        })

        # Para cada columna, creamos "expect_column_values_to_be_of_type"
        for _, row in columns_df.iterrows():
            col_name = row["column_name"]
            pg_type = row["data_type"]
            ge_type = map_postgres_type_to_ge(pg_type)

            suite_dict["expectations"].append({
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {
                    "column": col_name,
                    "type_": ge_type
                },
                "meta": {
                    "table": table,
                    "postgres_type": pg_type
                }
            })

    # 3. Guardar la suite en un archivo YAML
    output_filename = f"gx_{schema}.yml"
    output_path = os.path.join(OUTPUT_YML_FOLDER, output_filename)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(suite_dict, f, default_flow_style=False, sort_keys=False)

    print(f"✅ Suite de Great Expectations generada para '{schema}' en: {output_path}")

def generar_yml_ge():
    """
    Genera la suite de expectativas de Great Expectations para cada esquema de SCHEMAS.
    """
    for schema in SCHEMAS:
        generar_suite_ge_por_esquema(schema)

# ------------------- DAG DE AIRFLOW -------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 25)
}

with DAG(
    "ge_validate_schema_rfpn",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    generar_yml_ge_task = PythonOperator(
        task_id="Generar_YML_GE",
        python_callable=generar_yml_ge
    )
    
    generar_yml_ge_task
