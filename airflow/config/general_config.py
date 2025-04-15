import os

# Detecta si se está ejecutando en el contenedor
IS_IN_CONTAINER = os.path.exists("/opt/airflow/otl")
BASE_PATH = "/opt/airflow/otl" if IS_IN_CONTAINER else os.path.join(os.path.dirname(os.path.dirname(__file__)), 'otl')

ILI2DB_JAR_PATH = os.path.join(BASE_PATH, "libs/ili2pg-5.1.0.jar")
EPSG_SCRIPT = os.path.join(BASE_PATH, "scripts/insert_ctm12_pg.sql")

def get_dynamic_config(dag_id: str):
    if dag_id == "etl_rfpp_xtf":
        model_dir_name = "modelos/modelo_rfpp"
        etl_dir_name = "etl/etl_rfpp"
    elif dag_id == "etl_rl2_xtf":
        model_dir_name = "modelos/modelo_rl2"
        etl_dir_name = "etl/etl_rl2"
    elif dag_id == "etl_prm_xtf":
        model_dir_name = "modelos/modelo_prm"
        etl_dir_name = "etl/etl_prm"
    elif dag_id == "etl_ap_xtf":
        model_dir_name = "modelos/modelo_ap"
        etl_dir_name = "etl/etl_ap"
    elif dag_id == "etl_hmdr_xtf":
        model_dir_name = "modelos/modelo_hmdr"
        etl_dir_name = "etl/etl_hmdr"
    elif dag_id == "etl_rfpn_xtf":
        model_dir_name = "modelos/modelo_rfpn"
        etl_dir_name = "etl/etl_rfpn"
    else:
        raise Exception("DAG_ID desconocido: " + dag_id)

    model_dir = os.path.join(BASE_PATH, model_dir_name)
    etl_dir = os.path.join(BASE_PATH, etl_dir_name)

    return {
        "MODEL_DIR": model_dir,
        "ETL_DIR": etl_dir,
        "CONFIG_PATH": os.path.join(etl_dir, 'config.json'),
        "TEMP_FOLDER": os.path.join(etl_dir, 'temp'),
        "XTF_DIR": os.path.join(BASE_PATH, 'output/xtf'),
        "GX_DIR": os.path.join(BASE_PATH, etl_dir, "gx"),
        "ILI2DB_JAR_PATH": ILI2DB_JAR_PATH,
        "EPSG_SCRIPT": EPSG_SCRIPT,
    }