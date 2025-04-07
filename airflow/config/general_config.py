# /opt/cpi/test/ETL_RL2/airflow/config/general_config.py
import os

# Rutas base fijas (iguales para ambos DAGs)
BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'OTL')
ILI2DB_JAR_PATH = os.path.join(BASE_PATH, "LIBS/ili2pg-5.1.0.jar")
EPSG_SCRIPT = os.path.join(BASE_PATH, "scripts/insert_ctm12_pg.sql")

def get_dynamic_config(dag_id: str):
    """
    Retorna un diccionario con las rutas dinámicas según el DAG.
    """
    if dag_id == "etl_rfpp_xtf":
        model_dir_name = "Modelo_Reservas_Forestales_Protectoras_Productoras/MODELO"
        etl_dir_name = "ETL_RFPP"
    elif dag_id == "etl_rl2_xtf":
        model_dir_name = "Modelo_Reservas_Ley_2/MODELO"
        etl_dir_name = "ETL_RL2"
    else:
        # Valor por defecto, si quisieras manejar otros DAGs
        model_dir_name = "Modelo_Reservas_Ley_2/MODELO"
        etl_dir_name = "ETL_RL2"

    model_dir = os.path.join(BASE_PATH, model_dir_name)
    etl_dir = os.path.join(BASE_PATH, etl_dir_name)

    return {
        "MODEL_DIR": model_dir,
        "ETL_DIR": etl_dir,
        "CONFIG_PATH": os.path.join(etl_dir, 'Config.json'),
        "TEMP_FOLDER": os.path.join(etl_dir, 'temp'),
        "XTF_DIR": os.path.join(etl_dir, 'XTF'),
        "GX_DIR": os.path.join(BASE_PATH, etl_dir, "gx"),
        "ILI2DB_JAR_PATH": ILI2DB_JAR_PATH,
        "EPSG_SCRIPT": EPSG_SCRIPT,
    }