import os

# Rutas base fijas (iguales para ambos DAGs)
BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'OTL')
ILI2DB_JAR_PATH = os.path.join(BASE_PATH, "libs/ili2pg-5.1.0.jar")
EPSG_SCRIPT = os.path.join(BASE_PATH, "scripts/insert_ctm12_pg.sql")

def get_dynamic_config(dag_id: str):
    """
    Retorna un diccionario con las rutas dinámicas según el DAG.
    """
    if dag_id == "etl_rfpp_xtf":
        model_dir_name = "modelos/modelo_rfpp/Modelo_Reservas_Forestales_Protectoras_Productoras/MODELO"
        etl_dir_name = "etl/etl_rfpp"
    elif dag_id == "etl_rl2_xtf":
        model_dir_name = "modelos/modelo_rl2/Modelo_Reservas_Ley_2/MODELO"
        etl_dir_name = "etl/etl_rl2"
    elif dag_id == "etl_prm_xtf":
        model_dir_name = "modelos/modelo_prm/Modelo_Paramos/MODELO"
        etl_dir_name = "etl/etl_prm"
    elif dag_id == "etl_ap_xtf":
        model_dir_name = "modelos/modelo_ap/Modelo_Areas_Protegidas_SINAP/MODELO"
        etl_dir_name = "etl/etl_ap"
    elif dag_id == "etl_hmdr_xtf":
        model_dir_name = "modelos/modelo_hmdr/Modelo_Humedales_RAMSAR/MODELO"
        etl_dir_name = "etl/etl_hmdr"
    elif dag_id == "etl_rfpn_xtf":
        model_dir_name = "modelos/modelo_rfpn/Modelo_Reservas_Forestales_Protectoras_Nacionales/MODELO"
        etl_dir_name = "etl/etl_rfpn"
    else:
        raise Exception("DAG_ID desconocido: " + dag_id)

    model_dir = os.path.join(BASE_PATH, model_dir_name)
    etl_dir = os.path.join(BASE_PATH, etl_dir_name)

    return {
        "MODEL_DIR": model_dir,
        "ETL_DIR": etl_dir,
        "CONFIG_PATH": os.path.join(etl_dir, 'Config.json'),
        "TEMP_FOLDER": os.path.join(etl_dir, 'temp'),
        "XTF_DIR": os.path.join(BASE_PATH, 'output/xtf'),
        "GX_DIR": os.path.join(BASE_PATH, etl_dir, "gx"),
        "ILI2DB_JAR_PATH": ILI2DB_JAR_PATH,
        "EPSG_SCRIPT": EPSG_SCRIPT,
    }