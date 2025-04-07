import os

BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'OTL')

ILI2DB_JAR_PATH = os.path.join(BASE_PATH, "LIBS/ili2pg-5.1.0.jar")

MODEL_DIR = os.path.join(BASE_PATH, "Modelo_Reservas_Ley_2/MODELO")

ETL_DIR = os.path.join(BASE_PATH, 'ETL_RL2')
CONFIG_PATH = os.path.join(ETL_DIR, 'Config.json')
TEMP_FOLDER = os.path.join(ETL_DIR, 'temp')
EPSG_SCRIPT = os.path.join(BASE_PATH, "scripts/insert_ctm12_pg.sql")
XTF_DIR = os.path.join(ETL_DIR, 'XTF')

GX_DIR = os.path.join(BASE_PATH, ETL_DIR, "gx")