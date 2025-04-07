import os
import unittest
import shutil
from unittest.mock import patch

from config.general_config import TEMP_FOLDER, CONFIG_PATH
from utils.rl2.utils import (
    leer_configuracion,
    limpiar_carpeta_temporal,
    clean_sql_script
)


class TestUtils(unittest.TestCase):
    def setUp(self):
        # Aseguramos que TEMP_FOLDER exista antes de cada test
        os.makedirs(TEMP_FOLDER, exist_ok=True)
        super().setUp()

    def tearDown(self):
        # Limpia o elimina TEMP_FOLDER tras cada test.
        if os.path.exists(TEMP_FOLDER):
            shutil.rmtree(TEMP_FOLDER)
        super().tearDown()

    # --------------------------------------------------------------------------------------
    #                               TESTS DE leer_configuracion
    # --------------------------------------------------------------------------------------
    def test_cargar_expectativas_desde_yaml(self):
        """
        Test que valida la lectura de la configuración real,
        comparándola con los datos esperados (sin la sección 'db').
        """
        # Config esperada (sin la sección 'db')
        test_config = {
            "insumos_web": {
                "area_reserva_ley2": "https://www.arcgis.com/sharing/rest/content/items/988bc78e62c64f8c8b0b8klioioio5fbac83c478/data",
                "compensacion_ley2": "https://www.arcgis.com/sharing/rest/content/items/75a38dc85b6bgbggbgb64c3ead307b65b21e8900/data",
                "sustracciones_temporales_ley2": "https://www.arcgis.com/sharing/rest/content/items/2216e81419ab4cc3a1f41ca7950de09a/data",
                "sustracciones_definitivas_ley2": "https://www.arcgis.com/sharing/rest/content/items/25749923a366465cbf829269a93472ad/data",
                "zonificacion_ley2": "https://www.arcgis.com/sharing/rest/content/items/3d2b7c951c494feab3549410f592512b/data"
            },
            "insumos_local": {
                "area_reserva_ley2": "/Insumos/Area Reserva/Reservas_ley2_Noviembre_2023.zip",
                "compensacion_ley2": "/Insumos/Compensación/COMP_SUSTR_RESERVAS_20240815.zip",
                "sustracciones_temporales_ley2": "/Insumos/Sustracción/Sustracciones_Temporales_Ley2_septiembre_2021.zip",
                "sustracciones_definitivas_ley2": "/Insumos/Sustracción/Sustracciones_definitivas_Ley2_Septiembre_2023.zip",
                "zonificacion_ley2": "/Insumos/Zonificación/Zonificacion_Ley2_Septiembre_2023.zip"
            }
        }

        # Leemos la configuración real:
        config = leer_configuracion()
        # Removemos la clave 'db' para equiparar con test_config
        config.pop('db', None)
        self.assertEqual(config, test_config)

