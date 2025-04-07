import os
import subprocess
import unittest
from unittest.mock import patch, MagicMock
import psycopg2  # Solo si se requiere para alguna comparación, sino se omite

from utils.interlis_utils import (
    exportar_datos_ladm_rl2,
    importar_esquema_ladm_rl2
)

# Configuración y constantes ficticias para los tests
fake_config = {
    "db": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "db_name": "test_db"
    }
}
fake_MODEL_DIR = "/fake/model"
fake_XTF_DIR = "/fake/xtf"
fake_ILI2DB_JAR_PATH = "/fake/ili2db.jar"
fake_EPSG_SCRIPT = "/fake/epsg.sh"

# ---------------------------
# Tests para exportar_datos_ladm_rl2
# ---------------------------
class TestExportarDatosLadmRL2(unittest.TestCase):

    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_config)
    @patch("os.path.exists")
    @patch("os.makedirs")
    @patch("utils.interlis_utils.subprocess.run")
    @patch("utils.interlis_utils.MODEL_DIR", fake_MODEL_DIR)
    @patch("utils.interlis_utils.XTF_DIR", fake_XTF_DIR)
    @patch("utils.interlis_utils.ILI2DB_JAR_PATH", fake_ILI2DB_JAR_PATH)
    def test_exportar_datos_success(self, mock_run, mock_makedirs, mock_exists, mock_leer_config):
        # Configurar os.path.exists:
        # - Para el directorio XTF: simular que no existe (retorna False)
        # - Para cualquier otro path (como xtf_path) se puede dejar que retorne True
        def exists_side_effect(path):
            if path == fake_XTF_DIR:
                return False
            return True
        mock_exists.side_effect = exists_side_effect

        # Simular que subprocess.run retorna un objeto con stderr
        fake_result = MagicMock()
        fake_result.stderr = "Exportación completada sin advertencias"
        mock_run.return_value = fake_result

        # Ejecutar la función
        exportar_datos_ladm_rl2()

        # Verificar que se haya creado el directorio XTF
        mock_makedirs.assert_called_once_with(fake_XTF_DIR)
        # Se construye el path esperado para el archivo XTF
        expected_xtf_path = os.path.join(fake_XTF_DIR, "rl2.xtf")
        # Se verifica que subprocess.run fue llamado con el comando esperado
        # Se comprueba que algunos parámetros críticos se encuentran en el comando
        args, kwargs = mock_run.call_args
        command = args[0]
        self.assertIn("java", command)
        self.assertIn("-jar", command)
        self.assertIn(fake_ILI2DB_JAR_PATH, command)
        self.assertIn("--dbhost", command)
        self.assertIn(fake_config["db"]["host"], command)
        self.assertIn(expected_xtf_path, command)
        # Verificar que se usen los flags de captura de salida, texto y check=True
        self.assertTrue(kwargs.get("capture_output"))
        self.assertTrue(kwargs.get("text"))
        self.assertTrue(kwargs.get("check"))

    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_config)
    @patch("os.path.exists", return_value=True)
    @patch("os.makedirs")
    @patch("utils.interlis_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, "java", "Error de ejecución"))
    @patch("utils.interlis_utils.MODEL_DIR", fake_MODEL_DIR)
    @patch("utils.interlis_utils.XTF_DIR", fake_XTF_DIR)
    @patch("utils.interlis_utils.ILI2DB_JAR_PATH", fake_ILI2DB_JAR_PATH)
    def test_exportar_datos_failure(self, mock_run, mock_makedirs, mock_exists, mock_leer_config):
        with self.assertRaises(Exception) as context:
            exportar_datos_ladm_rl2()
        self.assertIn("Error exportando XTF", str(context.exception))


# ---------------------------
# Tests para importar_esquema_ladm_rl2
# ---------------------------
class TestImportarEsquemaLadmRL2(unittest.TestCase):

    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_config)
    @patch("os.path.exists")
    @patch("utils.interlis_utils.subprocess.run")
    @patch("utils.interlis_utils.MODEL_DIR", fake_MODEL_DIR)
    @patch("utils.interlis_utils.ILI2DB_JAR_PATH", fake_ILI2DB_JAR_PATH)
    @patch("utils.interlis_utils.EPSG_SCRIPT", fake_EPSG_SCRIPT)
    def test_importar_esquema_success(self, mock_run, mock_exists, mock_leer_config):
        # Simular que el archivo JAR existe
        mock_exists.return_value = True

        # Ejecutar la función
        importar_esquema_ladm_rl2()

        # Verificar que se llamó a subprocess.run con el comando que incluye:
        # - Parámetros de idioma y país
        # - El flag --schemaimport
        # - La presencia del preScript EPSG_SCRIPT
        args, kwargs = mock_run.call_args
        command = args[0]
        self.assertIn("java", command)
        self.assertIn("-jar", command)
        self.assertIn(fake_ILI2DB_JAR_PATH, command)
        self.assertIn("--schemaimport", command)
        self.assertIn("--preScript", command)
        self.assertIn(fake_EPSG_SCRIPT, command)
        # Se verifica que check=True fue usado
        self.assertTrue(kwargs.get("check"))

    @patch("utils.interlis_utils.ILI2DB_JAR_PATH", fake_ILI2DB_JAR_PATH)
    @patch("os.path.exists", return_value=False)
    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_config)
    def test_importar_esquema_jar_no_existe(self, mock_leer_config, mock_exists):
        with self.assertRaises(FileNotFoundError) as context:
            importar_esquema_ladm_rl2()
        self.assertIn("Archivo JAR no encontrado", str(context.exception))


    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_config)
    @patch("os.path.exists", return_value=True)
    @patch("utils.interlis_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, "java", "Error en importación"))
    @patch("utils.interlis_utils.MODEL_DIR", fake_MODEL_DIR)
    @patch("utils.interlis_utils.ILI2DB_JAR_PATH", fake_ILI2DB_JAR_PATH)
    @patch("utils.interlis_utils.EPSG_SCRIPT", fake_EPSG_SCRIPT)
    def test_importar_esquema_failure(self, mock_run, mock_exists, mock_leer_config):
        with self.assertRaises(Exception) as context:
            importar_esquema_ladm_rl2()
        self.assertIn("Error importando LADM-RL2", str(context.exception))


if __name__ == '__main__':
    # Configurar logging para mostrar información durante los tests si es necesario
    import logging
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()