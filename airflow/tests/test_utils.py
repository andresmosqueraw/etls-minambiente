import os
import json
import shutil
import re
import unittest
from unittest.mock import patch, mock_open
import logging

# Se asume que las funciones a testear se encuentran en un módulo, por ejemplo, "utils.utils.py"
from utils.utils import (
    leer_configuracion,
    limpiar_carpeta_temporal,
    clean_sql_script
)

# Definimos valores ficticios para los constantes importados desde config.general_config
FAKE_CONFIG_PATH = "dummy_config.json"
FAKE_TEMP_FOLDER = "temp_test"

# ---------------------------
# Tests para leer_configuracion
# ---------------------------
class TestLeerConfiguracion(unittest.TestCase):
    @patch("utils.utils.CONFIG_PATH", new=FAKE_CONFIG_PATH)
    def test_leer_configuracion_success(self):
        fake_data = {"key": "value"}
        m = mock_open(read_data=json.dumps(fake_data))
        with patch("utils.utils.open", m):
            result = leer_configuracion()
            self.assertEqual(result, fake_data)

    @patch("utils.utils.CONFIG_PATH", new=FAKE_CONFIG_PATH)
    def test_leer_configuracion_file_not_found(self):
        with patch("utils.utils.open", side_effect=FileNotFoundError("No se encontró")):
            with self.assertRaises(FileNotFoundError) as context:
                leer_configuracion()
            self.assertIn("Archivo no encontrado", str(context.exception))

    @patch("utils.utils.CONFIG_PATH", new=FAKE_CONFIG_PATH)
    def test_leer_configuracion_other_exception(self):
        with patch("utils.utils.open", side_effect=Exception("Error inesperado")):
            with self.assertRaises(Exception) as context:
                leer_configuracion()
            self.assertIn("Error leyendo la configuración", str(context.exception))

# ---------------------------
# Tests para limpiar_carpeta_temporal
# ---------------------------
class TestLimpiarCarpetaTemporal(unittest.TestCase):
    @patch("utils.utils.TEMP_FOLDER", new=FAKE_TEMP_FOLDER)
    def test_limpiar_carpeta_temporal_exists(self):
        # Simulamos que la carpeta existe y contiene un archivo y un directorio.
        with patch("utils.utils.os.path.exists", return_value=True) as mock_exists, \
             patch("utils.utils.os.listdir", return_value=["file.txt", "dir"]) as mock_listdir, \
             patch("utils.utils.os.path.isfile", side_effect=lambda path: "file.txt" in path), \
             patch("utils.utils.os.path.islink", return_value=False), \
             patch("utils.utils.os.path.isdir", side_effect=lambda path: "dir" in path), \
             patch("utils.utils.os.unlink") as mock_unlink, \
             patch("utils.utils.shutil.rmtree") as mock_rmtree, \
             patch("utils.utils.os.makedirs") as mock_makedirs:
            
            limpiar_carpeta_temporal()
            
            file_path = os.path.join(FAKE_TEMP_FOLDER, "file.txt")
            dir_path = os.path.join(FAKE_TEMP_FOLDER, "dir")
            mock_unlink.assert_called_once_with(file_path)
            mock_rmtree.assert_called_once_with(dir_path)
            mock_makedirs.assert_called_once_with(FAKE_TEMP_FOLDER, exist_ok=True)

    @patch("utils.utils.TEMP_FOLDER", new=FAKE_TEMP_FOLDER)
    def test_limpiar_carpeta_temporal_not_exists(self):
        # Simulamos que la carpeta no existe: se debe crear.
        with patch("utils.utils.os.path.exists", return_value=False) as mock_exists, \
             patch("utils.utils.os.makedirs") as mock_makedirs:
            limpiar_carpeta_temporal()
            mock_makedirs.assert_called_once_with(FAKE_TEMP_FOLDER, exist_ok=True)

    @patch("utils.utils.TEMP_FOLDER", new=FAKE_TEMP_FOLDER)
    def test_limpiar_carpeta_temporal_error(self):
        # Simulamos un error al eliminar un archivo (por ejemplo, en os.unlink).
        with patch("utils.utils.os.path.exists", return_value=True) as mock_exists, \
             patch("utils.utils.os.listdir", return_value=["file.txt"]) as mock_listdir, \
             patch("utils.utils.os.path.isfile", return_value=True), \
             patch("utils.utils.os.path.islink", return_value=False), \
             patch("utils.utils.os.unlink", side_effect=Exception("Error al eliminar")), \
             patch("utils.utils.os.makedirs") as mock_makedirs:
            with self.assertRaises(Exception) as context:
                limpiar_carpeta_temporal()
            self.assertIn("Error eliminando", str(context.exception))

# ---------------------------
# Tests para clean_sql_script
# ---------------------------
class TestCleanSqlScript(unittest.TestCase):
    def test_clean_sql_script_remove_balanced_comments(self):
        script = "SELECT * FROM table; /* This is a comment */ SELECT 1;"
        expected = "SELECT * FROM table;  SELECT 1;"
        result = clean_sql_script(script)
        self.assertEqual(result, expected)

    def test_clean_sql_script_unclosed_comment(self):
        script = "SELECT * FROM table; /* unclosed comment"
        expected = "SELECT * FROM table; "
        result = clean_sql_script(script)
        self.assertEqual(result, expected)

    def test_clean_sql_script_no_comment(self):
        script = "SELECT * FROM table;"
        result = clean_sql_script(script)
        self.assertEqual(result, script)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()