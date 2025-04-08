import os
import json
import shutil
import re
import unittest
from unittest.mock import patch, mock_open
import logging

# Importamos las funciones a testear
from utils.utils import leer_configuracion, limpiar_carpeta_temporal, clean_sql_script

# Constantes para las pruebas
FAKE_CONFIG_PATH = "dummy_config.json"
FAKE_TEMP_FOLDER = "temp_test"

# ---------------------------
# Pruebas para leer_configuracion
# ---------------------------
class TestLeerConfiguracion(unittest.TestCase):
    def setUp(self):
        # Cada test recibirá un diccionario de configuración con la clave CONFIG_PATH.
        self.cfg = {"CONFIG_PATH": FAKE_CONFIG_PATH}

    def test_leer_configuracion_success(self):
        fake_data = {"key": "value"}
        m = mock_open(read_data=json.dumps(fake_data))
        # Parchamos la función open en el módulo utils.utils para simular la lectura del archivo.
        with patch("utils.utils.open", m):
            result = leer_configuracion(self.cfg)
            self.assertEqual(result, fake_data)
    
    def test_leer_configuracion_file_not_found(self):
        # Simulamos que open lanza FileNotFoundError.
        with patch("utils.utils.open", side_effect=FileNotFoundError("No se encontró")):
            with self.assertRaises(FileNotFoundError) as context:
                leer_configuracion(self.cfg)
            self.assertIn("Archivo no encontrado", str(context.exception))
    
    def test_leer_configuracion_other_exception(self):
        # Simulamos otro error (por ejemplo, error al parsear)
        with patch("utils.utils.open", side_effect=Exception("Error inesperado")):
            with self.assertRaises(Exception) as context:
                leer_configuracion(self.cfg)
            self.assertIn("Error leyendo la configuración", str(context.exception))

# ---------------------------
# Pruebas para limpiar_carpeta_temporal
# ---------------------------
class TestLimpiarCarpetaTemporal(unittest.TestCase):
    def setUp(self):
        # Cada test recibirá un diccionario de configuración con la clave TEMP_FOLDER.
        self.cfg = {"TEMP_FOLDER": FAKE_TEMP_FOLDER}

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
            
            limpiar_carpeta_temporal(self.cfg)
            
            file_path = os.path.join(FAKE_TEMP_FOLDER, "file.txt")
            dir_path = os.path.join(FAKE_TEMP_FOLDER, "dir")
            mock_unlink.assert_called_once_with(file_path)
            mock_rmtree.assert_called_once_with(dir_path)
            # Verificamos que se cree la carpeta (con exist_ok=True)
            mock_makedirs.assert_called_once_with(FAKE_TEMP_FOLDER, exist_ok=True)

    def test_limpiar_carpeta_temporal_not_exists(self):
        # Simulamos que la carpeta no existe: se debe crear.
        with patch("utils.utils.os.path.exists", return_value=False) as mock_exists, \
             patch("utils.utils.os.makedirs") as mock_makedirs:
            limpiar_carpeta_temporal(self.cfg)
            mock_makedirs.assert_called_once_with(FAKE_TEMP_FOLDER, exist_ok=True)

    def test_limpiar_carpeta_temporal_error(self):
        # Simulamos un error al eliminar un archivo.
        with patch("utils.utils.os.path.exists", return_value=True) as mock_exists, \
             patch("utils.utils.os.listdir", return_value=["file.txt"]) as mock_listdir, \
             patch("utils.utils.os.path.isfile", return_value=True), \
             patch("utils.utils.os.path.islink", return_value=False), \
             patch("utils.utils.os.unlink", side_effect=Exception("Error al eliminar")), \
             patch("utils.utils.os.makedirs") as mock_makedirs:
            with self.assertRaises(Exception) as context:
                limpiar_carpeta_temporal(self.cfg)
            self.assertIn("Error eliminando", str(context.exception))

# ---------------------------
# Pruebas para clean_sql_script
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