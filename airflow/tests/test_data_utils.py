#!/usr/bin/env python
import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import sys
import tempfile
import shutil
import zipfile
import subprocess
import pandas as pd
import json

# Agregamos al path la ubicación del módulo a probar
sys.path.append('/opt/cpi/test/ETL_RL2/airflow/utils')

import data_utils

class TestDataUtils(unittest.TestCase):
    def setUp(self):
        # Creamos un directorio temporal para simular ETL_DIR y TEMP_FOLDER
        self.temp_dir = tempfile.mkdtemp()
        # Incluimos CONFIG_PATH para evitar KeyError
        self.cfg = {
            "ETL_DIR": self.temp_dir,
            "TEMP_FOLDER": os.path.join(self.temp_dir, "temp"),
            "CONFIG_PATH": os.path.join(self.temp_dir, "Config.json")
        }
        os.makedirs(self.cfg["TEMP_FOLDER"], exist_ok=True)
        # Creamos un archivo JSON dummy para CONFIG_PATH con contenido mínimo
        # Nota: el contenido se sobreescribirá en cada prueba que parchee leer_configuracion
        dummy_config = {"insumos_web": {}, "insumos_local": {}, "db": {}}
        with open(self.cfg["CONFIG_PATH"], "w") as f:
            json.dump(dummy_config, f)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    # -------------------------------------------------------------------------
    # Pruebas para obtener_insumos_desde_web
    # -------------------------------------------------------------------------
    @patch("data_utils.limpiar_carpeta_temporal")
    @patch("data_utils.leer_configuracion")
    def test_obtener_insumos_desde_web_no_insumos(self, mock_leer_config, mock_limpiar):
        # Simulamos que la configuración no tiene la clave "insumos_web"
        mock_leer_config.return_value = {}
        fake_ti = MagicMock()
        context = {"ti": fake_ti}

        with self.assertRaises(ValueError):
            data_utils.obtener_insumos_desde_web(self.cfg, **context)

    @patch("data_utils.requests.get")
    @patch("data_utils.limpiar_carpeta_temporal")
    @patch("data_utils.leer_configuracion")
    def test_obtener_insumos_desde_web_success(self, mock_leer_config, mock_limpiar, mock_requests_get):
        # Configuración con insumos_web y respaldo insumos_local
        insumos_web = {"test": "http://example.com/test.zip"}
        insumos_local = {"test": "local/test.zip"}
        mock_leer_config.return_value = {
            "insumos_web": insumos_web,
            "insumos_local": insumos_local,
            "db": {}
        }

        # Configuramos una respuesta falsa para requests.get
        fake_response = MagicMock()
        fake_response.iter_content = lambda chunk_size: [b"data"]
        fake_response.raise_for_status = lambda: None
        mock_requests_get.return_value = fake_response

        fake_ti = MagicMock()
        context = {"ti": fake_ti}
        # Parcheamos os.path.getsize para forzar que el archivo tenga tamaño > 0
        with patch("os.path.getsize", return_value=10):
            resultado = data_utils.obtener_insumos_desde_web(self.cfg, **context)
        self.assertIn("test", resultado)
        self.assertIsNotNone(resultado["test"])
        # Verificamos que se hayan empujado (xcom_push) los valores correctos
        fake_ti.xcom_push.assert_any_call(key="errores", value=[])
        fake_ti.xcom_push.assert_any_call(key="insumos_web", value=resultado)

    # -------------------------------------------------------------------------
    # Pruebas para _validar_archivo_local y copia_insumo_local
    # -------------------------------------------------------------------------
    def test_validar_archivo_local_success(self):
        key = "test"
        # Suponemos que en la configuración el path es relativo (se elimina la barra inicial)
        insumos_local = {"test": "/dummy/test.zip"}
        base_local = self.temp_dir
        # Simulamos la existencia del archivo
        local_dir = os.path.join(base_local, "dummy")
        os.makedirs(local_dir, exist_ok=True)
        full_path = os.path.join(local_dir, "test.zip")
        with open(full_path, "w") as f:
            f.write("contenido")
        resultado = data_utils._validar_archivo_local(key, insumos_local, base_local)
        self.assertEqual(resultado, full_path)

    def test_validar_archivo_local_no_entry(self):
        key = "not_exist"
        insumos_local = {"test": "/dummy/test.zip"}
        base_local = self.temp_dir
        with self.assertRaises(FileNotFoundError):
            data_utils._validar_archivo_local(key, insumos_local, base_local)

    def test_validar_archivo_local_file_missing(self):
        key = "test"
        insumos_local = {"test": "/dummy/missing.zip"}
        base_local = self.temp_dir
        with self.assertRaises(FileNotFoundError):
            data_utils._validar_archivo_local(key, insumos_local, base_local)

    @patch("data_utils.copia_insumo_local")
    def test_ejecutar_copia_insumo_local(self, mock_copia):
        fake_ti = MagicMock()
        context = {"ti": fake_ti}
        # Para simular un respaldo local existente, incluimos la entrada con ruta relativa;
        # además, para evitar que _validar_archivo_local se ejecute, la función copia_insumo_local se parchea.
        error_entry = {
            "url": "http://example.com",
            "key": "test",
            "insumos_local": {"test": "dummy/test.zip"},
            "base_local": self.temp_dir,
            "error": "error de descarga",
        }
        fake_ti.xcom_pull.return_value = [error_entry]
        # Forzamos que copia_insumo_local (ya parcheada) retorne una ruta simulada
        mock_copia.return_value = "/dummy/test.zip"
        data_utils.ejecutar_copia_insumo_local(**context)
        fake_ti.xcom_push.assert_called_with(key="insumos_local", value={"test": "/dummy/test.zip"})

    # -------------------------------------------------------------------------
    # Pruebas para procesar_insumos_descargados
    # -------------------------------------------------------------------------
    def test_procesar_insumos_descargados_zip_success(self):
        # Creamos un archivo ZIP dummy en TEMP_FOLDER
        temp_folder = self.cfg["TEMP_FOLDER"]
        dummy_zip_path = os.path.join(temp_folder, "test.zip")
        with zipfile.ZipFile(dummy_zip_path, 'w') as zipf:
            zipf.writestr("dummy.txt", "contenido dummy")
        # Simulamos que la tarea "Obtener_Insumos_Web" devuelve { "test": dummy_zip_path }
        fake_ti = MagicMock()
        fake_ti.xcom_pull.side_effect = [
            {"test": dummy_zip_path},  # Para insumos_web
            {}                         # Para insumos_local
        ]
        context = {"ti": fake_ti}
        resultado = data_utils.procesar_insumos_descargados(self.cfg, **context)
        self.assertTrue(isinstance(resultado, list))
        self.assertEqual(resultado[0]["key"], "test")
        extract_folder = resultado[0]["folder"]
        # Verificamos que la carpeta de extracción se haya creado y que contenga el archivo extraído
        self.assertTrue(os.path.exists(extract_folder))
        self.assertIn("dummy.txt", os.listdir(extract_folder))

    # -------------------------------------------------------------------------
    # Pruebas para _buscar_archivos_en_carpeta
    # -------------------------------------------------------------------------
    def test_buscar_archivos_en_carpeta(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            file1 = os.path.join(tmp_dir, "archivo.shp")
            file2 = os.path.join(tmp_dir, "archivo.txt")
            with open(file1, "w") as f:
                f.write("contenido")
            with open(file2, "w") as f:
                f.write("contenido")
            encontrados = data_utils._buscar_archivos_en_carpeta(tmp_dir, [".shp"])
            self.assertIn(file1, encontrados)
            self.assertNotIn(file2, encontrados)

    # -------------------------------------------------------------------------
    # Pruebas para _importar_shp_a_postgres y _importar_geojson_a_postgres
    # -------------------------------------------------------------------------
    @patch("data_utils.subprocess.run")
    def test_importar_shp_a_postgres_success(self, mock_run):
        db_config = {
            "host": "localhost",
            "port": "5432",
            "db_name": "testdb",
            "user": "user",
            "password": "pass",
        }
        data_utils._importar_shp_a_postgres(db_config, "dummy.shp", "insumos.test")
        mock_run.assert_called()

    @patch("data_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd", stderr="error"))
    def test_importar_shp_a_postgres_failure(self, mock_run):
        db_config = {
            "host": "localhost",
            "port": "5432",
            "db_name": "testdb",
            "user": "user",
            "password": "pass",
        }
        with self.assertRaises(Exception) as context_exc:
            data_utils._importar_shp_a_postgres(db_config, "dummy.shp", "insumos.test")
        self.assertIn("error", str(context_exc.exception))

    @patch("data_utils.subprocess.run")
    def test_importar_geojson_a_postgres_success(self, mock_run):
        db_config = {
            "host": "localhost",
            "port": "5432",
            "db_name": "testdb",
            "user": "user",
            "password": "pass",
        }
        data_utils._importar_geojson_a_postgres(db_config, "dummy.geojson", "insumos.test")
        mock_run.assert_called()

    @patch("data_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd", stderr="geo error"))
    def test_importar_geojson_a_postgres_failure(self, mock_run):
        db_config = {
            "host": "localhost",
            "port": "5432",
            "db_name": "testdb",
            "user": "user",
            "password": "pass",
        }
        with self.assertRaises(Exception) as context_exc:
            data_utils._importar_geojson_a_postgres(db_config, "dummy.geojson", "insumos.test")
        self.assertIn("geo error", str(context_exc.exception))

    # -------------------------------------------------------------------------
    # Pruebas para _importar_excel_a_postgres
    # -------------------------------------------------------------------------
    @patch("data_utils.pd.read_excel")
    @patch("data_utils.pd.DataFrame.to_sql")
    def test_importar_excel_a_postgres_success(self, mock_to_sql, mock_read_excel):
        df_dummy = pd.DataFrame({"col": [1, 2, 3]})
        mock_read_excel.return_value = df_dummy
        dummy_engine = MagicMock()
        data_utils._importar_excel_a_postgres(dummy_engine, "dummy.xlsx", "test_table")
        mock_read_excel.assert_called_with("dummy.xlsx")
        mock_to_sql.assert_called_with(
            name="test_table",
            con=dummy_engine,
            schema="insumos",
            if_exists="replace",
            index=False,
        )

    @patch("data_utils.pd.read_excel", side_effect=Exception("read error"))
    def test_importar_excel_a_postgres_failure(self, mock_read_excel):
        dummy_engine = MagicMock()
        with self.assertRaises(Exception) as context_exc:
            data_utils._importar_excel_a_postgres(dummy_engine, "dummy.xlsx", "test_table")
        self.assertIn("read error", str(context_exc.exception))

    # -------------------------------------------------------------------------
    # Prueba para ejecutar_importacion_general_a_postgres cuando no se extraen insumos válidos
    # -------------------------------------------------------------------------
    def test_ejecutar_importacion_general_a_postgres_no_insumos(self):
        fake_ti = MagicMock()
        fake_ti.xcom_pull.return_value = None  # Simulamos que no se recuperó información
        with self.assertRaises(Exception) as context_exc:
            data_utils.ejecutar_importacion_general_a_postgres(self.cfg, ti=fake_ti)
        self.assertIn("No se encontró información válida de insumos", str(context_exc.exception))


if __name__ == "__main__":
    unittest.main()