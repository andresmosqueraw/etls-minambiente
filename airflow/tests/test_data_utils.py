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
import re

# Agregamos al path la ubicación del módulo a probar
import utils.data_utils

class TestDataUtils(unittest.TestCase):
    def setUp(self):
        # Creamos un directorio temporal para simular ETL_DIR y TEMP_FOLDER
        self.temp_dir = tempfile.mkdtemp()
        self.cfg = {
            "ETL_DIR": self.temp_dir,
            "TEMP_FOLDER": os.path.join(self.temp_dir, "temp"),
            "CONFIG_PATH": os.path.join(self.temp_dir, "config.json")
        }
        os.makedirs(self.cfg["TEMP_FOLDER"], exist_ok=True)
        # Se crea un archivo JSON dummy para CONFIG_PATH
        dummy_config = {"insumos_web": {}, "insumos_local": {}, "db": {}}
        with open(self.cfg["CONFIG_PATH"], "w") as f:
            json.dump(dummy_config, f)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    # -----------------------------
    # Pruebas existentes ya implementadas
    # -----------------------------
    @patch("utils.data_utils.limpiar_carpeta_temporal")
    @patch("utils.data_utils.leer_configuracion")
    def test_obtener_insumos_desde_web_no_insumos(self, mock_leer_config, mock_limpiar):
        mock_leer_config.return_value = {}
        fake_ti = MagicMock()
        context = {"ti": fake_ti}
        with self.assertRaises(ValueError):
            utils.data_utils.obtener_insumos_desde_web(self.cfg, **context)

    @patch("utils.data_utils.requests.get")
    @patch("utils.data_utils.limpiar_carpeta_temporal")
    @patch("utils.data_utils.leer_configuracion")
    def test_obtener_insumos_desde_web_success(self, mock_leer_config, mock_limpiar, mock_requests_get):
        insumos_web = {"test": "http://example.com/test.zip"}
        insumos_local = {"test": "local/test.zip"}
        mock_leer_config.return_value = {
            "insumos_web": insumos_web,
            "insumos_local": insumos_local,
            "db": {}
        }
        fake_response = MagicMock()
        fake_response.iter_content = lambda chunk_size: [b"data"]
        fake_response.raise_for_status = lambda: None
        mock_requests_get.return_value = fake_response
        fake_ti = MagicMock()
        context = {"ti": fake_ti}
        with patch("os.path.getsize", return_value=10):
            resultado = utils.data_utils.obtener_insumos_desde_web(self.cfg, **context)
        self.assertIn("test", resultado)
        self.assertIsNotNone(resultado["test"])
        fake_ti.xcom_push.assert_any_call(key="errores", value=[])
        fake_ti.xcom_push.assert_any_call(key="insumos_web", value=resultado)

    def test_validar_archivo_local_success(self):
        key = "test"
        insumos_local = {"test": "/dummy/test.zip"}
        base_local = self.temp_dir
        local_dir = os.path.join(base_local, "dummy")
        os.makedirs(local_dir, exist_ok=True)
        full_path = os.path.join(local_dir, "test.zip")
        with open(full_path, "w") as f:
            f.write("contenido")
        resultado = utils.data_utils._validar_archivo_local(key, insumos_local, base_local)
        self.assertEqual(resultado, full_path)

    def test_validar_archivo_local_no_entry(self):
        key = "not_exist"
        insumos_local = {"test": "/dummy/test.zip"}
        base_local = self.temp_dir
        with self.assertRaises(FileNotFoundError):
            utils.data_utils._validar_archivo_local(key, insumos_local, base_local)

    def test_validar_archivo_local_file_missing(self):
        key = "test"
        insumos_local = {"test": "/dummy/missing.zip"}
        base_local = self.temp_dir
        with self.assertRaises(FileNotFoundError):
            utils.data_utils._validar_archivo_local(key, insumos_local, base_local)

    @patch("utils.data_utils.copia_insumo_local")
    def test_ejecutar_copia_insumo_local(self, mock_copia):
        fake_ti = MagicMock()
        context = {"ti": fake_ti}
        error_entry = {
            "url": "http://example.com",
            "key": "test",
            "insumos_local": {"test": "dummy/test.zip"},
            "base_local": self.temp_dir,
            "error": "error de descarga",
        }
        fake_ti.xcom_pull.return_value = [error_entry]
        mock_copia.return_value = "/dummy/test.zip"
        utils.data_utils.ejecutar_copia_insumo_local(**context)
        fake_ti.xcom_push.assert_called_with(key="insumos_local", value={"test": "/dummy/test.zip"})

    def test_procesar_insumos_descargados_zip_success(self):
        temp_folder = self.cfg["TEMP_FOLDER"]
        dummy_zip_path = os.path.join(temp_folder, "test.zip")
        with zipfile.ZipFile(dummy_zip_path, 'w') as zipf:
            zipf.writestr("dummy.txt", "contenido dummy")
        fake_ti = MagicMock()
        fake_ti.xcom_pull.side_effect = [
            {"test": dummy_zip_path},
            {}
        ]
        context = {"ti": fake_ti}
        resultado = utils.data_utils.procesar_insumos_descargados(self.cfg, **context)
        self.assertTrue(isinstance(resultado, list))
        self.assertEqual(resultado[0]["key"], "test")
        extract_folder = resultado[0]["folder"]
        self.assertTrue(os.path.exists(extract_folder))
        self.assertIn("dummy.txt", os.listdir(extract_folder))

    def test_buscar_archivos_en_carpeta(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            file1 = os.path.join(tmp_dir, "archivo.shp")
            file2 = os.path.join(tmp_dir, "archivo.txt")
            with open(file1, "w") as f:
                f.write("contenido")
            with open(file2, "w") as f:
                f.write("contenido")
            encontrados = utils.data_utils._buscar_archivos_en_carpeta(tmp_dir, [".shp"])
            self.assertIn(file1, encontrados)
            self.assertNotIn(file2, encontrados)

    @patch("utils.data_utils.subprocess.run")
    def test_importar_shp_a_postgres_success(self, mock_run):
        db_config = {"host": "localhost", "port": "5432", "db_name": "testdb", "user": "user", "password": "pass"}
        utils.data_utils._importar_shp_a_postgres(db_config, "dummy.shp", "insumos.test")
        mock_run.assert_called()

    @patch("utils.data_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd", stderr="error"))
    def test_importar_shp_a_postgres_failure(self, mock_run):
        db_config = {"host": "localhost", "port": "5432", "db_name": "testdb", "user": "user", "password": "pass"}
        with self.assertRaises(Exception) as context_exc:
            utils.data_utils._importar_shp_a_postgres(db_config, "dummy.shp", "insumos.test")
        self.assertIn("error", str(context_exc.exception))

    @patch("utils.data_utils.subprocess.run")
    def test_importar_geojson_a_postgres_success(self, mock_run):
        db_config = {"host": "localhost", "port": "5432", "db_name": "testdb", "user": "user", "password": "pass"}
        utils.data_utils._importar_geojson_a_postgres(db_config, "dummy.geojson", "insumos.test")
        mock_run.assert_called()

    @patch("utils.data_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd", stderr="geo error"))
    def test_importar_geojson_a_postgres_failure(self, mock_run):
        db_config = {"host": "localhost", "port": "5432", "db_name": "testdb", "user": "user", "password": "pass"}
        with self.assertRaises(Exception) as context_exc:
            utils.data_utils._importar_geojson_a_postgres(db_config, "dummy.geojson", "insumos.test")
        self.assertIn("geo error", str(context_exc.exception))

    @patch("utils.data_utils.pd.read_excel")
    @patch("utils.data_utils.pd.DataFrame.to_sql")
    def test_importar_excel_a_postgres_success(self, mock_to_sql, mock_read_excel):
        df_dummy = pd.DataFrame({"col": [1, 2, 3]})
        mock_read_excel.return_value = df_dummy
        dummy_engine = MagicMock()
        utils.data_utils._importar_excel_a_postgres(self.cfg, dummy_engine, "dummy.xlsx", "test_table")
        mock_read_excel.assert_called_with("dummy.xlsx")
        mock_to_sql.assert_called_with(
            name="test_table",
            con=dummy_engine,
            schema="insumos",
            if_exists="replace",
            index=False,
        )

    @patch("utils.data_utils.pd.read_excel", side_effect=Exception("read error"))
    def test_importar_excel_a_postgres_failure(self, mock_read_excel):
        dummy_engine = MagicMock()
        with self.assertRaises(Exception) as context_exc:
            utils.data_utils._importar_excel_a_postgres(self.cfg, dummy_engine, "dummy.xlsx", "test_table")
        self.assertIn("read error", str(context_exc.exception))

    def test_ejecutar_importacion_general_a_postgres_no_insumos(self):
        fake_ti = MagicMock()
        fake_ti.xcom_pull.return_value = None
        with self.assertRaises(Exception) as context_exc:
            utils.data_utils.ejecutar_importacion_general_a_postgres(self.cfg, ti=fake_ti)
        self.assertIn("No se encontró información válida de insumos", str(context_exc.exception))

    # -----------------------------
    # NUEVAS PRUEBAS PARA CUBRIR FUNCIONES ADICIONALES
    # -----------------------------
    def test_descargar_archivo_success(self):
        dest_file = os.path.join(self.temp_dir, "downloaded_file.txt")
        fake_response = MagicMock()
        fake_response.iter_content = lambda chunk_size: [b"test data"]
        fake_response.raise_for_status = lambda: None
        with patch("utils.data_utils.requests.get", return_value=fake_response) as mock_get:
            resultado = utils.data_utils.descargar_archivo("http://dummy", dest_file)
        self.assertEqual(resultado, dest_file)
        with open(dest_file, "rb") as f:
            self.assertEqual(f.read(), b"test data")

    def test_obtener_ruta_local_success(self):
        file_path = os.path.join(self.temp_dir, "dummy.txt")
        with open(file_path, "w") as f:
            f.write("content")
        ruta = utils.data_utils.obtener_ruta_local(self.temp_dir, "/dummy.txt")
        self.assertEqual(ruta, file_path)

    def test_obtener_ruta_local_failure(self):
        with self.assertRaises(FileNotFoundError):
            utils.data_utils.obtener_ruta_local(self.temp_dir, "/nonexistent.txt")

    def test_ejecutar_ogr2ogr_success(self):
        command = ["echo", "hello"]
        with patch("utils.data_utils.subprocess.run") as mock_run:
            utils.data_utils.ejecutar_ogr2ogr(command, "test error context")
            mock_run.assert_called_with(command, capture_output=True, text=True, check=True)

    def test_ejecutar_ogr2ogr_failure(self):
        command = ["bad", "command"]
        with patch("utils.data_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, command, stderr="failed")):
            with self.assertRaises(Exception) as cm:
                utils.data_utils.ejecutar_ogr2ogr(command, "test failure")
            self.assertIn("failed", str(cm.exception))

    def test__crear_error(self):
        error_dict = utils.data_utils._crear_error("http://dummy", "key1", {"key1": "path"}, "base", "error msg")
        expected = {
            "url": "http://dummy",
            "key": "key1",
            "insumos_local": {"key1": "path"},
            "base_local": "base",
            "error": "error msg"
        }
        self.assertEqual(error_dict, expected)

    def test_copia_insumo_local_success(self):
        dummy_dir = os.path.join(self.temp_dir, "dummy")
        os.makedirs(dummy_dir, exist_ok=True)
        file_path = os.path.join(dummy_dir, "test.zip")
        with open(file_path, "w") as f:
            f.write("content")
        insumos_local = {"test": "/dummy/test.zip"}
        resultado = utils.data_utils.copia_insumo_local("http://dummy", "test", insumos_local, self.temp_dir, "some error")
        self.assertEqual(resultado, file_path)

    def test_copia_insumo_local_failure(self):
        insumos_local = {"test": "/dummy/missing.zip"}
        with self.assertRaises(FileNotFoundError):
            utils.data_utils.copia_insumo_local("http://dummy", "test", insumos_local, self.temp_dir, "some error")

    def test__procesar_zip_excel_detected(self):
        extract_folder = os.path.join(self.temp_dir, "extract")
        os.makedirs(extract_folder, exist_ok=True)
        zip_file = os.path.join(self.temp_dir, "excel.zip")
        with zipfile.ZipFile(zip_file, 'w') as zipf:
            zipf.writestr("[Content_Types].xml", "dummy")
        resultado = utils.data_utils._procesar_zip("excel_key", zip_file, extract_folder)
        self.assertEqual(resultado["type"], "excel")
        self.assertTrue(resultado["file"].endswith(".xlsx"))

    def test__procesar_zip_zip_extraction(self):
        extract_folder = os.path.join(self.temp_dir, "extract2")
        os.makedirs(extract_folder, exist_ok=True)
        zip_file = os.path.join(self.temp_dir, "normal.zip")
        with zipfile.ZipFile(zip_file, 'w') as zipf:
            zipf.writestr("dummy.txt", "content")
        resultado = utils.data_utils._procesar_zip("normal_key", zip_file, extract_folder)
        self.assertEqual(resultado["type"], "zip")
        self.assertEqual(resultado["file"], zip_file)
        self.assertIn("dummy.txt", os.listdir(extract_folder))

    def test__procesar_zip_badzip(self):
        extract_folder = os.path.join(self.temp_dir, "extract3")
        os.makedirs(extract_folder, exist_ok=True)
        bad_zip = os.path.join(self.temp_dir, "bad.zip")
        with open(bad_zip, "w") as f:
            f.write("not a zip")
        resultado = utils.data_utils._procesar_zip("bad_key", bad_zip, extract_folder)
        self.assertEqual(resultado["type"], "geojson")
        self.assertTrue(resultado["file"].endswith(".geojson"))

    def test_importar_excel_a_postgres_etl_ap(self):
        cfg_ap = self.cfg.copy()
        cfg_ap["ETL_DIR"] = os.path.join(self.temp_dir, "etl/etl_ap")
        os.makedirs(cfg_ap["ETL_DIR"], exist_ok=True)
        engine = MagicMock()
        with patch("utils.data_utils.import_excel_to_db", return_value="ap result") as mock_import:
            resultado = utils.data_utils._importar_excel_a_postgres(cfg_ap, engine, "dummy.xlsx", "table_ap")
            mock_import.assert_called()
            self.assertEqual(resultado, "ap result")

    def test_importar_excel_a_postgres_etl_rfpn(self):
        cfg_rfpn = self.cfg.copy()
        cfg_rfpn["ETL_DIR"] = os.path.join(self.temp_dir, "etl/etl_rfpn")
        os.makedirs(cfg_rfpn["ETL_DIR"], exist_ok=True)
        engine = MagicMock()
        with patch("utils.data_utils.import_excel_to_db2", return_value="rfpn result") as mock_import:
            resultado = utils.data_utils._importar_excel_a_postgres(cfg_rfpn, engine, "dummy.xlsx", "informacion_area_reserva")
            mock_import.assert_called()
            self.assertEqual(resultado, "rfpn result")

    def test_shorten_identifier(self):
        resultado = utils.data_utils.shorten_identifier("Test-Identifier!")
        self.assertEqual(resultado, "test_identifier_")

    def test_crear_cruce_area_reserva_directo(self):
        with patch("utils.data_utils.ejecutar_sql") as mock_ejecutar_sql:
            utils.data_utils.crear_cruce_area_reserva_directo(self.cfg)
            self.assertTrue(mock_ejecutar_sql.called)
            calls = mock_ejecutar_sql.call_args_list
            self.assertIn("DROP TABLE IF EXISTS insumos.cruce_area_reserva", calls[0][0][1])

    @patch("utils.data_utils.pd.DataFrame.to_sql")
    @patch("utils.data_utils.pd.read_excel")
    def test_import_excel_to_db2_success(self, mock_read_excel, mock_to_sql):
        db_config = {"user": "u", "password": "p", "host": "h", "port": "5432", "db_name": "d"}
        dummy_df = pd.DataFrame({
            "Id del área protegida": [1, 2],
            "Objeto del acto": ["Declaratoria", "Other"],
            "Categoría de manejo": ["Reservas Forestales Protectoras Nacionales", "Other"],
            "Tipo de acto administrativo": ["Acuerdos", "Other"],
        })
        mock_read_excel.return_value = dummy_df
        # Con sheet_name se invoca la lectura de Excel y se invoca to_sql que ahora se patchéa.
        resultado = utils.data_utils.import_excel_to_db2(db_config, "dummy.xlsx", "table_test", sheet_name="Actos")
        mock_read_excel.assert_called_with("dummy.xlsx", sheet_name="Actos")
        # Verificamos que se llamó al método to_sql (el cual ahora está parcheado)
        self.assertTrue(mock_to_sql.called)


    def test_import_excel_to_db_success(self):
        db_config = {"user": "u", "password": "p", "host": "h", "port": "5432", "db_name": "d"}
        df_general = pd.DataFrame({"Id del área protegida": [1, 2], "A": [10, 20]})
        df_actos = pd.DataFrame({
            "Id del área protegida": [1, 2],
            "Objeto del acto": ["Declaratoria", "Other"],
            "Categoría de manejo": ["Reservas Forestales Protectoras Nacionales", "Other"],
            "Tipo de acto administrativo": ["Acuerdos", "Other"],
        })
        with patch("utils.data_utils.pd.read_excel", side_effect=[df_general, df_actos]) as mock_read_excel:
            with patch("utils.data_utils.sqlalchemy.create_engine") as mock_engine:
                engine_instance = MagicMock()
                mock_engine.return_value = engine_instance
                utils.data_utils.import_excel_to_db(db_config, "dummy.xlsx", "table_key")
                self.assertEqual(mock_read_excel.call_count, 2)

if __name__ == "__main__":
    unittest.main()