#!/usr/bin/env python
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import sqlalchemy
import yaml
import logging

from utils.gx_utils import (
    _cargar_expectativas_desde_yaml,
    _obtener_tablas_esperadas,
    _obtener_engine_sqlalchemy,
    _obtener_tablas_esquema,
    _revisar_tablas_encontradas,
    _revisar_columnas_tabla,
    _comparar_columnas_exactas,
    _comparar_columnas_subconjunto
)

# =============================================================================
# Pruebas para _cargar_expectativas_desde_yaml
# =============================================================================
class TestCargarExpectativasDesdeYaml(unittest.TestCase):
    def setUp(self):
        # Creamos un directorio temporal para simular GX_DIR
        self.test_dir = tempfile.mkdtemp()
        self.cfg = {"GX_DIR": self.test_dir}
        self.yaml_filename = "test_expectations.yaml"
        self.yaml_path = os.path.join(self.test_dir, self.yaml_filename)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.test_dir)

    def test_success(self):
        # Contenido YAML válido con la clave "expectations"
        yaml_content = {
            "expectations": [
                {
                    "expectation_type": "expect_table_columns_to_match_set",
                    "meta": {"table": "my_table"},
                    "kwargs": {"column_set": ["col1", "col2"], "exact_match": True}
                }
            ]
        }
        with open(self.yaml_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_content, f)

        data = _cargar_expectativas_desde_yaml(self.yaml_filename, self.cfg)
        self.assertIn("expectations", data)
        self.assertEqual(data, yaml_content)

    def test_file_not_found(self):
        # Prueba que se lance FileNotFoundError si el archivo no existe
        with self.assertRaises(FileNotFoundError):
            _cargar_expectativas_desde_yaml("nonexistent.yaml", self.cfg)

    def test_missing_expectations_key(self):
        # Si el YAML existe pero no contiene "expectations"
        yaml_content = {"other_key": []}
        with open(self.yaml_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_content, f)
        with self.assertRaises(ValueError) as context:
            _cargar_expectativas_desde_yaml(self.yaml_filename, self.cfg)
        self.assertIn("no contiene 'expectations'", str(context.exception))


# =============================================================================
# Pruebas para _obtener_tablas_esperadas
# =============================================================================
class TestObtenerTablasEsperadas(unittest.TestCase):
    def test_obtener_tablas_esperadas(self):
        # Datos de ejemplo
        data = {
            "expectations": [
                {
                    "expectation_type": "expect_table_columns_to_match_set",
                    "meta": {"table": "table1"},
                    "kwargs": {"column_set": ["a", "b", "c"], "exact_match": True}
                },
                {
                    "expectation_type": "other_expectation",
                    "meta": {"table": "table2"},
                    "kwargs": {"column_set": ["x", "y"], "exact_match": False}
                },
                {
                    "expectation_type": "expect_table_columns_to_match_set",
                    "meta": {"table": "table3"},
                    "kwargs": {"column_set": ["col1"], "exact_match": False}
                }
            ]
        }
        expected = {
            "table1": {"expected_columns": set(["a", "b", "c"]), "exact_match": True},
            "table3": {"expected_columns": set(["col1"]), "exact_match": False}
        }
        result = _obtener_tablas_esperadas(data)
        self.assertEqual(result, expected)


# =============================================================================
# Pruebas para _obtener_engine_sqlalchemy
# =============================================================================
class TestObtenerEngineSQLAlchemy(unittest.TestCase):
    @patch("utils.gx_utils.leer_configuracion")
    @patch("utils.gx_utils.sqlalchemy.create_engine")
    def test_engine_success(self, mock_create_engine, mock_leer_config):
        fake_conf = {
            "db": {
                "host": "localhost",
                "port": 5432,
                "user": "user",
                "password": "pass",
                "db_name": "mydb"
            }
        }
        mock_leer_config.return_value = fake_conf
        fake_engine = MagicMock()
        mock_create_engine.return_value = fake_engine

        engine = _obtener_engine_sqlalchemy(fake_conf)
        expected_url = "postgresql://user:pass@localhost:5432/mydb"
        mock_create_engine.assert_called_once_with(expected_url)
        self.assertEqual(engine, fake_engine)


# =============================================================================
# Pruebas para _obtener_tablas_esquema
# =============================================================================
class TestObtenerTablasEsquema(unittest.TestCase):
    @patch("utils.gx_utils.pd.read_sql")
    def test_success(self, mock_read_sql):
        # Simular que read_sql retorna un DataFrame con una columna "table_name"
        df = pd.DataFrame({"table_name": ["table1", "table2"]})
        mock_read_sql.return_value = df
        fake_engine = MagicMock()
        schema = "public"
        result = _obtener_tablas_esquema(fake_engine, schema)
        self.assertEqual(result, set(["table1", "table2"]))
        # Verificar que la consulta SQL contiene "FROM information_schema.tables"
        sql_query = mock_read_sql.call_args[0][0]
        self.assertIn("FROM information_schema.tables", sql_query)

    @patch("utils.gx_utils.pd.read_sql", side_effect=Exception("DB error"))
    def test_failure(self, mock_read_sql):
        fake_engine = MagicMock()
        with self.assertRaises(Exception) as context:
            _obtener_tablas_esquema(fake_engine, "public")
        # Ahora se verifica que el mensaje de excepción contenga "DB error"
        self.assertIn("DB error", str(context.exception))


# =============================================================================
# Pruebas para _revisar_tablas_encontradas
# =============================================================================
class TestRevisarTablasEncontradas(unittest.TestCase):
    def test_revisar_tablas_encontradas(self):
        schema = "test_schema"
        expected_tables = {"table1": {}, "table2": {}}
        actual_tables = {"table1", "table3"}
        report = _revisar_tablas_encontradas(schema, expected_tables, actual_tables)
        # Verificar que se indique que "table1" se encontró y "table2" falta.
        self.assertTrue(any("table1: ✔️" in line for line in report))
        self.assertTrue(any("table2: ❌" in line for line in report))


# =============================================================================
# Pruebas para _revisar_columnas_tabla
# =============================================================================
class TestRevisarColumnasTabla(unittest.TestCase):
    @patch("utils.gx_utils.pd.read_sql")
    def test_exact_match_success(self, mock_read_sql):
        # Caso donde las columnas esperadas y reales son iguales
        expected_tables = {
            "table1": {"expected_columns": set(["a", "b"]), "exact_match": True}
        }
        actual_tables = {"table1"}
        df = pd.DataFrame({"column_name": ["a", "b"]})
        mock_read_sql.return_value = df
        fake_engine = MagicMock()
        schema = "public"
        report = _revisar_columnas_tabla(fake_engine, schema, expected_tables, actual_tables)
        self.assertIn("✔", report[0])

    @patch("utils.gx_utils.pd.read_sql")
    def test_subconjunto_success(self, mock_read_sql):
        # Caso donde las columnas esperadas son un subconjunto de las reales
        expected_tables = {
            "table1": {"expected_columns": set(["a"]), "exact_match": False}
        }
        actual_tables = {"table1"}
        df = pd.DataFrame({"column_name": ["a", "b", "c"]})
        mock_read_sql.return_value = df
        fake_engine = MagicMock()
        schema = "public"
        report = _revisar_columnas_tabla(fake_engine, schema, expected_tables, actual_tables)
        self.assertIn("✔", report[0])

    @patch("utils.gx_utils.pd.read_sql")
    def test_missing_columns(self, mock_read_sql):
        # Caso donde faltan columnas esperadas
        expected_tables = {
            "table1": {"expected_columns": set(["x", "y"]), "exact_match": False}
        }
        actual_tables = {"table1"}
        df = pd.DataFrame({"column_name": ["a", "b", "c"]})
        mock_read_sql.return_value = df
        fake_engine = MagicMock()
        schema = "public"
        report = _revisar_columnas_tabla(fake_engine, schema, expected_tables, actual_tables)
        self.assertIn("❌ Faltan columnas", report[0])


# =============================================================================
# Pruebas para _comparar_columnas_exactas y _comparar_columnas_subconjunto
# =============================================================================
class TestCompararColumnas(unittest.TestCase):
    def test_comparar_columnas_exactas_match(self):
        table = "table1"
        expected_cols = set(["a", "b"])
        actual_cols = set(["a", "b"])
        result = _comparar_columnas_exactas(table, expected_cols, actual_cols)
        self.assertIn("✔", result)

    def test_comparar_columnas_exactas_diff(self):
        table = "table1"
        expected_cols = set(["a", "b"])
        actual_cols = set(["a"])
        result = _comparar_columnas_exactas(table, expected_cols, actual_cols)
        self.assertIn("Faltantes", result)
        self.assertIn("❌", result)

    def test_comparar_columnas_subconjunto_success(self):
        table = "table1"
        expected_cols = set(["a"])
        actual_cols = set(["a", "b", "c"])
        result = _comparar_columnas_subconjunto(table, expected_cols, actual_cols)
        self.assertIn("✔", result)

    def test_comparar_columnas_subconjunto_failure(self):
        table = "table1"
        expected_cols = set(["a", "d"])
        actual_cols = set(["a", "b", "c"])
        result = _comparar_columnas_subconjunto(table, expected_cols, actual_cols)
        self.assertIn("Faltan columnas", result)
        self.assertIn("❌", result)

if __name__ == '__main__':
    unittest.main()
