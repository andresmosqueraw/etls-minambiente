#!/usr/bin/env python
import unittest
from unittest.mock import patch, MagicMock, call
import psycopg2
import logging

from utils.db_utils import (
    ejecutar_sql,
    validar_conexion_postgres,
    revisar_existencia_db,
    crear_base_datos,
    adicionar_extensiones,
    restablecer_esquema_insumos,
    restablecer_esquema_estructura_intermedia,
    restablecer_esquema_ladm
)

# Configuración ficticia para los tests
fake_config = {
    "db": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "db_name": "test_db"
    }
}

# ---------------------------
# Tests para ejecutar_sql
# ---------------------------
class TestEjecutarSQL(unittest.TestCase):
    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_ejecutar_sql_success(self, mock_connect, mock_leer_config):
        # Se simula que la configuración retorna fake_config
        mock_leer_config.return_value = fake_config

        # Crear una conexión y cursor ficticios
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # El cursor se usa en un contexto (with statement)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        sql = "SELECT * FROM tabla;"
        params = (1, 2, 3)

        # Se llama a la función con parámetros
        ejecutar_sql(fake_config, sql, params)

        # Verificar que se haya ejecutado el SQL con parámetros y realizado el commit
        mock_cursor.execute.assert_called_with(sql, params)
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_ejecutar_sql_failure(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Forzar excepción en la ejecución del SQL
        mock_cursor.execute.side_effect = Exception("Error de ejecución")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        sql = "INSERT INTO tabla VALUES (%s, %s);"

        with self.assertRaises(Exception) as context:
            ejecutar_sql(fake_config, sql)
        self.assertIn("Error ejecutando SQL", str(context.exception))
        # Se debe llamar rollback y cerrar la conexión
        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()

# ---------------------------
# Tests para validar_conexion_postgres
# ---------------------------
class TestValidarConexionPostgres(unittest.TestCase):
    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_validar_conexion_postgres_success(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = validar_conexion_postgres(fake_config)
        self.assertTrue(result)
        mock_conn.close.assert_called_once()

    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect', side_effect=psycopg2.Error("Fallo en la conexión"))
    def test_validar_conexion_postgres_failure(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        with self.assertRaises(Exception) as context:
            validar_conexion_postgres(fake_config)
        self.assertIn("Error en la conexión", str(context.exception))

# ---------------------------
# Tests para revisar_existencia_db
# ---------------------------
class TestRevisarExistenciaDB(unittest.TestCase):
    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_revisar_existencia_db_exists(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Simular que la base de datos existe (fetchone devuelve algo distinto de None)
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        result = revisar_existencia_db(fake_config)
        # Se espera que si la base existe, se retorne la lista ["Adicionar_Extensiones"]
        self.assertEqual(result, ["Adicionar_Extensiones"])
        mock_conn.close.assert_called_once()

    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_revisar_existencia_db_not_exists(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Simular que la base de datos no existe (fetchone devuelve None)
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        result = revisar_existencia_db(fake_config)
        # En este caso se espera ["Crear_Base_Datos"]
        self.assertEqual(result, ["Crear_Base_Datos"])
        mock_conn.close.assert_called_once()

    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect', side_effect=Exception("Error en DB"))
    def test_revisar_existencia_db_exception(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        with self.assertRaises(Exception) as context:
            revisar_existencia_db(fake_config)
        self.assertIn("Error revisando existencia de la base de datos", str(context.exception))

# ---------------------------
# Tests para crear_base_datos
# ---------------------------
class TestCrearBaseDatos(unittest.TestCase):
    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_crear_base_datos_success(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        crear_base_datos(fake_config)

        expected_sql = f"CREATE DATABASE {fake_config['db']['db_name']};"
        mock_cursor.execute.assert_called_with(expected_sql)
        mock_conn.close.assert_called_once()

    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_crear_base_datos_failure(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Forzar error al ejecutar el SQL de creación de la DB
        mock_cursor.execute.side_effect = Exception("Error al crear DB")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        with self.assertRaises(Exception) as context:
            crear_base_datos(fake_config)
        self.assertIn("Error creando base de datos", str(context.exception))

# ---------------------------
# Tests para adicionar_extensiones
# ---------------------------
class TestAdicionarExtensiones(unittest.TestCase):
    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_adicionar_extensiones_success(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        adicionar_extensiones(fake_config)

        expected_calls = [
            call("CREATE EXTENSION IF NOT EXISTS plpgsql;"),
            call("CREATE EXTENSION IF NOT EXISTS postgis;"),
            call("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        ]
        mock_cursor.execute.assert_has_calls(expected_calls, any_order=False)
        mock_conn.close.assert_called_once()

    @patch('utils.db_utils.leer_configuracion')
    @patch('psycopg2.connect')
    def test_adicionar_extensiones_failure(self, mock_connect, mock_leer_config):
        mock_leer_config.return_value = fake_config

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Forzar fallo al ejecutar alguna extensión
        mock_cursor.execute.side_effect = Exception("Error en extensión")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        with self.assertRaises(Exception) as context:
            adicionar_extensiones(fake_config)
        self.assertIn("Error adicionando extensiones", str(context.exception))

# ---------------------------
# Tests para restablecer esquemas
# ---------------------------
class TestRestablecerEsquema(unittest.TestCase):
    @patch('utils.db_utils.ejecutar_sql')
    def test_restablecer_esquema_insumos(self, mock_ejecutar_sql):
        restablecer_esquema_insumos(fake_config)
        expected_sql = "DROP SCHEMA IF EXISTS insumos CASCADE; CREATE SCHEMA insumos;"
        mock_ejecutar_sql.assert_called_once_with(fake_config, expected_sql)

    @patch('utils.db_utils.ejecutar_sql')
    def test_restablecer_esquema_estructura_intermedia(self, mock_ejecutar_sql):
        restablecer_esquema_estructura_intermedia(fake_config)
        expected_sql = "DROP SCHEMA IF EXISTS estructura_intermedia CASCADE; CREATE SCHEMA estructura_intermedia;"
        mock_ejecutar_sql.assert_called_once_with(fake_config, expected_sql)

    @patch('utils.db_utils.ejecutar_sql')
    def test_restablecer_esquema_ladm(self, mock_ejecutar_sql):
        restablecer_esquema_ladm(fake_config)
        expected_sql = "DROP SCHEMA IF EXISTS ladm CASCADE; CREATE SCHEMA ladm;"
        mock_ejecutar_sql.assert_called_once_with(fake_config, expected_sql)

if __name__ == '__main__':
    # Se activa un nivel de logging para depuración (opcional)
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()