import os
import zipfile
import requests
import subprocess
import logging
import unittest
from unittest.mock import patch, MagicMock, mock_open


# Ajuste la ruta de importación según corresponda.
from utils.data_utils import (
    obtener_insumos_desde_web,
    ejecutar_copia_insumo_local,
    copia_insumo_local,
    _validar_archivo_local,
    procesar_insumos_descargados,
    _extraer_zip,
    ejecutar_importacion_general_a_postgres,
    _buscar_shp_en_carpeta,
    _importar_shp_a_postgres
)

from config.general_config import TEMP_FOLDER, ETL_DIR

# ---------------------------
# Tests para obtener_insumos_desde_web
# ---------------------------
class TestObtenerInsumosDesdeWeb(unittest.TestCase):
    @patch('utils.data_utils.limpiar_carpeta_temporal')
    @patch('utils.data_utils.leer_configuracion')
    @patch('os.makedirs')
    @patch('utils.data_utils.requests.get')
    @patch('os.path.getsize')
    @patch('utils.data_utils.open', new_callable=mock_open)
    def test_obtener_insumos_desde_web_exito(self, mock_file, mock_getsize, mock_requests_get, mock_makedirs,
                                               mock_leer_config, mock_limpiar_temp):
        """
        Prueba que obtener_insumos_desde_web descarga correctamente el ZIP y lo deja en la carpeta temporal.
        """
        config = {
            "insumos_web": {"insumo1": "http://fake-url/insumo1.zip"},
            "insumos_local": {"insumo1": "local/path/insumo1.zip"}
        }
        mock_leer_config.return_value = config

        # Simular respuesta exitosa de requests.get
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'datos']
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        # Simular que el archivo descargado tiene contenido
        mock_getsize.return_value = 100

        # Crear un contexto simulado con ti (para XCom de Airflow)
        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        resultado = obtener_insumos_desde_web(**context)
        expected_path = os.path.join(TEMP_FOLDER, "insumo1.zip")
        self.assertEqual(resultado, {"insumo1": expected_path})
        # Verificar que se hayan hecho los push de XCom
        mock_ti.xcom_push.assert_any_call(key="errores", value=[])
        mock_ti.xcom_push.assert_any_call(key="insumos_web", value=resultado)

    @patch('os.remove')
    @patch('utils.data_utils.limpiar_carpeta_temporal')
    @patch('utils.data_utils.leer_configuracion')
    @patch('os.makedirs')
    @patch('utils.data_utils.requests.get')
    @patch('os.path.getsize')
    @patch('utils.data_utils.open', new_callable=mock_open)
    def test_obtener_insumos_desde_web_archivo_vacio(self, mock_file, mock_getsize, mock_requests_get, mock_makedirs,
                                                     mock_leer_config, mock_limpiar_temp, mock_remove):
        """
        Prueba que si el ZIP descargado está vacío se elimina y se asigna None para ese insumo.
        """
        config = {
            "insumos_web": {"insumo1": "http://fake-url/insumo1.zip"},
            "insumos_local": {"insumo1": "local/path/insumo1.zip"}
        }
        mock_leer_config.return_value = config

        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'']
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        # Simular archivo vacío
        mock_getsize.return_value = 0

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        resultado = obtener_insumos_desde_web(**context)
        self.assertEqual(resultado, {"insumo1": None})
        mock_remove.assert_called_once_with(os.path.join(TEMP_FOLDER, "insumo1.zip"))

    @patch('utils.data_utils.limpiar_carpeta_temporal')
    @patch('utils.data_utils.leer_configuracion')
    @patch('os.makedirs')
    @patch('utils.data_utils.requests.get', side_effect=Exception("Error de red"))
    @patch('os.path.getsize')
    def test_obtener_insumos_desde_web_falla_descarga(self, mock_getsize, mock_requests_get, mock_makedirs,
                                                      mock_leer_config, mock_limpiar_temp):
        """
        Prueba que al fallar la descarga se asigne None para el insumo y se registre el error.
        """
        config = {
            "insumos_web": {"insumo1": "http://fake-url/insumo1.zip"},
            "insumos_local": {"insumo1": "local/path/insumo1.zip"}
        }
        mock_leer_config.return_value = config
        mock_getsize.return_value = 100  # No se usará dado que ocurre la excepción

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        resultado = obtener_insumos_desde_web(**context)
        self.assertEqual(resultado, {"insumo1": None})

    @patch('utils.data_utils.limpiar_carpeta_temporal')
    @patch('utils.data_utils.leer_configuracion')
    @patch('os.makedirs')
    def test_obtener_insumos_desde_web_sin_insumos_web(self, mock_makedirs, mock_leer_config, mock_limpiar_temp):
        """
        Prueba que se lance ValueError si no se encuentra la clave 'insumos_web' en la configuración.
        """
        config = {"insumos_local": {"insumo1": "local/path/insumo1.zip"}}
        mock_leer_config.return_value = config
        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        with self.assertRaises(ValueError):
            obtener_insumos_desde_web(**context)


# ---------------------------
# Tests para ejecutar_copia_insumo_local y copia_insumo_local/_validar_archivo_local
# ---------------------------
class TestCopiaInsumoLocal(unittest.TestCase):
    @patch('utils.data_utils._validar_archivo_local', return_value='/base/local/insumo1.zip')
    @patch('os.path.exists', return_value=True)
    def test_copia_insumo_local_exito(self, mock_exists, mock_validar):
        """
        Prueba que copia_insumo_local retorne la ruta válida cuando se encuentra el respaldo local.
        """
        resultado = copia_insumo_local("http://fake-url/insumo1.zip", "insumo1",
                                       {"insumo1": "local/insumo1.zip"}, "/base", "error simulado")
        self.assertEqual(resultado, '/base/local/insumo1.zip')

    @patch('utils.data_utils._validar_archivo_local', return_value='/base/local/insumo1.zip')
    @patch('os.path.exists', return_value=False)
    def test_copia_insumo_local_falla(self, mock_exists, mock_validar):
        """
        Prueba que copia_insumo_local lance excepción si el archivo de respaldo no existe en disco.
        """
        with self.assertRaises(Exception) as ctx:
            copia_insumo_local("http://fake-url/insumo1.zip", "insumo1",
                               {"insumo1": "local/insumo1.zip"}, "/base", "error simulado")
        self.assertIn("Respaldo local no encontrado", str(ctx.exception))

class TestValidarArchivoLocal(unittest.TestCase):
    @patch('os.path.exists', return_value=True)
    def test_validar_archivo_local_exito(self, mock_exists):
        """
        Prueba que _validar_archivo_local retorne la ruta correcta si el archivo existe.
        """
        insumos_local = {"insumo1": "ruta/insumo1.zip"}
        resultado = _validar_archivo_local("insumo1", insumos_local, "/base")
        self.assertEqual(resultado, os.path.join("/base", "ruta/insumo1.zip"))

    @patch('os.path.exists', return_value=False)
    def test_validar_archivo_local_no_existe(self, mock_exists):
        """
        Prueba que _validar_archivo_local lance FileNotFoundError si el archivo no existe.
        """
        insumos_local = {"insumo1": "ruta/insumo1.zip"}
        with self.assertRaises(FileNotFoundError):
            _validar_archivo_local("insumo1", insumos_local, "/base")

    def test_validar_archivo_local_sin_entrada(self):
        """
        Prueba que _validar_archivo_local lance FileNotFoundError si no hay entrada para la clave.
        """
        with self.assertRaises(FileNotFoundError):
            _validar_archivo_local("insumo_inexistente", {}, "/base")

# ---------------------------
# Tests para procesar_insumos_descargados y _extraer_zip
# ---------------------------
class TestProcesarInsumosDescargados(unittest.TestCase):
    @patch('utils.data_utils._extraer_zip')
    @patch('os.makedirs')
    @patch('os.path.exists', return_value=True)
    def test_procesar_insumos_descargados_exito(self, mock_exists, mock_makedirs, mock_extraer_zip):
        """
        Prueba que procesar_insumos_descargados descomprima los ZIP correctamente y empuje los datos a XCom.
        """
        # Simular que xcom_pull retorna diccionarios de insumos (mezcla de web y local)
        ti = MagicMock()
        ti.xcom_pull.side_effect = [
            {"insumo1": "/base/insumo1.zip"},  # insumos_web
            {"insumo1": "/base_local/insumo1.zip"}  # insumos_local
        ]
        context = {"ti": ti}

        resultado = procesar_insumos_descargados(**context)
        # Se espera que el insumo se procese correctamente
        self.assertEqual(resultado, [{"key": "insumo1", "zip_path": "/base_local/insumo1.zip"}])
        # Verificar que se haya llamado a XCom push para insumos_procesados
        ti.xcom_push.assert_called_with(key="insumos_procesados", value=[{"key": "insumo1", "zip_path": "/base_local/insumo1.zip"}])

    @patch('os.path.exists', return_value=False)
    def test_procesar_insumos_descargados_archivo_no_existe(self, mock_exists):
        """
        Prueba que procesar_insumos_descargados lance excepción si algún ZIP no existe.
        """
        ti = MagicMock()
        ti.xcom_pull.side_effect = [
            {"insumo1": "/base/insumo1.zip"},
            {}
        ]
        context = {"ti": ti}
        with self.assertRaises(Exception) as ctx:
            procesar_insumos_descargados(**context)
        self.assertIn("Archivo ZIP no encontrado", str(ctx.exception))

class TestExtraerZip(unittest.TestCase):
    def test_extraer_zip_exito(self):
        """
        Prueba que _extraer_zip extraiga correctamente el contenido de un ZIP válido.
        """
        with patch.object(zipfile, 'ZipFile') as mock_zip_class:
            mock_zip = MagicMock()
            mock_zip_class.return_value.__enter__.return_value = mock_zip
            _extraer_zip("/fake/path.zip", "/fake/extract_folder")
            mock_zip.extractall.assert_called_once_with("/fake/extract_folder")

    def test_extraer_zip_bad_zip(self):
        """
        Prueba que _extraer_zip lance excepción si el ZIP está corrupto.
        """
        with patch.object(zipfile, 'ZipFile', side_effect=zipfile.BadZipFile("Archivo corrupto")):
            with self.assertRaises(Exception) as ctx:
                _extraer_zip("/fake/path.zip", "/fake/extract_folder")
            self.assertIn("no es un ZIP válido", str(ctx.exception))

# ---------------------------
# Tests para ejecutar_importacion_general_a_postgres, _buscar_shp_en_carpeta y _importar_shp_a_postgres
# ---------------------------
class TestImportarSHPPostgres(unittest.TestCase):
    @patch('utils.data_utils._importar_shp_a_postgres')
    @patch('utils.data_utils._buscar_shp_en_carpeta', return_value="/fake/path/file.shp")
    @patch('utils.data_utils.leer_configuracion')
    def test_ejecutar_importar_shp_a_postgres_exito(self, mock_leer_config, mock_buscar_shp, mock_importar_shp):
        """
        Prueba que ejecutar_importacion_general_a_postgres invoque la importación del SHP correctamente.
        """
        config = {
            "db": {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "postgres",
                "db_name": "test_db"
            }
        }
        mock_leer_config.return_value = config
        ti = MagicMock()
        ti.xcom_pull.return_value = [{"key": "insumo1", "zip_path": "/fake/path/file.zip"}]
        context = {"ti": ti}

        ejecutar_importacion_general_a_postgres(**context)
        mock_buscar_shp.assert_called_once_with(os.path.join(TEMP_FOLDER, "insumo1"))
        # Se espera que _importar_shp_a_postgres se llame con la configuración y los paths adecuados
        mock_importar_shp.assert_called_once_with(
            config["db"], "/fake/path/file.shp", "insumos.insumo1"
        )

    @patch('utils.data_utils.leer_configuracion')
    def test_ejecutar_importar_shp_a_postgres_sin_insumos(self, mock_leer_config):
        """
        Prueba que ejecutar_importacion_general_a_postgres lance excepción si XCom no retorna datos válidos.
        """
        ti = MagicMock()
        ti.xcom_pull.return_value = None  # O puede ser algo que no sea una lista
        context = {"ti": ti}
        with self.assertRaises(Exception) as ctx:
            ejecutar_importacion_general_a_postgres(**context)
        self.assertIn("No se encontró información válida de insumos", str(ctx.exception))

    @patch('utils.data_utils._buscar_shp_en_carpeta', return_value=None)
    @patch('utils.data_utils.leer_configuracion')
    def test_ejecutar_importar_shp_a_postgres_sin_shp(self, mock_leer_config, mock_buscar_shp):
        """
        Prueba que ejecutar_importacion_general_a_postgres lance FileNotFoundError si no se encuentra un archivo SHP.
        """
        config = {
            "db": {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "postgres",
                "db_name": "test_db"
            }
        }
        mock_leer_config.return_value = config
        ti = MagicMock()
        ti.xcom_pull.return_value = [{"key": "insumo1", "zip_path": "/fake/path/file.zip"}]
        context = {"ti": ti}
        with self.assertRaises(FileNotFoundError):
            ejecutar_importacion_general_a_postgres(**context)

class TestBuscarEImportarSHP(unittest.TestCase):
    @patch('os.walk')
    @patch('os.path.exists', return_value=True)
    @patch('os.path.getsize', return_value=100)
    def test_buscar_shp_en_carpeta_encuentra(self, mock_getsize, mock_exists, mock_walk):
        """
        Prueba que _buscar_shp_en_carpeta retorne la ruta del primer archivo .shp válido.
        """
        mock_walk.return_value = [
            ('/base', ['subdir'], ['archivo.txt', 'mapa.shp']),
            ('/base/subdir', [], ['otro.shp'])
        ]
        resultado = _buscar_shp_en_carpeta("/base")
        self.assertEqual(resultado, os.path.join("/base", "mapa.shp"))

    @patch('os.walk')
    def test_buscar_shp_en_carpeta_no_encuentra(self, mock_walk):
        """
        Prueba que _buscar_shp_en_carpeta retorne None si no hay ningún archivo .shp válido.
        """
        mock_walk.return_value = [
            ('/base', ['subdir'], ['archivo.txt']),
            ('/base/subdir', [], ['otro.doc'])
        ]
        resultado = _buscar_shp_en_carpeta("/base")
        self.assertIsNone(resultado)

    @patch('utils.data_utils.subprocess.run')
    def test_importar_shp_a_postgres_exito(self, mock_subproc):
        """
        Prueba que _importar_shp_a_postgres invoque correctamente ogr2ogr sin lanzar excepción.
        """
        db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres',
            'db_name': 'test_db'
        }
        shp_file = "/fake/path/file.shp"
        table_name = "insumos.insumo1"

        # Simulamos una ejecución exitosa
        mock_subproc.return_value = MagicMock(returncode=0)
        try:
            _importar_shp_a_postgres(db_config, shp_file, table_name)
        except Exception as e:
            self.fail(f"_importar_shp_a_postgres lanzó excepción inesperada: {e}")

        expected_command = [
            "ogr2ogr", "-f", "PostgreSQL",
            f"PG:host={db_config['host']} port={db_config['port']} dbname={db_config['db_name']} "
            f"user={db_config['user']} password={db_config['password']}",
            shp_file,
            "-nln", table_name,
            "-overwrite",
            "-progress",
            "-lco", "GEOMETRY_NAME=geom",
            "-lco", "FID=gid",
            "-nlt", "PROMOTE_TO_MULTI",
            "-t_srs", "EPSG:9377"
        ]
        mock_subproc.assert_called_once()
        self.assertEqual(mock_subproc.call_args[0][0], expected_command)

    @patch('utils.data_utils.subprocess.run', side_effect=subprocess.CalledProcessError(1, 'ogr2ogr', "Error OGR2OGR"))
    def test_importar_shp_a_postgres_falla(self, mock_subproc):
        """
        Prueba que _importar_shp_a_postgres lance excepción cuando ogr2ogr falla.
        """
        db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres',
            'db_name': 'test_db'
        }
        with self.assertRaises(Exception) as ctx:
            _importar_shp_a_postgres(db_config, "/fake/path/file.shp", "insumos.insumo1")
        self.assertIn("Error importando", str(ctx.exception))

if __name__ == '__main__':
    unittest.main()