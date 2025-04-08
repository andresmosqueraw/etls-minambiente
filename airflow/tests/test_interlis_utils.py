#!/usr/bin/env python
import os
import subprocess
import unittest
from unittest.mock import patch, MagicMock
import logging

# Importamos las funciones a testear desde interlis_utils.
from utils.interlis_utils import exportar_datos_ladm, importar_esquema_ladm

# Configuración externa ficticia (normalmente se obtiene de un sistema dinámico)
fake_outer_cfg = {
    "MODEL_DIR": "/fake/model",
    "XTF_DIR": "/fake/xtf",
    "ILI2DB_JAR_PATH": "/fake/ili2db.jar",
    "EPSG_SCRIPT": "/fake/epsg.sh"
}

# Configuración dinámica ficticia (lo que retorna leer_configuracion)
fake_dynamic_config = {
    "db": {
        "host": "localhost",
        "port": 5432,
        "user": "user",
        "password": "pass",
        "db_name": "test_db"
    },
    "logs": {
        "nombre_etl": "TestETL"
    },
    "interlis": {
        "nombre_archivo_xtf": "test.xtf",
        "nombre_modelo": "test_model"
    }
}

# Combinamos ambas configuraciones en una sola (como se espera que reciba la función)
combined_cfg = {**fake_outer_cfg, **fake_dynamic_config}

# -----------------------------------------------------------------------------
# Pruebas para exportar_datos_ladm
# -----------------------------------------------------------------------------
class TestExportarDatosLadm(unittest.TestCase):

    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_dynamic_config)
    @patch("utils.interlis_utils.os.path.exists")
    @patch("utils.interlis_utils.os.makedirs")
    @patch("utils.interlis_utils.subprocess.run")
    def test_exportar_datos_success(self, mock_run, mock_makedirs, mock_exists, mock_leer_config):
        """
        Verifica que exportar_datos_ladm:
         - Cree el directorio XTF si no existe.
         - Arme correctamente el comando a ejecutar.
         - Pase a subprocess.run los flags check=True, capture_output=True y text=True.
         - Termine sin lanzar excepciones cuando subprocess.run tiene éxito.
        """
        # Simulamos que el directorio XTF NO existe para forzar su creación.
        def exists_side_effect(path):
            if path == fake_outer_cfg["XTF_DIR"]:
                return False
            return True  # Simulamos que otros caminos existen.
        mock_exists.side_effect = exists_side_effect

        # Simular ejecución exitosa de subprocess.run con un objeto que tenga stderr.
        fake_result = MagicMock()
        fake_result.stderr = "Exportación completada sin advertencias"
        mock_run.return_value = fake_result

        # Se llama a exportar_datos_ladm con la configuración combinada.
        exportar_datos_ladm(combined_cfg)

        # Se verifica que se haya llamado a os.makedirs para crear el directorio XTF.
        mock_makedirs.assert_called_once_with(fake_outer_cfg["XTF_DIR"])

        # El path esperado para el archivo XTF se arma a partir de XTF_DIR y el nombre del archivo (desde interlis).
        expected_xtf_path = os.path.join(fake_outer_cfg["XTF_DIR"],
                                         fake_dynamic_config["interlis"]["nombre_archivo_xtf"])
        args, kwargs = mock_run.call_args
        command = args[0]
        # Se verifica que los elementos críticos aparecen en el comando.
        self.assertIn("java", command)
        self.assertIn("-jar", command)
        self.assertIn(fake_outer_cfg["ILI2DB_JAR_PATH"], command)
        self.assertIn("--dbhost", command)
        self.assertIn(fake_dynamic_config["db"]["host"], command)
        self.assertIn(expected_xtf_path, command)
        # Se verifica que se pasen los flags correctos a subprocess.run.
        self.assertTrue(kwargs.get("capture_output"))
        self.assertTrue(kwargs.get("text"))
        self.assertTrue(kwargs.get("check"))

    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_dynamic_config)
    @patch("utils.interlis_utils.os.path.exists", return_value=True)
    @patch("utils.interlis_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, "java", "Error de ejecución"))
    def test_exportar_datos_failure(self, mock_run, mock_exists, mock_leer_config):
        """
        Verifica que exportar_datos_ladm lance una excepción si subprocess.run falla.
        """
        with self.assertRaises(Exception) as context:
            exportar_datos_ladm(combined_cfg)
        self.assertIn("Error exportando XTF", str(context.exception))


# -----------------------------------------------------------------------------
# Pruebas para importar_esquema_ladm
# -----------------------------------------------------------------------------
class TestImportarEsquemaLadm(unittest.TestCase):

    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_dynamic_config)
    @patch("utils.interlis_utils.os.path.exists")
    @patch("utils.interlis_utils.subprocess.run")
    def test_importar_esquema_success(self, mock_run, mock_exists, mock_leer_config):
        """
        Verifica que importar_esquema_ladm ejecute correctamente el comando para importar el esquema,
        comprobando que se incluyan parámetros críticos (por ejemplo, '--schemaimport' y el preScript).
        """
        # Simular que el archivo JAR existe.
        mock_exists.return_value = True

        # Llamar a importar_esquema_ladm con la configuración combinada.
        importar_esquema_ladm(combined_cfg)

        # Extraer el comando enviado a subprocess.run.
        args, kwargs = mock_run.call_args
        command = args[0]
        self.assertIn("java", command)
        self.assertIn("-jar", command)
        self.assertIn(fake_outer_cfg["ILI2DB_JAR_PATH"], command)
        self.assertIn("--schemaimport", command)
        self.assertIn("--preScript", command)
        self.assertIn(fake_outer_cfg["EPSG_SCRIPT"], command)
        self.assertTrue(kwargs.get("check"))

    @patch("utils.interlis_utils.os.path.exists", return_value=False)
    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_dynamic_config)
    def test_importar_esquema_jar_no_existe(self, mock_leer_config, mock_exists):
        """
        Verifica que importar_esquema_ladm lance FileNotFoundError si el archivo JAR no existe.
        """
        with self.assertRaises(FileNotFoundError) as context:
            importar_esquema_ladm(combined_cfg)
        self.assertIn("Archivo JAR no encontrado", str(context.exception))

    @patch("utils.interlis_utils.leer_configuracion", return_value=fake_dynamic_config)
    @patch("utils.interlis_utils.os.path.exists", return_value=True)
    @patch("utils.interlis_utils.subprocess.run", side_effect=subprocess.CalledProcessError(1, "java", "Error en importación"))
    def test_importar_esquema_failure(self, mock_run, mock_exists, mock_leer_config):
        """
        Verifica que importar_esquema_ladm lance una excepción si subprocess.run falla durante la importación.
        """
        with self.assertRaises(Exception) as context:
            importar_esquema_ladm(combined_cfg)
        self.assertIn("Error importando", str(context.exception))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()