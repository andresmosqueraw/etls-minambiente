import logging

from utils.gx_utils import (
    _cargar_expectativas_desde_yaml,
    _obtener_tablas_esperadas,
    _obtener_engine_sqlalchemy,
    _obtener_tablas_esquema,
    _revisar_tablas_encontradas,
    _revisar_columnas_tabla,
)


def reporte_expectativas_insumos(yaml_filename, schema, cfg):
    """
    Genera un reporte comparando las tablas y columnas esperadas (definidas en el YAML)
    con las encontradas en el esquema indicado.
    """
    logging.info("Iniciando reporte de expectativas...")
    logging.info(f"Generando reporte de estructura para {yaml_filename} en el esquema '{schema}'...")
    try:
        data = _cargar_expectativas_desde_yaml(yaml_filename, cfg)
        expected_tables = _obtener_tablas_esperadas(data)
        engine = _obtener_engine_sqlalchemy(cfg)
        actual_tables = _obtener_tablas_esquema(engine, schema)
        report_lines = _revisar_tablas_encontradas(schema, expected_tables, actual_tables)
        report_lines += _revisar_columnas_tabla(engine, schema, expected_tables, actual_tables)
        final_report = "\n".join(report_lines)
        logging.info("Reporte de validación de estructura:\n" + final_report)
        if "❌" in final_report:
            logging.error("\033[91m❌ reporte de expectativas falló (diferencias encontradas).\033[0m")
            raise Exception(f"Fallo en las expectativas del esquema '{schema}'.\n{final_report}")
        logging.info("\033[92m✔ reporte de expectativas finalizó sin errores.\033[0m")
        return final_report
    except Exception as e:
        logging.error(f"Error en reporte de expectativas: {e}")
        logging.error("\033[91m❌ reporte de expectativas falló.\033[0m")
        raise