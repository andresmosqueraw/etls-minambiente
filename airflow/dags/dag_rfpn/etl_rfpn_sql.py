
def estructura_intermedia():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/*****************************
             Creación de estructura de datos intermedia 
        	Migración del rfpn  al modelo LADM_COL-RFPN
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 *************************/
/*************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 ****************************/


--====================================================
-- Creación de la extensión postgis
--====================================================
CREATE EXTENSION IF NOT EXISTS postgis;

--====================================================
-- Creación de la extensión uuid
--====================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

--====================================================
-- Inserción del sistema origen unico nacional
--====================================================
INSERT into spatial_ref_sys (
  srid, auth_name, auth_srid, proj4text, srtext
)
values
  (
    9377,
    'EPSG',
    9377,
    '+proj=tmerc +lat_0=4.0 +lon_0=-73.0 +k=0.9992 +x_0=5000000 +y_0=2000000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs ',
    'PROJCRS["MAGNA-SIRGAS / Origen-Nacional", BASEGEOGCRS["MAGNA-SIRGAS", DATUM["Marco Geocentrico Nacional de Referencia", ELLIPSOID["GRS 1980",6378137,298.257222101, LENGTHUNIT["metre",1]]], PRIMEM["Greenwich",0, ANGLEUNIT["degree",0.0174532925199433]], ID["EPSG",4686]], CONVERSION["Colombia Transverse Mercator", METHOD["Transverse Mercator", ID["EPSG",9807]], PARAMETER["Latitude of natural origin",4, ANGLEUNIT["degree",0.0174532925199433], ID["EPSG",8801]], PARAMETER["Longitude of natural origin",-73, ANGLEUNIT["degree",0.0174532925199433], ID["EPSG",8802]], PARAMETER["Scale factor at natural origin",0.9992, SCALEUNIT["unity",1], ID["EPSG",8805]], PARAMETER["False easting",5000000, LENGTHUNIT["metre",1], ID["EPSG",8806]], PARAMETER["False northing",2000000, LENGTHUNIT["metre",1], ID["EPSG",8807]]], CS[Cartesian,2], AXIS["northing (N)",north, ORDER[1], LENGTHUNIT["metre",1]], AXIS["easting (E)",east, ORDER[2], LENGTHUNIT["metre",1]], USAGE[ SCOPE["unknown"], AREA["Colombia"], BBOX[-4.23,-84.77,15.51,-66.87]], ID["EPSG",9377]]'
  ) ON CONFLICT (srid) DO NOTHING;
  
--========================================
--Creación de esquema
--========================================
 CREATE schema IF NOT EXISTS estructura_intermedia;

--========================================
--Fijar esquema
--========================================
set search_path to 
	estructura_intermedia,	--Nombre del esquema de estructura de datos intermedia
	public;


--=============================================
-- Área de reserva rfpn
--=============================================
DROP TABLE IF exists rfpn_uab_areareserva;

CREATE TABLE rfpn_uab_areareserva (
	uab_identificador varchar(7) NOT null,
	uab_id_ap varchar (255) NOT NULL,  
	uab_nombre_reserva varchar(150) NOT NULL, 
	uab_nombre varchar(255) NULL,
	ue_geometria public.geometry(multipolygonz, 9377) NULL,
	interesado_nombre varchar(255) NULL, 
	interesado_tipo_interesado varchar(255) NOT NULL,
	interesado_tipo_documento varchar(255) NULL,
	interesado_numero_documento varchar(255) NULL,
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL,
	fuente_administrativa_tipo_formato varchar(255) NULL, 
	fuente_administrativa_fecha_documento_fuente date NULL, 
	fuente_administrativa_nombre varchar(255) null, 
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);
--=============================================
-- Zonificacion rfpn
--=============================================
DROP TABLE IF exists rfpn_uab_zonificacion;

CREATE TABLE rfpn_uab_zonificacion (
	uab_identificador varchar(1000) NOT NULL, 
	uab_tipo_zona varchar(255) NOT NULL,
	uab_detalle_zona varchar(1000) NULL,
	uab_regimen_usos varchar(10000) NOT NULL,
	uab_areareserva varchar(255) NOT NULL,
	uab_nombre varchar(255) NULL, 
	ue_geometria public.geometry(multipolygonz, 9377) NULL, 
	interesado_nombre varchar(255) NULL, 
	interesado_tipo_interesado varchar(255) NOT NULL, 
	interesado_tipo_documento varchar(255) NULL, 
	interesado_numero_documento varchar(255) NULL,
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) null, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);

--========================================
-- Régimen Uso Actividades
--========================================
DROP TABLE IF EXISTS rfpn_regimenusosactividades;

CREATE TABLE rfpn_regimenusosactividades (
    uab_identificador VARCHAR(1000) NOT NULL,
    uab_principal VARCHAR(5000) NOT NULL,
    uab_compatible VARCHAR(5000) NULL,
    uab_condicionado VARCHAR(5000) NULL,
    uab_prohibido VARCHAR(5000) NULL,
    rfpn_uab_area_reserva_regimen_actividades VARCHAR(255) NOT NULL,
    rfpn_uab_zonificacion_regimen_actividades VARCHAR(255) NOT NULL
);


--=============================================
-- Compensacion rfpn
--=============================================
DROP TABLE IF exists rfpn_uab_compensacion;

CREATE TABLE rfpn_uab_compensacion (
	uab_identificador varchar(15) NOT NULL, -- Interno con el cual se identifica la reserva a la cual se está haciendo la compensación
	uab_expediente varchar(100) NOT NULL, -- Número del expediente interno relacionado con la compensación
	uab_observaciones text NULL, -- Nombre del proyecto objeto de la sustracción
	uab_areareserva varchar(255) NOT NULL,
	uab_sustraccion varchar(255) NULL,
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) NULL, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);

--=============================================
-- Sustraccion rfpn
--=============================================
DROP TABLE IF exists rfpn_uab_sustraccion;

CREATE TABLE rfpn_uab_sustraccion (
	uab_identificador varchar(15) NOT NULL, -- Identificador interno para identificación de la sustracción definitiva o temporal de la Reserva rfpnª de 1959
	uab_expediente varchar(100) NOT NULL,
	uab_tipo_sustraccion varchar(255) NOT NULL, -- Dominio que define el tipo de sustracción realikzado a la Reserva rfpnª de 1959
	uab_fin_sustraccion date NULL, -- Corresponde a la fecha en la que finaliza la sustracción (aplica para las sustracciones temporales)
	uab_tipo_sector varchar(255) NOT NULL, -- Identifica el sector que realizó la solicitud de sustracción
	uab_detalle_sector varchar(255) NULL, -- Cuando el tipo de sector sea Otro, se podrá especificar el detalle de este sector
	uab_observaciones text NULL, -- Observaciones generales de la sustracción
	uab_areareserva varchar(255) NULL,
	uab_nombre_areareserva varchar(255) NULL,
	uab_compensacion varchar(255) NULL,	
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	uab_solicitante varchar (255) NULL, 
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) NULL, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);


--=============================================
-- Fuente Administrativa rfpn
--=============================================
DROP TABLE IF exists rfpn_fuenteadministrativa;

CREATE TABLE rfpn_fuenteadministrativa (
	uab_identificador varchar(15) NOT NULL, -- Interno con el cual se identifica la reserva a la cual se está haciendo la compensación
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_numero int4 null,
	fuente_administrativa_anio int4 null
);

--=============================================
-----------------------------------------------
-- Funciones de homologacion rfpn
-----------------------------------------------
--=============================================

--=============================================
-- Funcion de homologacion a texto
--=============================================

DROP FUNCTION IF exists homologar_texto;

CREATE OR REPLACE FUNCTION homologar_texto(texto_input TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN upper(
        regexp_replace(
            translate(trim(texto_input), 'áéíóúÁÉÍÓÚ', 'aeiouAEIOU'), 
            '\s+', '', 
            'g'
        )
    );
END;
$$ LANGUAGE plpgsql;

--=============================================
-- Funcion de homologacion a numero
--=============================================

DROP FUNCTION IF exists estructura_intermedia.homologar_numero;

CREATE OR REPLACE FUNCTION estructura_intermedia.homologar_numero(texto TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN regexp_replace(texto, '[^0-9]', '', 'g');
END;
$$ LANGUAGE plpgsql;
    """
    return sql_script

def transformacion_datos():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/**********
            ETL de tranformación de insumos a estructura de datos intermedia
                    Migración del RFPN  al modelo LADM_COL-RFPN
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        email           : contacto@ceicol.com
 *********/
/*********
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 **********/

--========================================
-- Fijar esquema
--========================================
SET search_path TO 
    estructura_intermedia, -- Esquema de estructura de datos intermedia
    public;

---------------------------------------------------------------
-- AREA DE RESERVA RFPN
---------------------------------------------------------------
INSERT INTO estructura_intermedia.rfpn_uab_areareserva(
    uab_identificador,
    uab_id_ap,  
    uab_nombre_reserva, 
    uab_nombre, 
    ue_geometria, 
    interesado_nombre, 
    interesado_tipo_interesado, 
    interesado_tipo_documento, 
    interesado_numero_documento,
    fuente_administrativa_tipo, 
    fuente_administrativa_estado_disponibilidad, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_nombre, 
    ddr_tipo_resposabilidad,
    ddr_tipo_derecho
)
WITH cte AS (
  SELECT 
    cr.*,
    TRIM(UPPER("Nombre del área protegida")) AS nombre_norm,
    CASE 
      WHEN TRIM(UPPER("Nombre del área protegida")) IN ('PÁRAMO GRANDE', 'QUEBRADA HONDA Y CAÑOS PARRADO Y BUQUE', 'RÍO NARE')
        THEN 1
      ELSE 0
    END AS is_static
  FROM insumos.cruce_area_reserva cr
),
cte2 AS (
  SELECT 
    cte.*,
    CASE 
      WHEN is_static = 0 THEN ROW_NUMBER() OVER (ORDER BY nombre_norm)
      ELSE NULL
    END AS rn_dynamic
  FROM cte
)
SELECT
  CASE 
    WHEN is_static = 1 THEN
      CASE 
        WHEN nombre_norm = 'PÁRAMO GRANDE' THEN 'RFPN_09'
        WHEN nombre_norm = 'QUEBRADA HONDA Y CAÑOS PARRADO Y BUQUE' THEN 'RFPN_56'
        WHEN nombre_norm = 'RÍO NARE' THEN 'RFPN_58'
      END
    ELSE
      CASE
        WHEN rn_dynamic BETWEEN 1 AND 8 THEN 'RFPN_' || LPAD(rn_dynamic::text, 2, '0')
        WHEN rn_dynamic BETWEEN 9 AND 54 THEN 'RFPN_' || LPAD((rn_dynamic + 1)::text, 2, '0')
        ELSE 'RFPN_' || LPAD((rn_dynamic + 4)::text, 2, '0')
      END
  END AS uab_identificador,
  "Id del área protegida" AS uab_id_ap,
  "Nombre del área protegida" AS uab_nombre_reserva,
  'rfpn_uab_areareserva'::varchar(255) AS uab_nombre,
  ST_Force3D(ST_Transform(geom,9377)) AS ue_geometria,
  'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre,
  'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado,
  'NIT'::varchar(255) AS interesado_tipo_documento,
  '830.115.395-1'::varchar(255) AS interesado_numero_documento,
  'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
  'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad,
  'Documento'::varchar(255) AS fuente_administrativa_tipo_formato,
  "Fecha del acto"::date AS fuente_administrativa_fecha_documento_fuente,
  replace("Tipo de acto administrativo", 'Actos', 'Acto') ||
    ' ' ||
    "Número del acto" ||
    ' de ' ||
    to_char("Fecha del acto"::date, 'YYYY') AS fuente_administrativa_nombre,
  NULL AS ddr_tipo_resposabilidad,
  'Realinderar'::varchar(255) AS ddr_tipo_derecho
FROM cte2
ORDER BY nombre_norm;

---------------------------------------------------------------
-- ZONIFICACIÓN RFPN_REGIMENUSOSACTIVIDADES
---------------------------------------------------------------
INSERT INTO estructura_intermedia.rfpn_regimenusosactividades(
    uab_identificador,
    uab_principal,
    uab_compatible,
    uab_condicionado,
    uab_prohibido,
    rfpn_uab_area_reserva_regimen_actividades,
    rfpn_uab_zonificacion_regimen_actividades
)
SELECT 
    ('RUA_' || ROW_NUMBER() OVER ())::VARCHAR(255) AS uab_identificador,
    CASE 
        WHEN uso_principal IS NULL OR uso_principal = '' THEN 'Otros Usos'
        ELSE uso_principal
    END AS uab_principal,
    actividades_compatibles AS uab_compatible,
    actividades_condicionadas AS uab_condicionado,
    actividades_prohibidas AS uab_prohibido,
    (
        SELECT uab_identificador
        FROM estructura_intermedia.rfpn_uab_areareserva
        WHERE uab_nombre_reserva = 
            CASE 
                WHEN area_reserva ILIKE 'Rio Nare' THEN 'Río Nare'
                WHEN area_reserva ILIKE 'Bosque Oriental de Bogotá' THEN 'Bosque Oriental de Bogotá'
                WHEN area_reserva ILIKE 'Quebrada Honda y Caños Parrado ' THEN 'Quebrada Honda y Caños Parrado y Buque'
                ELSE area_reserva
            END
        LIMIT 1
    ) AS rfpn_uab_area_reserva_regimen_actividades,
    CASE 
        WHEN zonificacion ILIKE 'Zona de preservación' THEN 'Preservacion'
        WHEN zonificacion ILIKE 'Zona de restauración' THEN 'Restauracion'
        WHEN zonificacion ILIKE 'Zona de uso sostenible' THEN 'Uso_Sostenible'
        WHEN zonificacion ILIKE 'Zona de uso público' THEN 'Uso_Publico'
        ELSE 'Otro'
    END AS rfpn_uab_zonificacion_regimen_actividades
FROM insumos.regimen_usos;

---------------------------------------------------------------
-- ZONIFICACIÓN RFPN
---------------------------------------------------------------
INSERT INTO estructura_intermedia.rfpn_uab_zonificacion (
    uab_identificador, 
    uab_tipo_zona, 
    uab_detalle_zona,
    uab_regimen_usos,
    uab_areareserva, 
    uab_nombre, 
    ue_geometria, 
    interesado_nombre, 
    interesado_tipo_interesado, 
    interesado_tipo_documento, 
    interesado_numero_documento, 
    fuente_administrativa_tipo, 
    fuente_administrativa_estado_disponibilidad, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_nombre, 
    ddr_tipo_resposabilidad, 
    ddr_tipo_derecho
)
WITH datos_union AS (
    -- Homologación para Río Nare
    SELECT 
        NULL AS uab_identificador,
        CASE
            WHEN zona_julio = 'Restauracion' THEN 'Restauracion'
            WHEN zona_julio = 'Uso Sostenible_Desarrollo' THEN 'Uso_Sostenible.Desarrollo'
            WHEN zona_julio = 'Preservacion' THEN 'Preservacion'
            WHEN zona_julio = 'Uso_Sostenible_apvto' THEN 'Uso_Sostenible.Aprovechamiento_Sostenible'
            WHEN zona_julio = 'Uso_Publico.Uso_Publico' THEN 'Uso_Publico.Uso_Publico'
            WHEN zona_julio = 'Uso_Sostenible.Uso_Sostenible' THEN 'Uso_Sostenible.Uso_Sostenible'
            WHEN zona_julio = 'Uso_Publico.Alta_Densidad_De_Uso' THEN 'Uso_Publico.Alta_Densidad_De_Uso'
            WHEN zona_julio = 'Uso_Publico.Recreacion' THEN 'Uso_Publico.Recreacion'
            ELSE 'Otro'
        END AS uab_tipo_zona,
        CASE
            WHEN zona_julio NOT IN (
                'Restauracion', 
                'Uso Sostenible_Desarrollo', 
                'Preservacion', 
                'Uso_Sostenible_apvto',
                'Uso_Publico.Uso_Publico', 
                'Uso_Sostenible.Uso_Sostenible', 
                'Uso_Publico.Alta_Densidad_De_Uso', 
                'Uso_Publico.Recreacion'
            ) THEN zona_julio
            ELSE NULL
        END AS uab_detalle_zona,
        COALESCE(( 
            SELECT uab_identificador
            FROM estructura_intermedia.rfpn_regimenusosactividades
            WHERE rfpn_uab_area_reserva_regimen_actividades = 
                  (SELECT uab_identificador 
                   FROM estructura_intermedia.rfpn_uab_areareserva 
                   WHERE LOWER(TRIM(uab_nombre_reserva)) = 'río nare' LIMIT 1)
              AND rfpn_uab_zonificacion_regimen_actividades = 
                  CASE
                      WHEN zona_julio = 'Restauracion' THEN 'Restauracion'
                      WHEN zona_julio = 'Uso Sostenible_Desarrollo' THEN 'Uso_Sostenible'
                      WHEN zona_julio = 'Preservacion' THEN 'Preservacion'
                      WHEN zona_julio = 'Uso_Sostenible_apvto' THEN 'Uso_Sostenible'
                      WHEN zona_julio = 'Uso_Publico.Uso_Publico' THEN 'Uso_Publico'
                      WHEN zona_julio = 'Uso_Sostenible.Uso_Sostenible' THEN 'Uso_Sostenible'
                      WHEN zona_julio = 'Uso_Publico.Alta_Densidad_De_Uso' THEN 'Uso_Publico'
                      WHEN zona_julio = 'Uso_Publico.Recreacion' THEN 'Uso_Publico'
                      ELSE 'Otro'
                  END
            LIMIT 1
        ), 'Sin información') AS uab_regimen_usos,
        (SELECT uab_identificador 
         FROM estructura_intermedia.rfpn_uab_areareserva 
         WHERE LOWER(TRIM(uab_nombre_reserva)) = 'río nare'
         LIMIT 1) AS uab_areareserva,
        'rfpn_uab_zonificacion' AS uab_nombre,
        ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
        'El Ministerio de Ambiente y Desarrollo Sostenible' AS interesado_nombre, 
        'Persona_Juridica' AS interesado_tipo_interesado, 
        'NIT' AS interesado_tipo_documento, 
        '830.115.395-1' AS interesado_numero_documento, 
        'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
        'Disponible' AS fuente_administrativa_estado_disponibilidad, 
        'Documento' AS fuente_administrativa_tipo_formato, 
        '2010-08-05'::date AS fuente_administrativa_fecha_documento_fuente,
        'Resolucion 1510 de 2010' AS fuente_administrativa_nombre,
        'Zonificar' AS ddr_tipo_resposabilidad,
        NULL AS ddr_tipo_derecho
    FROM insumos.zonificacion_rio_nare  

    UNION ALL
    -- Homologación para Bosques Orientales
    SELECT 
        NULL AS uab_identificador,
        CASE
            WHEN zona = 'Zona de Preservación' THEN 'Preservacion'
            WHEN zona = 'Zona de Restauración' THEN 'Restauracion'
            WHEN zona = 'Zona general de uso Público' THEN 'Uso_Publico.Uso_Publico'
            WHEN zona = 'Zona de uso Sostenible' THEN 'Uso_Sostenible.Uso_Sostenible'
            ELSE 'Otro'
        END AS uab_tipo_zona,
        CASE
            WHEN zona NOT IN (
                'Zona de Preservación',
                'Zona de uso Sostenible', 
                'Zona de Restauración', 
                'Zona general de uso Público', 
                'Zona de Uso Sostenible'
            ) THEN zona
            ELSE NULL
        END AS uab_detalle_zona,
        COALESCE(( 
            SELECT uab_identificador 
            FROM estructura_intermedia.rfpn_regimenusosactividades
            WHERE rfpn_uab_area_reserva_regimen_actividades = 
                  (SELECT uab_identificador 
                   FROM estructura_intermedia.rfpn_uab_areareserva 
                   WHERE LOWER(TRIM(uab_nombre_reserva)) = 'bosque oriental de bogotá' LIMIT 1)
              AND rfpn_uab_zonificacion_regimen_actividades = 
                  CASE
                      WHEN zona = 'Zona de Preservación' THEN 'Preservacion'
                      WHEN zona = 'Zona de Restauración' THEN 'Restauracion'
                      WHEN zona = 'Zona general de uso Público' THEN 'Uso_Publico'
                      WHEN zona = 'Zona de Uso Sostenible' THEN 'Uso_Sostenible'
                      ELSE 'Otro'
                  END
            LIMIT 1
        ), 'Sin información') AS uab_regimen_usos,
        (SELECT uab_identificador 
         FROM estructura_intermedia.rfpn_uab_areareserva 
         WHERE LOWER(TRIM(uab_nombre_reserva)) = 'bosque oriental de bogotá'
         LIMIT 1) AS uab_areareserva,
        'rfpn_uab_zonificacion' AS uab_nombre,
        ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
        'El Ministerio de Ambiente y Desarrollo Sostenible' AS interesado_nombre, 
        'Persona_Juridica' AS interesado_tipo_interesado, 
        'NIT' AS interesado_tipo_documento, 
        '830.115.395-1' AS interesado_numero_documento, 
        'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
        'Disponible' AS fuente_administrativa_estado_disponibilidad, 
        'Documento' AS fuente_administrativa_tipo_formato, 
        '2016-10-27'::date AS fuente_administrativa_fecha_documento_fuente,
        'Resolucion 1766 de 2016' AS fuente_administrativa_nombre,
        'Zonificar' AS ddr_tipo_resposabilidad,
        NULL AS ddr_tipo_derecho
    FROM insumos.zonificacion_bosque_oriental_de_bogota

    UNION ALL 
    -- Homologación para Quebrada Honda
    SELECT 
        NULL AS uab_identificador,
        CASE
            WHEN zona = 'Zona de Preservación' THEN 'Preservacion'
            WHEN zona = 'Zona de Restauración' THEN 'Restauracion'
            WHEN zona = 'Zona de Uso Sostenible' THEN 'Uso_Sostenible.Uso_Sostenible'
            WHEN zona = 'Zona de Uso Público' THEN 'Uso_Publico.Uso_Publico'
            ELSE 'Otro'
        END AS uab_tipo_zona,
        CASE
            WHEN zona NOT IN (
                'Zona de Preservación', 
                'Zona de Restauración', 
                'Zona de Uso Sostenible', 
                'Zona de Uso Público'
            ) THEN zona
            ELSE NULL
        END AS uab_detalle_zona,
        COALESCE(( 
            SELECT uab_identificador 
            FROM estructura_intermedia.rfpn_regimenusosactividades
            WHERE rfpn_uab_area_reserva_regimen_actividades = 
                  (SELECT uab_identificador 
                   FROM estructura_intermedia.rfpn_uab_areareserva 
                   WHERE LOWER(TRIM(uab_nombre_reserva)) = 'quebrada honda y caños parrado y buque' LIMIT 1)
              AND rfpn_uab_zonificacion_regimen_actividades= 
                  CASE
                      WHEN zona = 'Zona de Preservación' THEN 'Preservacion'
                      WHEN zona = 'Zona de Restauración' THEN 'Restauracion'
                      WHEN zona = 'Zona de Uso Sostenible' THEN 'Uso_Sostenible'
                      WHEN zona = 'Zona de Uso Público' THEN 'Uso_Publico'
                      ELSE 'Otro'
                  END
            LIMIT 1
        ), 'Sin información') AS uab_regimen_usos,
        (SELECT uab_identificador 
         FROM estructura_intermedia.rfpn_uab_areareserva 
         WHERE LOWER(TRIM(uab_nombre_reserva)) = 'quebrada honda y caños parrado y buque'
         LIMIT 1) AS uab_areareserva,
        'rfpn_uab_zonificacion' AS uab_nombre,
        ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
        'El Ministerio de Ambiente y Desarrollo Sostenible' AS interesado_nombre, 
        'Persona_Juridica' AS interesado_tipo_interesado, 
        'NIT' AS interesado_tipo_documento, 
        '830.115.395-1' AS interesado_numero_documento, 
        'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
        'Disponible' AS fuente_administrativa_estado_disponibilidad, 
        'Documento' AS fuente_administrativa_tipo_formato, 
        '2014-11-04'::date AS fuente_administrativa_fecha_documento_fuente,
        'Resolucion 1762 de 2014' AS fuente_administrativa_nombre,
        'Zonificar' AS ddr_tipo_resposabilidad,
        NULL AS ddr_tipo_derecho
    FROM insumos.zonificacion_quebrada_honda_y_canos_parrado_y_buque
)
SELECT 
    ('ZNRFPN_' || ROW_NUMBER() OVER (ORDER BY uab_tipo_zona))::varchar(255) AS uab_identificador,
    uab_tipo_zona,
    uab_detalle_zona,
    uab_regimen_usos,
    uab_areareserva,
    uab_nombre,
    ue_geometria,
    interesado_nombre,
    interesado_tipo_interesado,
    interesado_tipo_documento,
    interesado_numero_documento,
    fuente_administrativa_tipo,
    fuente_administrativa_estado_disponibilidad,
    fuente_administrativa_tipo_formato,
    fuente_administrativa_fecha_documento_fuente,
    fuente_administrativa_nombre,
    ddr_tipo_resposabilidad,
    ddr_tipo_derecho
FROM datos_union;

---------------------------------------------------------------
-- COMPENSACIÓN RFPN
---------------------------------------------------------------
INSERT INTO estructura_intermedia.rfpn_uab_compensacion(
    uab_identificador, 
    uab_expediente, 
    uab_observaciones, 
    uab_areareserva, 
    uab_sustraccion,
    uab_nombre, 
    ue_geometria, 
    interesado_nombre, 
    interesado_tipo_interesado, 
    interesado_tipo_documento, 
    interesado_numero_documento, 
    fuente_administrativa_tipo, 
    fuente_administrativa_estado_disponibilidad, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_nombre, 
    ddr_tipo_resposabilidad, 
    ddr_tipo_derecho
)
SELECT 
    ('CMRFPN_' || ROW_NUMBER() OVER ())::varchar(255) AS uab_identificador,
    expediente AS uab_expediente,
    proyecto AS uab_observaciones,
    UPPER(id_reserva)::varchar(255) AS uab_areareserva,
    NULL AS uab_sustraccion,
    'rfpn_uab_compensacion' AS uab_nombre,
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
    'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre, 
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
    'NIT'::varchar(255) AS interesado_tipo_documento, 
    '830.115.395-1'::varchar(255) AS interesado_numero_documento,
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
    fecha_acto AS fuente_administrativa_fecha_documento_fuente,
    replace(
      CASE 
          WHEN acto_admin ILIKE '%Resoluci%' THEN 
              regexp_replace(acto_admin, 'Resoluci[óÓ]n', 'Resolucion', 'gi')
          ELSE acto_admin
      END::varchar(255),
      'Actos',
      'Acto'
    ) AS fuente_administrativa_nombre,
    'Compensar'::varchar(255) AS ddr_tipo_resposabilidad,
    NULL AS ddr_tipo_derecho
FROM insumos.compensaciones
WHERE LEFT(id_reserva, 4) = 'RFPN';

---------------------------------------------------------------
-- SUSTRACCIÓN RFPN
---------------------------------------------------------------
WITH sustracciones_definitivas_clean AS (
    SELECT 
        ('SDRFPN_' || ROW_NUMBER() OVER ())::varchar(255) AS uab_identificador,
        CASE 
            WHEN acto_admin = 'Res 194 de 2014 Modifica Res 1859 de 2009' THEN 'Resolucion 194 de 2014/Resolucion 1859 de 2009'
            WHEN acto_admin = 'Resolucion  1462 de 2018' THEN 'Resolucion 1462 de 2018'
            WHEN acto_admin LIKE 'Res %' THEN REPLACE(acto_admin, 'Res', 'Resolucion')
            ELSE acto_admin
        END AS uab_acto_admin,
        CASE 
            WHEN sector::integer = 1 THEN 'Infraestructura_Transporte'
            WHEN sector::integer = 2 THEN 'Mineria'
            WHEN sector::integer = 3 THEN 'Energia'
            WHEN sector::integer = 4 THEN 'Hidrocarburos'
            WHEN sector::integer = 5 THEN 'Area_Urbana_Expansion_Rural'
            WHEN sector::integer = 6 THEN 'Vivienda_VIS_VIP'
            WHEN sector::integer = 7 THEN 'Restitucion_Tierras'
            WHEN sector::integer = 8 THEN 'Reforma_Agraria'
            WHEN sector::integer = 9 THEN 'Inciso_Segundo'
            ELSE 'Otro'
        END AS uab_tipo_sector,
        geom,
        expediente,
        sector,
        detalle,
        rfpnn AS uab_nombre_areareserva,
        solicitant
    FROM insumos.sustracciones
)
INSERT INTO estructura_intermedia.rfpn_uab_sustraccion(
    uab_identificador, 
    uab_tipo_sustraccion,
    uab_fin_sustraccion, 
    uab_tipo_sector, 
    uab_detalle_sector, 
    uab_observaciones, 
    uab_expediente, 
    uab_areareserva,
    uab_nombre_areareserva,
    uab_compensacion, 
    uab_nombre, 
    ue_geometria, 
    uab_solicitante,
    interesado_nombre,
    interesado_tipo_interesado, 
    interesado_tipo_documento, 
    interesado_numero_documento, 
    fuente_administrativa_tipo, 
    fuente_administrativa_estado_disponibilidad, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_nombre,
    ddr_tipo_resposabilidad,
    ddr_tipo_derecho
)
SELECT 
    s.uab_identificador,
    'Definitiva'::varchar(255) AS uab_tipo_sustraccion,
    NULL AS uab_fin_sustraccion,
    s.uab_tipo_sector,
    CASE 
        WHEN s.uab_tipo_sector = 'Otro' THEN s.sector::TEXT
        ELSE NULL
    END AS uab_detalle_sector,
    s.detalle AS uab_observaciones,
    COALESCE(s.expediente, 'Sin información') AS uab_expediente,
    CASE
        WHEN s.uab_nombre_areareserva = 'Bosque Oriental de Bogotá' THEN 'RFPN_01'
        WHEN s.uab_nombre_areareserva = 'Cuenca Alta del Río Mocoa' THEN 'RFPN_11'
        WHEN s.uab_nombre_areareserva = 'La Elvira' THEN 'RFPN_25'
        WHEN s.uab_nombre_areareserva = 'Laguna La Cocha Cerro Patascoy' THEN 'RFPN_27'
        WHEN s.uab_nombre_areareserva = 'Páramo Grande' THEN 'RFPN_09'
        WHEN s.uab_nombre_areareserva = 'Quebrada Honda y Caños Parrado y Buque' THEN 'RFPN_56'
        WHEN s.uab_nombre_areareserva = 'Río Cali' THEN 'RFPN_10'
        WHEN s.uab_nombre_areareserva = 'Río Dagua' THEN 'RFPN_42'
        WHEN s.uab_nombre_areareserva = 'Río Meléndez' THEN 'RFPN_44'
        WHEN s.uab_nombre_areareserva = 'Río Nare' THEN 'RFPN_58'
        WHEN s.uab_nombre_areareserva = 'Río San Francisco' THEN 'RFPN_49'
        ELSE NULL
    END AS uab_areareserva,
    s.uab_nombre_areareserva,
    NULL AS uab_compensacion,
    'rfpn_uab_sustraccion' AS uab_nombre,
    ST_Force3D(ST_Transform(s.geom, 9377)) AS ue_geometria,
    COALESCE(s.solicitant, 'Sin información') AS uab_solicitante,
    'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre,
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado,
    'NIT'::varchar(255) AS interesado_tipo_documento,
    '830.115.395-1'::varchar(255) AS interesado_numero_documento, 
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad,
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato,
    NULL AS fuente_administrativa_fecha_documento_fuente,
    replace(
        CASE
            WHEN s.uab_acto_admin ILIKE '%Resoluci%' THEN regexp_replace(s.uab_acto_admin, 'Resoluci[oóÓ]n', 'Resolucion', 'gi')
            ELSE s.uab_acto_admin
        END::varchar(255),
        'Actos', 'Acto'
    ) AS fuente_administrativa_nombre,
    NULL AS ddr_tipo_resposabilidad,
    'Sustraer'::varchar(255) AS ddr_tipo_derecho
FROM sustracciones_definitivas_clean s;

---------------------------------------------------------------
-- FUENTES ADMINISTRATIVAS DE ÁREAS DE RESERVA
---------------------------------------------------------------
INSERT INTO estructura_intermedia.rfpn_fuenteadministrativa(
    uab_identificador, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_numero, 
    fuente_administrativa_anio
)
WITH areareserva_acto AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        SUBSTRING(cl.fuente_administrativa_nombre, 1, POSITION(' ' IN cl.fuente_administrativa_nombre) - 1) AS fuente_administrativa_tipo,
        SUBSTRING(cl.fuente_administrativa_nombre, POSITION(' ' IN cl.fuente_administrativa_nombre) + 1, POSITION(' DE ' IN UPPER(cl.fuente_administrativa_nombre)) - POSITION(' ' IN cl.fuente_administrativa_nombre)) AS numero_acto,
        SUBSTRING(cl.fuente_administrativa_nombre, POSITION(' DE ' IN UPPER(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
    FROM estructura_intermedia.rfpn_uab_areareserva cl
)
SELECT 
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE 
        WHEN fuente_administrativa_tipo ILIKE 'Resolución' THEN 'Resolucion'
        WHEN fuente_administrativa_tipo ILIKE 'Acuerdos' THEN 'Acuerdo'
        ELSE fuente_administrativa_tipo
    END AS fuente_administrativa_tipo,
    estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_numero,
    estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM areareserva_acto;

---------------------------------------------------------------
-- FUENTES ADMINISTRATIVAS DE ZONIFICACIÓN
---------------------------------------------------------------
INSERT INTO estructura_intermedia.rfpn_fuenteadministrativa(
    uab_identificador, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_numero, 
    fuente_administrativa_anio
)
WITH zonificacion_acto AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        SUBSTRING(cl.fuente_administrativa_nombre, 1, POSITION(' ' IN cl.fuente_administrativa_nombre) - 1) AS tipo_acto,
        SUBSTRING(cl.fuente_administrativa_nombre, POSITION(' ' IN cl.fuente_administrativa_nombre) + 1, POSITION(' DE ' IN UPPER(cl.fuente_administrativa_nombre)) - POSITION(' ' IN cl.fuente_administrativa_nombre)) AS numero_acto,
        SUBSTRING(cl.fuente_administrativa_nombre, POSITION(' DE ' IN UPPER(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
    FROM estructura_intermedia.rfpn_uab_zonificacion cl
)
SELECT 
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE 
        WHEN tipo_acto = 'Resolución' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Acuerdos' THEN 'Acuerdo'
        ELSE tipo_acto
    END AS fuente_administrativa_tipo,
    estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_acto,
    estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM zonificacion_acto;

---------------------------------------------------------------
-- FUENTES ADMINISTRATIVAS DE SUSTRACCIONES
---------------------------------------------------------------
INSERT INTO estructura_intermedia.rfpn_fuenteadministrativa(
    uab_identificador, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_numero, 
    fuente_administrativa_anio
)
WITH split_acto_sustraccion AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        cl.fuente_administrativa_nombre AS acto,
        TRIM(UNNEST(string_to_array(cl.fuente_administrativa_nombre, '/'))) AS fuente_administrativa_nombre
    FROM estructura_intermedia.rfpn_uab_sustraccion cl   
), homologado_acto_sustraccion AS (
    SELECT 
        uab_identificador,
        acto,
        fuente_administrativa_fecha_documento_fuente,
        REGEXP_REPLACE(fuente_administrativa_nombre, '[^A-Za-z0-9 ]', '', 'g') AS fuente_administrativa_nombre
    FROM split_acto_sustraccion
), sustraccion_acto AS (
    SELECT 
        uab_identificador,
        acto,
        fuente_administrativa_fecha_documento_fuente,
        SUBSTRING(cl.fuente_administrativa_nombre, 1, POSITION(' ' IN cl.fuente_administrativa_nombre) - 1) AS tipo_acto,
        SUBSTRING(cl.fuente_administrativa_nombre, POSITION(' ' IN cl.fuente_administrativa_nombre) + 1, POSITION(' DE ' IN UPPER(cl.fuente_administrativa_nombre)) - POSITION(' ' IN cl.fuente_administrativa_nombre)) AS numero_acto,
        SUBSTRING(cl.fuente_administrativa_nombre, POSITION(' DE ' IN UPPER(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
    FROM homologado_acto_sustraccion cl  
)
SELECT 
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE 
        WHEN tipo_acto ILIKE 'Resolucíon' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Resolución' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Resoluciión' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Res' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Acuerdos' THEN 'Acuerdo'
        ELSE tipo_acto
    END AS fuente_administrativa_tipo,
    estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_numero,
    estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM sustraccion_acto;

---------------------------------------------------------------
-- FUENTES ADMINISTRATIVAS DE COMPENSACIONES
---------------------------------------------------------------
INSERT INTO estructura_intermedia.rfpn_fuenteadministrativa(
    uab_identificador, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_numero, 
    fuente_administrativa_anio
)
WITH split_acto_compensacion AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        cl.fuente_administrativa_nombre AS acto,
        TRIM(UNNEST(string_to_array(cl.fuente_administrativa_nombre, '/'))) AS fuente_administrativa_nombre
    FROM estructura_intermedia.rfpn_uab_compensacion cl   
), homologado_acto_compensacion AS (
    SELECT 
        uab_identificador,
        acto,
        fuente_administrativa_fecha_documento_fuente,
        TRIM(REGEXP_REPLACE(fuente_administrativa_nombre, '\s+', ' ', 'g')) AS fuente_administrativa_nombre
    FROM split_acto_compensacion
), compensacion_acto AS (
    SELECT 
        uab_identificador,
        acto,
        fuente_administrativa_fecha_documento_fuente,
        TRIM(SPLIT_PART(cl.fuente_administrativa_nombre, ' ', 2)) AS numero_acto,
        TRIM(SPLIT_PART(UPPER(cl.fuente_administrativa_nombre), ' DE ', 2)) AS anio_acto,
        SUBSTRING(cl.fuente_administrativa_nombre, 1, POSITION(' ' IN cl.fuente_administrativa_nombre) - 1) AS tipo_acto
    FROM homologado_acto_compensacion cl  
)
SELECT 
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE 
        WHEN tipo_acto ILIKE 'Resolucn' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Resolucin' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Resolución' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Resolucón' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Res' THEN 'Resolucion'
        WHEN tipo_acto ILIKE 'Acuerdos' THEN 'Acuerdo'
        ELSE tipo_acto
    END AS fuente_administrativa_tipo,
    CASE 
        WHEN numero_acto ~ '^\d+$' THEN estructura_intermedia.homologar_numero(numero_acto::text)::bigint
        ELSE NULL
    END AS fuente_administrativa_numero,
    CASE 
        WHEN anio_acto ~ '^\d{4}$' THEN estructura_intermedia.homologar_numero(anio_acto::text)::int4
        ELSE NULL
    END AS fuente_administrativa_anio
FROM compensacion_acto;

---------------------------------------------------------------
-- RELACIONAR COMPENSACIONES A LAS SUSTRACCIONES
---------------------------------------------------------------
WITH fuente_s AS (
    SELECT *
    FROM estructura_intermedia.rfpn_fuenteadministrativa
    WHERE LEFT(uab_identificador, 1) = 'S'
), fuente_c AS (
    SELECT *
    FROM estructura_intermedia.rfpn_fuenteadministrativa
    WHERE LEFT(uab_identificador, 1) = 'C'
), fuente_sustraccion_compensacion AS (
    SELECT DISTINCT 
        s.uab_identificador AS uab_sustraccion,
        c.uab_identificador AS uab_compensacion
    FROM fuente_c c  
    LEFT JOIN fuente_s s
        ON s.fuente_administrativa_tipo_formato = c.fuente_administrativa_tipo_formato
        AND s.fuente_administrativa_numero = c.fuente_administrativa_numero
        AND s.fuente_administrativa_anio = c.fuente_administrativa_anio
    WHERE s.uab_identificador IS NOT NULL
), asigna_expendiente_sustraccion AS (
    SELECT c.*, uc.uab_expediente
    FROM fuente_sustraccion_compensacion c
    LEFT JOIN estructura_intermedia.rfpn_uab_compensacion uc
        ON c.uab_compensacion = uc.uab_identificador
)
UPDATE estructura_intermedia.rfpn_uab_sustraccion
SET uab_compensacion = sc.uab_compensacion,
    uab_expediente = sc.uab_expediente
FROM asigna_expendiente_sustraccion sc
WHERE sc.uab_sustraccion = rfpn_uab_sustraccion.uab_identificador;

---------------------------------------------------------------
-- RELACIONAR SUSTRACCIONES A LAS COMPENSACIONES
---------------------------------------------------------------
WITH fuente_s AS (
    SELECT *
    FROM estructura_intermedia.rfpn_fuenteadministrativa
    WHERE LEFT(uab_identificador, 1) = 'S'
), fuente_c AS (
    SELECT *
    FROM estructura_intermedia.rfpn_fuenteadministrativa
    WHERE LEFT(uab_identificador, 1) = 'C'
), fuente_sustraccion_compensacion AS (
    SELECT DISTINCT 
        s.uab_identificador AS uab_sustraccion,
        c.uab_identificador AS uab_compensacion
    FROM fuente_s s
    LEFT JOIN fuente_c c
        ON s.fuente_administrativa_tipo_formato = c.fuente_administrativa_tipo_formato
        AND s.fuente_administrativa_numero = c.fuente_administrativa_numero
        AND s.fuente_administrativa_anio = c.fuente_administrativa_anio
    WHERE s.uab_identificador IS NOT NULL
), asigna_solicitante_compensacion AS (
    SELECT c.*, s.interesado_nombre
    FROM fuente_sustraccion_compensacion c 
    LEFT JOIN estructura_intermedia.rfpn_uab_sustraccion s 
        ON c.uab_sustraccion = s.uab_identificador
)
UPDATE estructura_intermedia.rfpn_uab_compensacion
SET uab_sustraccion = sc.uab_sustraccion,
    interesado_nombre = sc.interesado_nombre
FROM asigna_solicitante_compensacion sc
WHERE sc.uab_compensacion = rfpn_uab_compensacion.uab_identificador;

---------------------------------------------------------------
-- Se eliminan sustracciones que tienen relacionada dos o más compensaciones
---------------------------------------------------------------
DELETE
FROM estructura_intermedia.rfpn_uab_sustraccion
WHERE uab_compensacion IN (
    SELECT uab_compensacion
    FROM estructura_intermedia.rfpn_uab_sustraccion
    WHERE uab_compensacion IS NOT NULL
    GROUP BY uab_compensacion
    HAVING COUNT(*) > 1
);

---------------------------------------------------------------
-- Eliminan compensaciones que no tienen relacionada sustracción
---------------------------------------------------------------
DELETE 
FROM estructura_intermedia.rfpn_uab_compensacion
WHERE uab_sustraccion NOT IN (
    SELECT uab_identificador
    FROM estructura_intermedia.rfpn_uab_sustraccion 
) 
   OR uab_sustraccion IS NULL 
   OR uab_sustraccion IN (
    SELECT uab_sustraccion
    FROM estructura_intermedia.rfpn_uab_compensacion
    GROUP BY uab_sustraccion
    HAVING COUNT(*) > 1
);



    """

    return sql_script



def validar_estructura():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/****************************
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración del RFPN  al modelo LADM_COL-RFPN
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 *************************/
/*************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 ****************************/

--========================================
--Fijar esquema
--========================================
set search_path to 
	estructura_intermedia, -- Esquema de estructura de datos intermedia
	ladm,
	public;


--========================================
-- Área de reserva RFPN
--========================================

SELECT *
FROM estructura_intermedia.rfpn_uab_areareserva;

SELECT count(*)
FROM estructura_intermedia.rfpn_uab_areareserva;
--7 area de Reserva

SELECT fuente_administrativa_tipo_formato, COUNT(*)
FROM estructura_intermedia.rfpn_fuenteadministrativa
WHERE SUBSTR(uab_identificador,1,1) = 'R'
GROUP BY fuente_administrativa_tipo_formato;

--========================================
-- Zonificación RFPN
--========================================
select * 
FROM estructura_intermedia.rfpn_uab_zonificacion;

select count(*) 
FROM estructura_intermedia.rfpn_uab_zonificacion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rfpn_uab_zonificacion
group by uab_areareserva
order by uab_areareserva;

select uab_tipo_zona,count(*) 
FROM estructura_intermedia.rfpn_uab_zonificacion
group by uab_tipo_zona
order by uab_tipo_zona;

select uab_areareserva,uab_tipo_zona,count(*) 
FROM estructura_intermedia.rfpn_uab_zonificacion
group by uab_areareserva,uab_tipo_zona
order by uab_areareserva,uab_tipo_zona;
---Cuántos usos hay por área de reserva

SELECT 
    uab_areareserva, 
    COUNT(*) AS total_usos
FROM 
    estructura_intermedia.rfpn_uab_zonificacion
GROUP BY 
    uab_areareserva
ORDER BY 
    uab_areareserva;

--- Cuáles son los registros que tienen uab_detalle_zona
SELECT 
    uab_areareserva, 
    uab_tipo_zona, 
    uab_detalle_zona, 
    uab_regimen_usos
FROM 
    estructura_intermedia.rfpn_uab_zonificacion
WHERE 
    uab_detalle_zona IS NOT NULL
ORDER BY 
    uab_areareserva, uab_detalle_zona;

 ----Cuántos registros tienen uab_detalle_zona por área de reserva
SELECT 
    uab_areareserva, 
    COUNT(*) AS usos_con_detalle_zona
FROM 
    estructura_intermedia.rfpn_uab_zonificacion
WHERE 
    uab_detalle_zona IS NOT NULL
GROUP BY 
    uab_areareserva
ORDER BY 
    uab_areareserva;
   
--Cuántos registros tienen uab_tipo_zona por área de reserva
SELECT 
    uab_areareserva, 
    uab_tipo_zona, 
    COUNT(*) AS total_por_tipo_zona
FROM 
    estructura_intermedia.rfpn_uab_zonificacion
GROUP BY 
    uab_areareserva, uab_tipo_zona
ORDER BY 
    uab_areareserva, uab_tipo_zona;
   
--Cuáles son los uab_tipo_zona por área de reserva
SELECT 
    uab_areareserva, 
    ARRAY_AGG(DISTINCT uab_tipo_zona) AS tipos_zona
FROM 
    estructura_intermedia.rfpn_uab_zonificacion
GROUP BY 
    uab_areareserva
ORDER BY 
    uab_areareserva;

--cuantos regiemenes de uso no cuentan con zonificación
SELECT 
    uab_areareserva,
    uab_tipo_zona, 
    COUNT(*) AS cantidad
FROM estructura_intermedia.rfpn_uab_zonificacion
WHERE uab_regimen_usos = 'Sin información'
GROUP BY uab_areareserva, uab_tipo_zona
ORDER BY cantidad DESC;
------ los que no tienen régimens e eliminan 

DELETE FROM estructura_intermedia.rfpn_uab_zonificacion
WHERE uab_regimen_usos = 'Sin información';


--========================================
-- Compensación Ley 
--========================================
SELECT *		
FROM estructura_intermedia.rfpn_uab_compensacion;

select count(*) 
FROM estructura_intermedia.rfpn_uab_compensacion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rfpn_uab_compensacion
group by uab_areareserva
order by uab_areareserva;


select uab_sustraccion,count(*)
FROM estructura_intermedia.rfpn_uab_compensacion
group by uab_sustraccion
having count(*)>1;

select *
from estructura_intermedia.rfpn_uab_compensacion
where uab_sustraccion is null;


select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rfpn_fuenteadministrativa
where left(uab_identificador,1) ='C'
group by fuente_administrativa_tipo_formato;

--========================================
-- Se listan Compensaciones que no tienen relacionada sustraccion, se relacionan en la Bitacora
--========================================
select *
from estructura_intermedia.rfpn_uab_compensacion
where uab_sustraccion is null;



--========================================
-- Sustracción RFPN
--========================================
SELECT *		
FROM estructura_intermedia.rfpn_uab_sustraccion;

select count(*) 
FROM estructura_intermedia.rfpn_uab_sustraccion;

select uab_tipo_sustraccion,count(*) 
FROM estructura_intermedia.rfpn_uab_sustraccion
group by uab_tipo_sustraccion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rfpn_uab_sustraccion
group by uab_areareserva
order by uab_areareserva;
--5 sustraciones con dos areas de reserva, RFPN_42 TIENE 10, RFPN_56 TIENE 4, RFPN_58 TIENE 3, RFPN_10 TIUENE 2, TIENE 2 RFPN_12


select uab_compensacion ,count(*)
from estructura_intermedia.rfpn_uab_sustraccion
group by uab_compensacion 
order by count(*) desc;

select *
from estructura_intermedia.rfpn_uab_sustraccion
where uab_compensacion in (
select uab_compensacion
from estructura_intermedia.rfpn_uab_sustraccion
where uab_compensacion is not null
group by uab_compensacion
having count(*)>1
);

select uab_compensacion, count(*)
from ladm.rfpn_uab_sustraccion rus 
where uab_compensacion is not null 
group by rus.uab_compensacion 
having count(*) > 1;

select rd.* 
from ladm.rfpn_derecho rd
left join (
	select * from ladm.col_rrrfuente 
	where rrr_rfpn_responsabilidad is null
) as cr 
on rd.t_id = cr.rrr_rfpn_derecho 
where cr.t_id is null;

select c.t_id,s.t_id,c.local_id 
from ladm.rfpn_uab_compensacion c
left join ladm.rfpn_uab_sustraccion s
on c.t_id =s.uab_compensacion 
where s.t_id is null;


select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rfpn_fuenteadministrativa
where left(uab_identificador,1) ='S'
group by fuente_administrativa_tipo_formato;
    """

    return sql_script

def importar_al_modelo():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    


/****************************
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración del rfpn al modelo LADM_COL-RPFN
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 *************************/
/*************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 ****************************/


/*SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'etl_rfpn' AND pid <> pg_backend_pid();

DROP SCHEMA ladm CASCADE;*/
 
--========================================
--Fijar esquema
--========================================
set search_path to
	estructura_intermedia,	-- Esquema de estructura de datos intermedia
	ladm,		-- Esquema modelo LADM-RFPN
	public;



--================================================================================
-- 1.Define Basket si este no existe
--================================================================================

--================================================================================
-- 1.Define Basket si este no existe
--================================================================================
INSERT INTO ladm.t_ili2db_dataset
(t_id, datasetname)
VALUES(1, 'Baseset');

INSERT INTO ladm.t_ili2db_basket(
	t_id, 
	dataset, 
	topic, 
	t_ili_tid, 
	attachmentkey, 
	domains)
VALUES(
	1,
	1, 
	'LADM_COL_v_1_0_0_Ext_RFPN.RFPN',
	uuid_generate_v4(),
	'ETL de importación de datos',
	NULL );


--================================================================================
-- 2. Migración de rfpn_interesado
--================================================================================
INSERT INTO ladm.rfpn_interesado(
	t_basket, 
	t_ili_tid, 
	observacion, 
	nombre, 
	tipo_interesado, 
	tipo_documento, 
	numero_documento, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)		
select 
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	null as observacion,
	interesado_nombre, 
	(select t_id from ladm.col_interesadotipo where ilicode like interesado_tipo_interesado) as tipo_interesado,
	(select t_id from ladm.col_documentotipo where ilicode like interesado_tipo_documento) as interesado_tipo_documento,
	interesado_numero_documento,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpn_interesado' as espacio_de_nombres, 
	row_number() OVER (ORDER BY interesado_nombre)   local_id
from (
	select 
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rfpn_uab_areareserva
	union
	select 
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rfpn_uab_sustraccion
	union
	select 
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rfpn_uab_compensacion
) t;


--================================================================================
-- 3. Migración de  Area de Reserva
--================================================================================
--3.1 diligenciamiento de la tabla  rfpn_uab_areareserva
INSERT INTO ladm.rfpn_uab_areareserva_runap(
	t_basket, 
	t_ili_tid, 
	id_ap,
	nombre_reserva, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	uab_id_ap::bigint,
	uab_nombre_reserva,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpn_uab_areareserva' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rfpn_uab_areareserva; 

--3.2 diligenciamiento de la tabla  rfpn_ue_areareserva+
INSERT INTO ladm.rfpn_ue_areareserva(
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	uab_nombre_reserva as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpn_ue_areareserva' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rfpn_uab_areareserva; 

--3.3 diligenciamiento de la tabla  rfpn_derecho para rfpn_uab_areareserva
INSERT INTO ladm.rfpn_derecho(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rfpn_interesado, 
	interesado_rfpn_agrupacioninteresados, 
	unidad_rfpn_uab_compensacion, 
	unidad_rfpn_uab_sustraccion, 
	unidad_rfpn_uab_zonificacion, 
	unidad_rfpn_uab_areareserva_runap, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.rfpn_derechotipo where ilicode like ddr_tipo_derecho) as tipo,
	null as descripcion,
	(select t_id from ladm.rfpn_interesado where nombre like interesado_nombre)  as interesado_rfpn_interesado, 
	null as interesado_rfpn_agrupacioninteresados, 
	null as unidad_rfpn_uab_compensacion, 
	null as unidad_rfpn_uab_sustraccion, 
	null as unidad_rfpn_uab_zonificacion, 
	(select t_id from ladm.rfpn_uab_areareserva_runap where local_id like uab_identificador) as unidad_rfpn_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpn_derecho' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rfpn_uab_areareserva; 


--3.4 diligenciamiento de la tabla rfpn_fuenteadministrativa para rfpn_uab_areareserva
INSERT INTO ladm.rfpn_fuenteadministrativa(
	t_basket, 
	t_ili_tid, 
	tipo, 
	fecha_fin, 
	estado_disponibilidad, 
	tipo_formato, 
	fecha_documento_fuente, 
	nombre, 
	descripcion, 
	url, 
	espacio_de_nombres, 
	local_id)	
select
		(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like a.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like a.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like a.fuente_administrativa_tipo_formato) as tipo_formato,
	a.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rfpp_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rfpn_fuenteadministrativa f,estructura_intermedia.rfpn_uab_areareserva a
where f.uab_identificador =a.uab_identificador;



--================================================================================
-- 4. Migración de  zonificacion
--================================================================================
--4.1 tabla de ladm.rfpn_regimenusosactividades
ALTER TABLE ladm.rfpn_regimenusosactividades
ADD COLUMN uab_identificacion VARCHAR(255);

INSERT INTO ladm.rfpn_regimenusosactividades(
    t_basket,
    t_ili_tid,
    uab_identificacion, -- Se incluye la nueva columna
    principal,
    compatible,
    condicionado,
    prohibido
)
SELECT 
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    uab_identificador AS uab_identificacion, -- Se asigna el identificador correcto
    uab_principal AS principal,
    uab_compatible AS compatible,
    uab_condicionado AS condicionado,
    uab_prohibido AS prohibido
FROM 
    estructura_intermedia.rfpn_regimenusosactividades;



--4.2 tabla de ladm.rfpn_uab_zonificacion
INSERT INTO ladm.rfpn_uab_zonificacion (
    t_basket,
    t_ili_tid,
    tipo_zona,
    detalle_zona,
    regimen_usos,
    uab_areareserva,
    nombre,
    tipo,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)

SELECT
    (SELECT t_id  
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN'
     LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id 
     FROM ladm.rfpn_zonificaciontipo 
     WHERE ilicode LIKE z.uab_tipo_zona
     LIMIT 1) AS tipo_zona,
    z.uab_detalle_zona,
    (SELECT t_id
     FROM ladm.rfpn_regimenusosactividades ra
     WHERE ra.uab_identificacion = z.uab_regimen_usos -- Ahora se usa el identificador correctamente
     LIMIT 1) AS regimen_usos,
    (SELECT t_id 
     FROM ladm.rfpn_uab_areareserva_runap
     WHERE nombre_reserva IN (
         SELECT a.uab_nombre_reserva 
         FROM estructura_intermedia.rfpn_uab_areareserva a 
         WHERE a.uab_identificador = z.uab_areareserva)
     LIMIT 1) AS uab_areareserva,
    z.uab_nombre,
    (SELECT t_id 
     FROM ladm.col_unidadadministrativabasicatipo 
     WHERE ilicode LIKE 'Ambiente_Desarrollo_Sostenible'
     LIMIT 1) AS tipo,
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'rfpn_uab_zonificacion' AS espacio_de_nombres,
    z.uab_identificador AS local_id
FROM  
    estructura_intermedia.rfpn_uab_zonificacion z;

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE ladm.rfpn_regimenusosactividades
DROP COLUMN uab_identificacion;


--4.3 diligenciamiento de la tabla  rfpn_ue_zonificacion
INSERT INTO ladm.rfpn_ue_zonificacion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	null as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpn_ue_zonificacion' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rfpn_uab_zonificacion; 


--4.4 diligenciamiento de la tabla  rfpn_responsabilidad para rfpn_uab_zonificacion
INSERT INTO ladm.rfpn_responsabilidad(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rfpn_interesado, 
	interesado_rfpn_agrupacioninteresados, 
	unidad_rfpn_uab_compensacion, 
	unidad_rfpn_uab_sustraccion, 
	unidad_rfpn_uab_zonificacion, 
	unidad_rfpn_uab_areareserva_runap,
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
SELECT
	(SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	(SELECT t_id FROM ladm.rfpn_responsabilidadtipo WHERE ilicode LIKE ddr_tipo_resposabilidad LIMIT 1) AS tipo,
	NULL AS descripcion,
	(SELECT t_id FROM ladm.rfpn_interesado WHERE nombre LIKE interesado_nombre LIMIT 1) AS interesado_rfpn_interesado, 
	NULL AS interesado_rfpn_agrupacioninteresados, 
	NULL AS unidad_rfpn_uab_compensacion, 
	NULL AS unidad_rfpn_uab_sustraccion, 
	(SELECT t_id FROM ladm.rfpn_uab_zonificacion WHERE local_id LIKE uab_identificador LIMIT 1) AS unidad_rfpn_uab_zonificacion, 
	NULL AS unidad_rfpn_uab_areareserva_runap,
	NOW() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'rfpn_responsabilidad' AS espacio_de_nombres, 
	uab_identificador AS local_id
FROM estructura_intermedia.rfpn_uab_zonificacion;


--4.5 diligenciamiento de la tabla rfpn_fuenteadministrativa para rfpn_uab_areareserva
INSERT INTO ladm.rfpn_fuenteadministrativa(
	t_basket, 
	t_ili_tid, 
	tipo, 
	fecha_fin, 
	estado_disponibilidad, 
	tipo_formato, 
	fecha_documento_fuente, 
	nombre, 
	descripcion, 
	url, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like a.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like a.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like a.fuente_administrativa_tipo_formato) as tipo_formato,
	a.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rfpp_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rfpn_fuenteadministrativa f,estructura_intermedia.rfpn_uab_zonificacion a
where f.uab_identificador =a.uab_identificador;


--================================================================================
-- 5. Migración de  compensacion
--================================================================================
INSERT INTO ladm.rfpn_uab_compensacion(
	t_basket, 
	t_ili_tid, 
	expediente, 
	observaciones, 
	uab_areareserva, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version,
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	c.uab_expediente,
	c.uab_observaciones,
	(select t_id from ladm.rfpn_uab_areareserva_runap where nombre_reserva in (select a.uab_nombre_reserva from estructura_intermedia.rfpn_uab_areareserva a where a.uab_identificador=c.uab_areareserva)) as tipo,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpn_uab_compensacion' as espacio_de_nombres, 
	c.uab_identificador as local_id
from  estructura_intermedia.rfpn_uab_compensacion c;	

--5.2 diligenciamiento de la tabla  rfpn_uab_compensacion
INSERT INTO ladm.rfpn_ue_compensacion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( st_area(ue_geometria)/10000 )::numeric(13, 4) as area_ha,
	null as etiqueta,
	( select t_id from ladm.col_relacionsuperficietipo 
	  where ilicode like 'En_Rasante' ) as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_ue_compensacion' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rfpn_uab_compensacion; 


--5.3 diligenciamiento de la tabla  rfpn_responsabilidad para rfpn_uab_compensacion
INSERT INTO ladm.rfpn_responsabilidad(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rfpn_interesado, 
	interesado_rfpn_agrupacioninteresados, 
	unidad_rfpn_uab_compensacion, 
	unidad_rfpn_uab_sustraccion, 
	unidad_rfpn_uab_zonificacion, 
	unidad_rfpn_uab_areareserva_runap, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
SELECT
	(SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	(SELECT t_id FROM ladm.rfpn_responsabilidadtipo WHERE ilicode LIKE ddr_tipo_resposabilidad LIMIT 1) AS tipo,
	NULL AS descripcion,
	(SELECT t_id FROM ladm.rfpn_interesado WHERE nombre LIKE interesado_nombre LIMIT 1) AS interesado_rfpn_interesado, 
	NULL AS interesado_rfpn_agrupacioninteresados, 
	(SELECT t_id FROM ladm.rfpn_uab_compensacion WHERE local_id LIKE uab_identificador LIMIT 1) AS unidad_rfpn_uab_compensacion, 
	NULL AS unidad_rfpn_uab_sustraccion, 
	NULL AS unidad_rfpn_uab_zonificacion, 
	NULL AS unidad_rfpn_uab_areareserva,
	NOW() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'rfpn_responsabilidad' AS espacio_de_nombres, 
	uab_identificador AS local_id
FROM estructura_intermedia.rfpn_uab_compensacion;


--5.4 diligenciamiento de la tabla rfpn_fuenteadministrativa para rfpn_uab_areareserva
INSERT INTO ladm.rfpn_fuenteadministrativa(
	t_basket, 
	t_ili_tid, 
	tipo, 
	fecha_fin, 
	estado_disponibilidad, 
	tipo_formato, 
	fecha_documento_fuente, 
	nombre, 
	descripcion, 
	url, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like a.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like a.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like a.fuente_administrativa_tipo_formato) as tipo_formato,
	a.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rfpp_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rfpn_fuenteadministrativa f,estructura_intermedia.rfpn_uab_compensacion a
where f.uab_identificador =a.uab_identificador;

--================================================================================
-- 6. Migración de sustraccion
--================================================================================
INSERT INTO ladm.rfpn_uab_sustraccion(
	t_basket, 
	t_ili_tid, 
	expediente,
	tipo_sustraccion, 
	fin_sustraccion, 
	tipo_sector, 
	detalle_sector, 
	observaciones, 
	uab_areareserva,
	uab_compensacion, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	s.uab_expediente,
	(select t_id from ladm.rfpn_sustraccionreservatipo where ilicode like s.uab_tipo_sustraccion) as tipo_sustraccion,
	s.uab_fin_sustraccion,
	(select t_id from ladm.rfpn_sectortipo where ilicode like s.uab_tipo_sector) as tipo_sector,
	s.uab_detalle_sector,
	s.uab_observaciones,
	(select t_id from ladm.rfpn_uab_areareserva_runap where local_id like s.uab_areareserva ) as uab_areareserva,
	(select t_id from ladm.rfpn_uab_compensacion where local_id like s.uab_compensacion ) as uab_compensacion,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpn_uab_sustraccion' as espacio_de_nombres, 
	s.uab_identificador as local_id
from  estructura_intermedia.rfpn_uab_sustraccion s;	

--6.2 diligenciamiento de la tabla  rfpn_ue_sustraccion
INSERT INTO ladm.rfpn_ue_sustraccion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	null as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpn_ue_sustraccion' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rfpn_uab_sustraccion ; 

--6.3 diligenciamiento de la tabla  rfpn_responsabilidad para rfpn_uab_sustraccion
INSERT INTO ladm.rfpn_derecho(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rfpn_interesado, 
	interesado_rfpn_agrupacioninteresados, 
	unidad_rfpn_uab_compensacion, 
	unidad_rfpn_uab_sustraccion, 
	unidad_rfpn_uab_zonificacion, 
	unidad_rfpn_uab_areareserva_runap, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
SELECT
	(SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,	
	(SELECT t_id FROM ladm.rfpn_derechotipo WHERE ilicode LIKE ddr_tipo_derecho LIMIT 1) AS tipo,
	NULL AS descripcion,
	(SELECT t_id FROM ladm.rfpn_interesado WHERE nombre LIKE interesado_nombre LIMIT 1) AS interesado_rfpn_interesado,
	NULL AS interesado_rfpn_agrupacioninteresados, 
	NULL AS unidad_rfpn_uab_compensacion, 
	(SELECT t_id FROM ladm.rfpn_uab_sustraccion WHERE local_id LIKE uab_identificador LIMIT 1) AS unidad_rfpn_uab_sustraccion, 
	NULL AS unidad_rfpn_uab_zonificacion, 
	NULL AS unidad_rfpn_uab_areareserva_runap,
	NOW() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'rfpn_derecho' AS espacio_de_nombres, 
	uab_identificador AS local_id
FROM estructura_intermedia.rfpn_uab_sustraccion;


--6.4 diligenciamiento de la tabla rfpn_fuenteadministrativa para rfpn_uab_areareserva
INSERT INTO ladm.rfpn_fuenteadministrativa(
	t_basket, 
	t_ili_tid, 
	tipo, 
	fecha_fin, 
	estado_disponibilidad, 
	tipo_formato, 
	fecha_documento_fuente, 
	nombre, 
	descripcion, 
	url, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like a.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like a.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like a.fuente_administrativa_tipo_formato) as tipo_formato,
	a.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rfpp_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rfpn_fuenteadministrativa f,estructura_intermedia.rfpn_uab_areareserva a
where f.uab_identificador =a.uab_identificador;


--================================================================================
-- 7. Migración de col_rrrfuente
--================================================================================
INSERT INTO ladm.col_rrrfuente(
	t_basket, 
	fuente_administrativa, 
	rrr_rfpn_derecho, 
	rrr_rfpn_responsabilidad)
select
(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
*
from (
	select distinct 
	f.t_id as fuente_administrativa,
	null::int8 as rrr_rfpn_derecho,
	r.t_id as rrr_rfpn_responsabilidad
	from ladm.rfpn_fuenteadministrativa f,
	ladm.rfpn_responsabilidad r
	where f.local_id=r.local_id 
	union  
	select distinct 
	f.t_id as fuente_administrativa,
	d.t_id as rrr_rfpn_derecho,
	null::int4 as rrr_rfpn_responsabilidad
	from ladm.rfpn_fuenteadministrativa f,
	ladm.rfpn_derecho d
	where f.local_id=d.local_id
) t;

--================================================================================
-- 8. Migración de col_uebaunit
--================================================================================
INSERT INTO ladm.col_uebaunit(
	t_basket, 
	ue_rfpn_ue_zonificacion, 
	ue_rfpn_ue_compensacion, 
	ue_rfpn_ue_sustraccion, 
	ue_rfpn_ue_areareserva, 
	baunit_rfpn_uab_zonificacion,
	baunit_rfpn_uab_compensacion, 
	baunit_rfpn_uab_sustraccion, 	 
	baunit_rfpn_uab_areareserva_runap)
select
(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPN.RFPN' limit 1) as t_basket,
*
from (
	select uez.t_id::int8 as ue_rfpn_ue_zonificacion,
	null::int8 as ue_rfpn_ue_compensacion,
	null::int8 as ue_rfpn_ue_sustraccion,
	null::int8 as ue_rfpn_ue_areareserva,
	uabz.t_id::int8 as baunit_rfpn_uab_zonificacion,
	null::int8 as baunit_rfpn_uab_compensacion,
	null::int8 as baunit_rfpn_uab_sustraccion,
	null::int8 as baunit_rfpn_uab_areareserva
	from ladm.rfpn_ue_zonificacion uez,
	ladm.rfpn_uab_zonificacion uabz
	where uez.local_id=uabz.local_id
	union	
	select null as ue_rfpn_ue_zonificacion,
	uez.t_id as ue_rfpn_ue_compensacion,
	null as ue_rfpn_ue_sustraccion,
	null as ue_rfpn_ue_areareserva,
	null as baunit_rfpn_uab_zonificacion,
	uabz.t_id as baunit_rfpn_uab_compensacion,
	null as baunit_rfpn_uab_sustraccion,
	null as baunit_rfpn_uab_areareserva
	from ladm.rfpn_ue_compensacion uez,
	ladm.rfpn_uab_compensacion uabz
	where uez.local_id=uabz.local_id
	union  
	select null as ue_rfpn_ue_zonificacion,
	null as ue_rfpn_ue_compensacion,
	uez.t_id as ue_rfpn_ue_sustraccion,
	null as ue_rfpn_ue_areareserva,
	null as baunit_rfpn_uab_zonificacion,
	null as baunit_rfpn_uab_compensacion,
	uabz.t_id as baunit_rfpn_uab_sustraccion,
	null as baunit_rfpn_uab_areareserva
	from ladm.rfpn_ue_sustraccion uez,
	ladm.rfpn_uab_sustraccion uabz
	where uez.local_id=uabz.local_id
	union  
	select null as ue_rfpn_ue_zonificacion,
	null as ue_rfpn_ue_compensacion,
	null as ue_rfpn_ue_sustraccion,
	uez.t_id as ue_rfpn_ue_areareserva,
	null as baunit_rfpn_uab_zonificacion,
	null as baunit_rfpn_uab_compensacion,
	null as baunit_rfpn_uab_sustraccion,
	uabz.t_id as baunit_rfpn_uab_areareserva
	from ladm.rfpn_ue_areareserva uez,
	ladm.rfpn_uab_areareserva_runap uabz
	where uez.local_id=uabz.local_id
) t;

    """

    return sql_script


