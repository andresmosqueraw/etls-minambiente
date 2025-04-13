def estructura_intermedia():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    

/***********************************************************************************
             Creación de estructura de datos intermedia 
        	Migración del HUMEDAL  al modelo LADM_COL-hmdr
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 ***************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 **********************************************************************************/

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
-- Agrupación Interesados
--=============================================
-- Crear tabla hmdr_interesado
CREATE TABLE hmdr_interesado (
    id VARCHAR(255) PRIMARY KEY, -- INT_NUMERO
    tipo VARCHAR(255),
    observacion VARCHAR(255),
    nombre VARCHAR(255),
    tipo_interesado VARCHAR(255),
    tipo_documento VARCHAR(255),
    numero_documento VARCHAR(255)
);

-- Crear tabla hmdr_agrupacioninteresados
CREATE TABLE hmdr_agrupacioninteresados (
    id VARCHAR(255) PRIMARY KEY, -- AGR_numero
    tipo VARCHAR(255),
    nombre VARCHAR(255),
    tipo_interesado VARCHAR(255)
);


-- Crear tabla col_miembros sin claves foráneas
CREATE TABLE col_miembros (
    interesado_hmdr_interesado VARCHAR(255), -- Nombre del interesado
    agrupacion VARCHAR(255),               -- Nombre de la agrupación
    area_HUMEDAL VARCHAR(255)               -- Nombre del área del páramo, 
);

--=============================================
-- Área de Humedal
--=============================================
DROP TABLE IF exists hmdr_uab_areahumedal;

CREATE TABLE hmdr_uab_areahumedal (
	uab_identificador varchar(255) NOT NULL, 
    uab_nombre_humedal varchar(255) NOT null,
    estado_proceso varchar(255) NULL,
	uab_nombre varchar(255) NULL, 
	ue_geometria public.geometry(multipolygonz, 9377) NULL, 
	interesado_tipo varchar(255) NULL,
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255)NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
    hmdr_interesado VARCHAR(1000),
	hmdr_agrupacioninteresados VARCHAR(1000), -- Nueva columna para agrupación del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) null, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);

--=============================================
-- Regimen de Uso de Actividades HUMEDAL
--=============================================

CREATE TABLE hmdr_regimenusosactividades (
    uab_identificador varchar(1000) NOT NULL, -- Identificador de la unidad administrativa básica (relación con prm_uab_zonificacion).
    uab_zona varchar(50000) NULL, -- TIpo Zona
    uab_principal varchar(50000)  NULL, -- Usos principales permitidos.
    uab_compatible varchar(50000) NULL, -- Usos compatibles permitidos.
    uab_condicionado varchar(50000) NULL, -- Usos condicionados permitidos.
    uab_prohibido varchar(50000) NULL -- Usos prohibidos.    
);

--=============================================
-- Zonificacion HUMEDAL
--=============================================
DROP TABLE IF exists hmdr_uab_zonificacion;


CREATE TABLE estructura_intermedia.hmdr_uab_zonificacion (
	uab_identificador varchar(1000) NOT NULL, 
	uab_tipo_zona varchar(1000) NULL,
	uab_detalle_zona varchar(1000) NULL,
	uab_regimen_usos varchar(1000) NOT NULL,
	uab_areahumedal varchar(255) NOT NULL,
    uab_etiqueta varchar(1000) NOT NULL,
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
    interesado_tipo VARCHAR(1000),
	hmdr_interesado VARCHAR(1000), -- Nueva columna para agrupación del interesado
	hmdr_agrupacioninteresados VARCHAR(1000), -- Nueva columna para agrupación del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) null, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);


--=============================================
-- Fuente Administrativa HUMEDAL
--=============================================
DROP TABLE IF exists hmdr_fuenteadministrativa;

CREATE TABLE hmdr_fuenteadministrativa (
	uab_identificador varchar(15) NOT NULL, -- Interno con el cual se identifica la reserva a la cual se está haciendo la compensación
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_numero int4 null,
	fuente_administrativa_anio int4 null
);

--=============================================
-----------------------------------------------
-- Funciones de homologacion HUMEDAL
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

/**********************************************************************************
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración de húmedales al modelo LADM_COL-HMDR
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        (C) 2024 by Leo Cardona (CEICOL SAS)
        (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)
        email           : contacto@ceicol.com
 **********************************************************************************/
/***************************************************************************
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License v3.0 as
 *   published by the Free Software Foundation.
 ***************************************************************************/

--========================================
--Fijar esquema
--========================================
SET search_path TO public, estructura_intermedia;

--========================================
-- INTERESADOS
--========================================
INSERT INTO hmdr_interesado (
    id,
    tipo,
    observacion,
    nombre,
    tipo_interesado,
    tipo_documento,
    numero_documento
)
SELECT
    ('INT_' || row_number() OVER ())::varchar(255) AS id,
    CASE
        WHEN nombre = 'Ministerio de Ambiente y Desarrollo Sostenible' THEN 'Regulador'
        ELSE 'Administrador'
    END AS tipo,
    NULL::varchar(255) AS observacion,
    nombre::varchar(255) AS nombre,
    'Persona_Juridica'::varchar(255) AS tipo_interesado,
    'NIT'::varchar(255) AS tipo_documento,
    MAX("NIT")::varchar(255) AS numero_documento
FROM insumos.corporaciones
GROUP BY nombre;

--========================================
-- Agrupación de interesados
--========================================
INSERT INTO estructura_intermedia.hmdr_agrupacioninteresados (
    id,
    tipo,
    nombre,
    tipo_interesado
)
SELECT
    ('AGR_' || row_number() OVER ())::varchar(255) AS id,
    'Grupo_Empresarial'::varchar(255) AS tipo,
    REPLACE(REPLACE(MAX("comisión conjunta"), '*', ''), ' - ', '-')::varchar(255) AS nombre,
    'Persona_Juridica'::varchar(255) AS tipo_interesado
FROM insumos.corporaciones
WHERE "comisión conjunta" IS NOT NULL
GROUP BY "humedal ramsar";

--========================================
-- Relación en col_miembros
--========================================
INSERT INTO estructura_intermedia.col_miembros (
    interesado_hmdr_interesado,
    agrupacion,
    area_humedal
)
SELECT
    nombre::varchar(255)               AS interesado_hmdr_interesado,
    "comisión conjunta"::varchar(255)  AS agrupacion,
    "humedal ramsar"::varchar(255)     AS area_humedal
FROM insumos.corporaciones
WHERE nombre IS NOT NULL
  AND "comisión conjunta" IS NOT NULL
  AND "humedal ramsar" IS NOT NULL;

--========================================
-- Área de reserva HMDR
--========================================
INSERT INTO estructura_intermedia.hmdr_uab_areahumedal (
    uab_identificador,
    uab_nombre_humedal,
    estado_proceso,
    uab_nombre,
    ue_geometria,
    interesado_tipo,
    interesado_nombre,
    interesado_tipo_interesado,
    interesado_tipo_documento,
    interesado_numero_documento,
    hmdr_interesado,
    hmdr_agrupacioninteresados,
    fuente_administrativa_tipo,
    fuente_administrativa_estado_disponibilidad,
    fuente_administrativa_tipo_formato,
    fuente_administrativa_fecha_documento_fuente,
    fuente_administrativa_nombre,
    ddr_tipo_resposabilidad,
    ddr_tipo_derecho
)
WITH grupo_humedales AS (
    SELECT
        cl."humedal ramsar" AS area_humedal,
        STRING_AGG(DISTINCT TRIM(cl."comisión conjunta"), ', ') AS hmdr_agrupacioninteresados,
        CASE
            WHEN MAX(TRIM(cl."comisión conjunta")) IS NULL THEN MAX(TRIM(cl.nombre))
            ELSE NULL
        END AS hmdr_interesado
    FROM insumos.corporaciones cl
    INNER JOIN insumos.area_humedal hr
        ON cl."humedal ramsar" = hr.nombre
    GROUP BY cl."humedal ramsar"
),
geometrias_unidas AS (
    SELECT
        nombre,
        ST_Transform(
            ST_Force3D(
                ST_Multi(
                    ST_Union(ST_CollectionExtract(ST_Force3D(geom), 3))
                )
            ), 9377
        ) AS ue_geometria
    FROM insumos.area_humedal
    GROUP BY nombre
),
grupo_texto AS (
    SELECT
        UPPER(id) AS uab_identificador,
        nombre AS uab_nombre_humedal,
        CASE
            WHEN estado_pro = 'Finalizado' THEN 'Finalizado'
            WHEN estado_pro = 'En proceso de ajustes' THEN 'Proceso_Ajuste'
            ELSE 'Sin_Estado'
        END AS uab_estado_proceso,
        fecha_acto,
        acto_admin
    FROM insumos.area_humedal
    GROUP BY id, nombre, estado_pro, fecha_acto, acto_admin
)
SELECT
    grupo_texto.uab_identificador,
    grupo_texto.uab_nombre_humedal,
    grupo_texto.uab_estado_proceso,
    'hmdr_uab_areahumedal'::varchar(255) AS uab_nombre,
    geometrias_unidas.ue_geometria,
    'Regulador' AS interesado_tipo,
    'Ministerio de Ambiente' AS interesado_nombre,
    'Persona_Juridica' AS interesado_tipo_interesado,
    'NIT' AS interesado_tipo_documento,
    '830.115.395-1' AS interesado_numero_documento,
    CASE
        WHEN grupo_humedales.hmdr_agrupacioninteresados IS NOT NULL THEN NULL
        ELSE COALESCE(grupo_humedales.hmdr_interesado, 'Ministerio de Ambiente')
    END AS hmdr_interesado,
    CASE
        WHEN grupo_humedales.hmdr_agrupacioninteresados = '' THEN NULL
        ELSE grupo_humedales.hmdr_agrupacioninteresados
    END AS hmdr_agrupacioninteresados,
    'Decreto' AS fuente_administrativa_tipo,
    'Disponible' AS fuente_administrativa_estado_disponibilidad,
    'Documento' AS fuente_administrativa_tipo_formato,
    grupo_texto.fecha_acto AS fuente_administrativa_fecha_documento_fuente,
    CASE
        WHEN grupo_texto.acto_admin ~ 'D\.(\d+)/\d+ Mod\. Decreto (\d+) de (\d{4})' THEN
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(grupo_texto.acto_admin), '\s+', ' '),
                'D\.(\d+)/\d+ Mod\. Decreto (\d+) de (\d{4})',
                'Decreto \2 de \3'
            )
        WHEN grupo_texto.acto_admin = 'Decreto 250 de 14 de febrero de 2017' THEN 'Decreto 250 de 2017'
        WHEN grupo_texto.acto_admin = 'Decreto 3888 de 8 de octubre de 2009' THEN 'Decreto 3888 de 2009'
        WHEN grupo_texto.acto_admin = 'Decreto 233 de 30 de Enero de 2008' THEN 'Decreto 233 de 2008'
        WHEN grupo_texto.acto_admin = 'Decreto 1275 de 08 de Julio  de 2014' THEN 'Decreto 1275 de 2014'
        WHEN grupo_texto.acto_admin = 'Decreto 251 de 14 de febrero de 2017' THEN 'Decreto 251 de 2017'
        ELSE
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(grupo_texto.acto_admin), '\s+', ' '),
                'Decreto\s+(\d+)(?:\s+de\s+\d+\s+de\s+|\s+de\s+\d{1,2}\s+de\s+\w+\s+|\s+de\s+)?(\d{4})',
                'Decreto \1 de \2'
            )
    END AS fuente_administrativa_nombre,
    'Administrar' AS ddr_tipo_resposabilidad,
    'Designar'    AS ddr_tipo_derecho
FROM grupo_texto
LEFT JOIN grupo_humedales
       ON grupo_texto.uab_nombre_humedal = grupo_humedales.area_humedal
LEFT JOIN geometrias_unidas
       ON grupo_texto.uab_nombre_humedal = geometrias_unidas.nombre
ORDER BY grupo_texto.uab_nombre_humedal;

--========================================
-- REGIMEN DE USOS
--========================================
INSERT INTO estructura_intermedia.hmdr_regimenusosactividades (
    uab_identificador,
    uab_zona,
    uab_principal,
    uab_compatible,
    uab_condicionado,
    uab_prohibido
)
SELECT
    ('RUA_' || ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS uab_identificador,
    "zonificacion"        AS uab_zona,
    "uso principal"       AS uab_principal,
    "uso compatible"      AS uab_compatible,
    "uso condicionado"    AS uab_condicionado,
    "uso prohibido"       AS uab_prohibido
FROM insumos.zonificacion_uso;

--========================================
-- Zonificación
--========================================
INSERT INTO estructura_intermedia.hmdr_uab_zonificacion (
    uab_identificador,
    uab_tipo_zona,
    uab_detalle_zona,
    uab_regimen_usos,
    uab_areahumedal,
    uab_etiqueta,
    uab_nombre,
    ue_geometria,
    hmdr_interesado,
    hmdr_agrupacioninteresados,
    fuente_administrativa_tipo,
    fuente_administrativa_estado_disponibilidad,
    fuente_administrativa_tipo_formato,
    fuente_administrativa_fecha_documento_fuente,
    fuente_administrativa_nombre,
    ddr_tipo_resposabilidad,
    ddr_tipo_derecho
)
SELECT
    ('ZNHMDR_' || row_number() OVER ()) AS uab_identificador,
    CASE
        WHEN "zoni_res_196_2006" = 'APSBCAE' THEN 'Produccion_Sostenible'
        WHEN "zoni_res_196_2006" = 'ARA'     THEN 'Recuperacion_Ambiental'
        WHEN "zoni_res_196_2006" = 'APPA'    THEN 'Preservacion_Y_Proteccion_Ambiental'
        ELSE 'Otro'
    END AS uab_tipo_zona,
    CASE
        WHEN "zoni_res_196_2006" NOT IN ('APSBCAE','ARA','APPA')
        THEN "zoni_res_196_2006"
        ELSE NULL
    END AS uab_detalle_zona,
    (
      SELECT uab_identificador
      FROM estructura_intermedia.hmdr_regimenusosactividades
      WHERE uab_zona = (
          CASE
            WHEN "zoni_res_196_2006" = 'APSBCAE' THEN 'Produccion_Sostenible'
            WHEN "zoni_res_196_2006" = 'ARA'     THEN 'Recuperacion_Ambiental'
            WHEN "zoni_res_196_2006" = 'APPA'    THEN 'Preservacion_Y_Proteccion_Ambiental'
            ELSE "zoni_res_196_2006"
          END
      )
        AND EXISTS (
          SELECT 1
          FROM insumos.corporaciones cl
          WHERE cl."humedal ramsar" = 'Complejo de Humedales Alto Río Cauca asociado a la Laguna de Sonso'
        )
      LIMIT 1
    ) AS uab_regimen_usos,
    (
      SELECT uab_identificador
      FROM estructura_intermedia.hmdr_uab_areahumedal
      WHERE uab_nombre_humedal = 'Complejo de Humedales Alto Río Cauca asociado a la Laguna de Sonso'
      LIMIT 1
    ) AS uab_areahumedal,
    nombre AS uab_etiqueta,
    'hmdr_uab_zonificacion' AS uab_nombre,
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
    CASE
        WHEN (
          SELECT MAX(nombre)
          FROM insumos.corporaciones cl
          WHERE cl."humedal ramsar" = 'Complejo de Humedales Alto Río Cauca asociado a la Laguna de Sonso'
        ) IS NOT NULL
        THEN (
          SELECT MAX(nombre)
          FROM insumos.corporaciones cl
          WHERE cl."humedal ramsar" = 'Complejo de Humedales Alto Río Cauca asociado a la Laguna de Sonso'
        )
        ELSE NULL
    END AS hmdr_interesado,
    CASE
        WHEN (
          SELECT STRING_AGG(DISTINCT "comisión conjunta", ', ')
          FROM insumos.corporaciones cl
          WHERE cl."humedal ramsar" = 'Complejo de Humedales Alto Río Cauca asociado a la Laguna de Sonso'
        ) IS NOT NULL
        THEN (
          SELECT STRING_AGG(DISTINCT "comisión conjunta", ', ')
          FROM insumos.corporaciones cl
          WHERE cl."humedal ramsar" = 'Complejo de Humedales Alto Río Cauca asociado a la Laguna de Sonso'
        )
        ELSE NULL
    END AS hmdr_agrupacioninteresados,
    'Documento_Publico' AS fuente_administrativa_tipo,
    'Disponible'        AS fuente_administrativa_estado_disponibilidad,
    'Documento'         AS fuente_administrativa_tipo_formato,
    '2018-05-18'::date  AS fuente_administrativa_fecha_documento_fuente,
    'Resolución 886 de 2018' AS fuente_administrativa_nombre,
    'Zonificar' AS ddr_tipo_resposabilidad,
    NULL         AS ddr_tipo_derecho
FROM insumos.zonificacion;

--========================================
-- Fuentes Administrativas de Áreas de humedal
--========================================
WITH areahumedal_acto AS (
    SELECT
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        CASE
            WHEN position(' ' IN cl.fuente_administrativa_nombre) > 0
            THEN substring(
                cl.fuente_administrativa_nombre,
                1,
                position(' ' IN cl.fuente_administrativa_nombre) - 1
            )
            ELSE NULL
        END AS fuente_administrativa_tipo,
        CASE
            WHEN position(' ' IN cl.fuente_administrativa_nombre) > 0
                 AND position(' DE ' IN upper(cl.fuente_administrativa_nombre))
                     > position(' ' IN cl.fuente_administrativa_nombre)
            THEN substring(
                cl.fuente_administrativa_nombre,
                position(' ' IN cl.fuente_administrativa_nombre) + 1,
                position(' DE ' IN upper(cl.fuente_administrativa_nombre))
                  - position(' ' IN cl.fuente_administrativa_nombre) - 1
            )
            ELSE NULL
        END AS numero_acto,
        CASE
            WHEN position(' DE ' IN upper(cl.fuente_administrativa_nombre)) > 0
            THEN substring(
                cl.fuente_administrativa_nombre,
                position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4
            )
            ELSE NULL
        END AS anio_acto
    FROM estructura_intermedia.hmdr_uab_areahumedal cl
)
INSERT INTO estructura_intermedia.hmdr_fuenteadministrativa (
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    fuente_administrativa_tipo_formato,
    fuente_administrativa_numero,
    fuente_administrativa_anio
)
SELECT
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE
        WHEN fuente_administrativa_tipo = 'Decreto  '
        THEN 'Decreto'
        ELSE fuente_administrativa_tipo
    END AS fuente_administrativa_tipo,
    -- Envolver con NULLIF para evitar ''::int4
    NULLIF(estructura_intermedia.homologar_numero(numero_acto::text), '')::int4 AS fuente_administrativa_numero,
    NULLIF(estructura_intermedia.homologar_numero(anio_acto::text), '')::int4   AS fuente_administrativa_anio
FROM areahumedal_acto;

--========================================
-- Fuentes Administrativas de Zonificación
--========================================
WITH zonificacion_acto AS (
    SELECT
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        CASE
            WHEN position(' ' IN cl.fuente_administrativa_nombre) > 0
            THEN substring(
                cl.fuente_administrativa_nombre,
                1,
                position(' ' IN cl.fuente_administrativa_nombre) - 1
            )
            ELSE NULL
        END AS tipo_acto,
        CASE
            WHEN position(' ' IN cl.fuente_administrativa_nombre) > 0
                 AND position(' DE ' IN upper(cl.fuente_administrativa_nombre))
                     > position(' ' IN cl.fuente_administrativa_nombre)
            THEN substring(
                cl.fuente_administrativa_nombre,
                position(' ' IN cl.fuente_administrativa_nombre) + 1,
                position(' DE ' IN upper(cl.fuente_administrativa_nombre))
                   - position(' ' IN cl.fuente_administrativa_nombre)
            )
            ELSE NULL
        END AS numero_acto,
        CASE
            WHEN position(' DE ' IN upper(cl.fuente_administrativa_nombre)) > 0
            THEN substring(
                cl.fuente_administrativa_nombre,
                position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4
            )
            ELSE NULL
        END AS anio_acto
    FROM estructura_intermedia.hmdr_uab_zonificacion cl
)
INSERT INTO estructura_intermedia.hmdr_fuenteadministrativa (
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    fuente_administrativa_tipo_formato,
    fuente_administrativa_numero,
    fuente_administrativa_anio
)
SELECT
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE
        WHEN tipo_acto = 'Resolución' THEN 'Resolucion'
        ELSE tipo_acto
    END AS fuente_administrativa_tipo,
    -- Envolver con NULLIF para evitar ''::int4
    NULLIF(estructura_intermedia.homologar_numero(numero_acto::text), '')::int4 AS fuente_administrativa_numero,
    NULLIF(estructura_intermedia.homologar_numero(anio_acto::text), '')::int4   AS fuente_administrativa_anio
FROM zonificacion_acto;


    """

    return sql_script



def validar_estructura():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/**********************************************************************************
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración de páramos  al modelo LADM_COL-hmdr
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 ***************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 **********************************************************************************/

--========================================
--Fijar esquema
--========================================
set search_path to 
	estructura_intermedia, -- Esquema de estructura de datos intermedia
	public;


--========================================
-- Área de Humedal
--========================================

SELECT *
FROM estructura_intermedia.hmdr_uab_areahumedal;

SELECT count(*)
FROM estructura_intermedia.hmdr_uab_areahumedal;
--36 area de Reserva
---- interesados y agrupaciones
SELECT 
    p.uab_nombre_humedal,
    CASE 
        WHEN COUNT(DISTINCT c.interesado_hmdr_interesado) = 0 THEN 
            STRING_AGG(DISTINCT c.agrupacion, ', ') || ' (Agrupaciones)'
        WHEN COUNT(DISTINCT c.agrupacion) = 0 THEN 
            STRING_AGG(DISTINCT c.interesado_hmdr_interesado, ', ') || ' (Interesados)'
        ELSE 'Agrupacion'
    END AS detalle,
    COUNT(DISTINCT c.agrupacion) AS total_agrupaciones,
    COUNT(DISTINCT c.interesado_hmdr_interesado) AS total_interesados
FROM 
    estructura_intermedia.hmdr_uab_areahumedal p
LEFT JOIN 
    estructura_intermedia.col_miembros c
ON 
    p.uab_nombre_humedal = c.area_humedal
GROUP BY 
    p.uab_nombre_humedal;

--========================================
-- Zonificación hmdr
--========================================
select * 
FROM estructura_intermedia.hmdr_uab_zonificacion;

select count(*) 
FROM estructura_intermedia.hmdr_uab_zonificacion;

select uab_areahumedal,count(*) 
FROM estructura_intermedia.hmdr_uab_zonificacion
group by uab_areahumedal;

select uab_tipo_zona,count(*) 
FROM estructura_intermedia.hmdr_uab_zonificacion
group by uab_tipo_zona
order by uab_tipo_zona;

select uab_areahumedal,uab_tipo_zona,count(*) 
FROM estructura_intermedia.hmdr_uab_zonificacion
group by uab_areahumedal,uab_tipo_zona
order by uab_areahumedal,uab_tipo_zona;

    """

    return sql_script

def importar_al_modelo():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/**********************************************************************************
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración del hmdr  al modelo LADM_COL-hmdr
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 ***************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 **********************************************************************************/

/* SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'etl_hmdr' AND pid <> pg_backend_pid();
DROP SCHEMA ladm CASCADE;*/
 
--========================================
--Fijar esquema
--========================================
/**********************************************************************************
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración del Ley 2  al modelo LADM_COL-hmdr
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 ***************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 **********************************************************************************/

/* SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'etl_hmdr' AND pid <> pg_backend_pid();
DROP SCHEMA ladm CASCADE;*/
 
--========================================
--Fijar esquema
--========================================
set search_path to
	estructura_intermedia,	-- Esquema de estructura de datos intermedia
	ladm,		-- Esquema modelo LADM-hmdr
	public;


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
	'LADM_COL_v_1_0_0_Ext_HMDR.HMDR',
	uuid_generate_v4(),
	'ETL de importación de datos',
	NULL );



--================================================================================
-- 2. Migración de hmdr_interesado
--================================================================================
--truncate table ladm.hmdr_interesado cascade
INSERT INTO ladm.hmdr_interesado(
    t_basket, 
    t_ili_tid, 
    tipo, 
    observacion, 
    nombre, 
    tipo_interesado, 
    tipo_documento, 
    numero_documento, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
)
SELECT 
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id FROM ladm.hmdr_interesadotipo WHERE ilicode LIKE tipo) AS tipo,
    NULL AS observacion,
    nombre AS nombre, 
    (SELECT t_id FROM ladm.col_interesadotipo WHERE ilicode LIKE tipo_interesado) AS tipo_interesado,
    (SELECT t_id FROM ladm.col_documentotipo WHERE ilicode LIKE tipo_documento) AS tipo_documento,
    numero_documento AS numero_documento,
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'hmdr_interesado' AS espacio_de_nombres, 
    id AS local_id    ------------ identificador texto tal cual 
FROM estructura_intermedia.hmdr_interesado;


----------------------------------------------------------------------------------------------------------
---2.1 migración de agrupacioninteresados
----------------------------------------------------------------------------------------------------------


INSERT INTO ladm.hmdr_agrupacioninteresados (
    t_basket,
    t_ili_tid, 
    tipo, 
    nombre, 
    tipo_interesado, 
    tipo_documento, 
    numero_documento, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
)
SELECT 
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id FROM ladm.col_grupointeresadotipo WHERE ilicode = ei.tipo) AS tipo, -- Ajuste para usar igualdad exacta
    ei.nombre AS nombre,
    (SELECT t_id FROM ladm.col_interesadotipo WHERE ilicode = ei.tipo_interesado) AS tipo_interesado, -- Igualdad exacta
    NULL AS tipo_documento, -- Define un valor si es obligatorio
    NULL AS numero_documento, -- Define un valor si es obligatorio
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'hmdr_interesado' AS espacio_de_nombres,
    ei.id AS local_id
FROM estructura_intermedia.hmdr_agrupacioninteresados ei
WHERE ei.nombre IS NOT NULL -- Excluir nombres nulos
  AND TRIM(ei.nombre) <> ''; -- Excluir nombres vacíos o con solo espacios


----------------------------------------------------------------------------------------------------------
---2.2 migración de col_miembros
----------------------------------------------------------------------------------------------------------
INSERT INTO ladm.col_miembros(
    t_basket,
    interesado_hmdr_interesado,
    interesado_hmdr_agrupacioninteresados,
    agrupacion,
    participacion
        
)
SELECT
    -- Obtener el t_basket
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    -- Obtener el t_id del interesado_hmdr_interesado
    (SELECT t_id 
     FROM ladm.hmdr_interesado 
     WHERE nombre = e.interesado_hmdr_interesado 
     LIMIT 1) AS interesado_hmdr_interesado,
    -- Agrupación de interesados (nulo)
    NULL AS interesado_hmdr_agrupacioninteresados,
    -- Obtener el t_id de la agrupación
    (SELECT t_id 
     FROM ladm.hmdr_agrupacioninteresados 
     WHERE nombre = e.agrupacion 
     LIMIT 1) AS agrupacion,
    -- Participación (nulo)
    NULL AS participacion
FROM 
    estructura_intermedia.col_miembros e
where e.agrupacion is not null;


--================================================================================
-- 3. Migración de  Area de Reserva
--================================================================================
--3.1 diligenciamiento de la tabla  hmdr_uab_areahumedal
--truncate table ladm.hmdr_uab_areahumedal cascade
INSERT INTO ladm.hmdr_uab_areahumedal(
	t_basket, 
	t_ili_tid, 
	nombre_humedal,
	estado_proceso,
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id) 
SELECT
	(SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	uab_nombre_humedal,
	(SELECT t_id FROM ladm.hmdr_estadoprocesotipo WHERE ilicode ILIKE estado_proceso) AS estado_proceso,
	uab_nombre,
	(SELECT t_id FROM ladm.col_unidadadministrativabasicatipo WHERE ilicode LIKE 'Ambiente_Desarrollo_Sostenible') AS tipo,
	now() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'hmdr_uab_areahumedal' AS espacio_de_nombres, 
	uab_identificador AS local_id
FROM estructura_intermedia.hmdr_uab_areahumedal;

--3.2 diligenciamiento de la tabla  hmdr_ue_areaparamo

INSERT INTO ladm.hmdr_ue_areahumedal(
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	uab_nombre_humedal as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'hmdr_ue_areahumedal' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.hmdr_uab_areahumedal; 

--3.3 diligenciamiento de derecho

INSERT INTO ladm.hmdr_derecho(
    t_basket, 
    t_ili_tid, 
    tipo, 
    descripcion, 
    interesado_hmdr_interesado, 
    interesado_hmdr_agrupacioninteresados, 
    unidad_hmdr_uab_zonificacion, 
    unidad_hmdr_uab_areahumedal, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
)
SELECT
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id FROM ladm.hmdr_derechotipo WHERE ilicode ILIKE ddr_tipo_derecho) AS tipo,
    NULL AS descripcion,
    -- Vinculamos con hmdr_interesado utilizando ILIKE para buscar coincidencias con Ministerio de Ambiente
    (SELECT t_id 
     FROM ladm.hmdr_interesado 
     WHERE nombre ILIKE '%Ministerio de Ambiente%' LIMIT 1) AS interesado_hmdr_interesado,
    NULL AS interesado_hmdr_agrupacioninteresados,
    NULL AS unidad_hmdr_uab_zonificacion, 
    (SELECT t_id 
     FROM ladm.hmdr_uab_areahumedal 
     WHERE local_id ILIKE uab_identificador LIMIT 1) AS unidad_hmdr_uab_areahumedal,
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'hmdr_responsabilidad' AS espacio_de_nombres, 
    uab_identificador AS local_id
FROM 
    estructura_intermedia.hmdr_uab_areahumedal;


--3.4 diligenciamiento de responsabilidad

INSERT INTO ladm.hmdr_responsabilidad(
    t_basket, 
    t_ili_tid, 
    tipo, 
    descripcion, 
    interesado_hmdr_interesado, 
    interesado_hmdr_agrupacioninteresados, 
    unidad_hmdr_uab_zonificacion, 
    unidad_hmdr_uab_areahumedal, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id)
SELECT
    -- Obtener el t_basket
    (SELECT t_id 
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    -- Generar un nuevo UUID para t_ili_tid
    uuid_generate_v4() AS t_ili_tid,
    -- Obtener el tipo de responsabilidad desde ladm.hmdr_responsabilidadtipo
    (SELECT t_id 
     FROM ladm.hmdr_responsabilidadtipo 
     WHERE ilicode LIKE ea.ddr_tipo_resposabilidad) AS tipo,
    -- Descripción nula
    NULL AS descripcion,
    -- Obtener el t_id del interesado usando hmdr_interesado = nombre
    (SELECT t_id 
     FROM ladm.hmdr_interesado 
     WHERE nombre = ea.hmdr_interesado
     LIMIT 1) AS interesado_hmdr_interesado,
    -- Obtener el t_id de agrupacioninteresados utilizando el nombre como referencia
    (SELECT t_id 
     FROM ladm.hmdr_agrupacioninteresados 
     WHERE nombre = ea.hmdr_agrupacioninteresados
     LIMIT 1) AS interesado_hmdr_agrupacioninteresados,
    -- Dejar nulo el campo de zonificación
    NULL AS unidad_hmdr_uab_zonificacion,
    -- Obtener el t_id del área páramo
    (SELECT t_id 
     FROM ladm.hmdr_uab_areahumedal
     WHERE local_id = ea.uab_identificador
     LIMIT 1) AS unidad_hmdr_uab_areahumedal,
    -- Fecha de comienzo de la vida útil
    NOW() AS comienzo_vida_util_version,
    -- Fecha de fin de la vida útil como NULL
    NULL AS fin_vida_util_version,
    -- Espacio de nombres fijo
    'hmdr_responsabilidad' AS espacio_de_nombres,
    -- Identificador local
    ea.uab_identificador AS local_id
FROM 
    estructura_intermedia.hmdr_uab_areahumedal ea;

--3.5 diligenciamiento de la tabla  fuente administrativa

INSERT INTO ladm.hmdr_fuenteadministrativa(
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
SELECT
    (SELECT t_id 
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id 
     FROM ladm.col_fuenteadministrativatipo cf 
     WHERE ilicode = 'Documento_Publico.Resolucion') AS tipo,
    NULL AS fecha_fin, 
    (SELECT t_id 
     FROM ladm.col_estadodisponibilidadtipo ce 
     WHERE ilicode LIKE a.fuente_administrativa_estado_disponibilidad) AS estado_disponibilidad,
    (SELECT t_id 
     FROM ladm.col_formatotipo cf 
     WHERE ilicode LIKE a.fuente_administrativa_tipo_formato) AS tipo_formato,
    a.fuente_administrativa_fecha_documento_fuente AS fecha_documento_fuente, 
    f.fuente_administrativa_tipo_formato || ' ' || f.fuente_administrativa_numero || ' de ' || f.fuente_administrativa_anio AS nombre, 
    NULL AS descripcion, 
    NULL AS url, 
    'hmdr_fuenteadministrativa' AS espacio_de_nombres, 
    f.uab_identificador AS local_id
FROM 
    estructura_intermedia.hmdr_fuenteadministrativa f,
    estructura_intermedia.hmdr_uab_areahumedal a
WHERE 
    f.uab_identificador = a.uab_identificador;

--================================================================================
-- 4. Migración de  zonificacion
--================================================================================
SET CONSTRAINTS ALL IMMEDIATE;
ALTER table ladm.hmdr_regimenusos
ADD COLUMN local_id VARCHAR(255);

INSERT INTO ladm.hmdr_regimenusos (
    t_basket,
    t_ili_tid,
    principal,
    compatible,
    condicionado,
    prohibido,
    local_id
)
SELECT
    -- Obtener t_basket utilizando la subconsulta especificada
    (SELECT t_id 
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    -- Generar un nuevo UUID para t_ili_tid
    uuid_generate_v4() AS t_ili_tid,
    -- Campos sin truncar
    a.uab_principal AS principal,
    a.uab_compatible AS compatible,
    a.uab_condicionado AS condicionado,
    a.uab_prohibido AS prohibido,
    -- Usar uab_identificador como local_id
    a.uab_identificador AS local_id
FROM 
    estructura_intermedia.hmdr_regimenusosactividades a;

--4.2 diligenciamiento de la tabla   zonificación
INSERT INTO ladm.hmdr_uab_zonificacion(
    t_basket, 
    t_ili_tid, 
    tipo_zona, 
    detalle_zona,
    area_hidrografica,
    regimen_usos,
    uab_areaparamo, 
    nombre, 
    tipo, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
)
SELECT
    -- Obtener t_basket
    (SELECT t_id 
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    -- Generar un nuevo UUID para t_ili_tid
    uuid_generate_v4() AS t_ili_tid,
    -- Obtener el tipo de zona desde ladm.hmdr_zonificaciontipo
    (SELECT t_id 
     FROM ladm.hmdr_zonificaciontipo 
     WHERE ilicode LIKE z.uab_tipo_zona LIMIT 1) AS tipo_zona,
    -- Detalle de la zona directamente desde la tabla origen
    z.uab_detalle_zona AS detalle_zona,
    uab_etiqueta as area_hidrografica,
    -- Extraer el t_id de ladm.hmdr_regimenusosactividades basado en el id_local de estructura_intermedia.hmdr_uab_zonificacion
    (SELECT l.t_id 
     FROM ladm.hmdr_regimenusos l
     WHERE l.local_id = z.uab_regimen_usos 
     LIMIT 1) AS regimen_usos,
    -- Obtener el área páramo desde ladm.hmdr_uab_areahumedal
    (SELECT t_id 
     FROM ladm.hmdr_uab_areahumedal 
     WHERE nombre_humedal IN 
         (SELECT a.uab_nombre_humedal 
          FROM estructura_intermedia.hmdr_uab_areahumedal a 
          WHERE a.uab_identificador = z.uab_areahumedal)
     LIMIT 1) AS uab_areaparamo,
    -- Nombre directamente desde la tabla origen
    z.uab_nombre AS nombre,
    -- Tipo fijo basado en un ilicode específico
    (SELECT t_id 
     FROM ladm.col_unidadadministrativabasicatipo 
     WHERE ilicode LIKE 'Ambiente_Desarrollo_Sostenible' LIMIT 1) AS tipo,
    -- Fecha de comienzo de la vida útil
    NOW() AS comienzo_vida_util_version,
    -- Fecha de fin de la vida útil (nula)
    NULL AS fin_vida_util_version,
    -- Espacio de nombres fijo
    'hmdr_uab_zonificacion' AS espacio_de_nombres, 
    -- Identificador local
    z.uab_identificador AS local_id
FROM 
    estructura_intermedia.hmdr_uab_zonificacion z;

ALTER table ladm.hmdr_regimenusos
DROP COLUMN local_id;

--4.3 diligenciamiento de la tabla  hmdr_ue_zonificacion
INSERT INTO ladm.hmdr_ue_zonificacion (
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	uab_etiqueta as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'hmdr_ue_zonificacion' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.hmdr_uab_zonificacion; 

------------------------------------------------------------------------------------
--4.4 diligenciamiento de la tabla  hmdr_responsabilidad para hmdr_uab_zonificacion
INSERT INTO ladm.hmdr_responsabilidad(
    t_basket, 
    t_ili_tid, 
    tipo, 
    descripcion, 
    interesado_hmdr_interesado, 
    interesado_hmdr_agrupacioninteresados, 
    unidad_hmdr_uab_zonificacion, 
    unidad_hmdr_uab_areahumedal, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
)
SELECT
    -- Obtener el t_basket
    (SELECT t_id 
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    -- Generar un nuevo UUID para t_ili_tid
    uuid_generate_v4() AS t_ili_tid,
    -- Obtener el tipo de responsabilidad desde ladm.hmdr_responsabilidadtipo
    (SELECT t_id 
     FROM ladm.hmdr_responsabilidadtipo 
     WHERE ilicode LIKE ea.ddr_tipo_resposabilidad) AS tipo,
    -- Descripción nula
    NULL AS descripcion,
    -- Obtener el t_id del interesado usando hmdr_interesado = nombre
    (SELECT t_id 
     FROM ladm.hmdr_interesado 
     WHERE nombre = ea.hmdr_interesado
     LIMIT 1) AS interesado_hmdr_interesado,
    -- Obtener el t_id de agrupacioninteresados utilizando el nombre como referencia
    (SELECT t_id 
     FROM ladm.hmdr_agrupacioninteresados 
     WHERE nombre = ea.hmdr_agrupacioninteresados
     LIMIT 1) AS interesado_hmdr_agrupacioninteresados,
    -- Relacionar con zonificación
    (SELECT t_id 
     FROM ladm.hmdr_uab_zonificacion 
     WHERE local_id = ea.uab_identificador
     LIMIT 1) AS unidad_hmdr_uab_zonificacion,
    -- Dejar nulo el campo de área páramo
    NULL AS unidad_hmdr_uab_areahumedal,
    -- Fecha de comienzo de la vida útil
    NOW() AS comienzo_vida_util_version,
    -- Fecha de fin de la vida útil como NULL
    NULL AS fin_vida_util_version,
    -- Espacio de nombres fijo
    'hmdr_responsabilidad' AS espacio_de_nombres,
    -- Identificador local
    ea.uab_identificador AS local_id
FROM 
    estructura_intermedia.hmdr_uab_zonificacion ea;

--4.5 diligenciamiento de la tabla hmdr_fuenteadministrativa para hmdr_uab_areahumedal

INSERT INTO ladm.hmdr_fuenteadministrativa(
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
SELECT
    (SELECT t_id 
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id 
     FROM ladm.col_fuenteadministrativatipo cf 
     WHERE ilicode = 'Documento_Publico.Resolucion') AS tipo,
    NULL AS fecha_fin, 
    (SELECT t_id 
     FROM ladm.col_estadodisponibilidadtipo ce 
     WHERE ilicode LIKE a.fuente_administrativa_estado_disponibilidad) AS estado_disponibilidad,
    (SELECT t_id 
     FROM ladm.col_formatotipo cf 
     WHERE ilicode LIKE a.fuente_administrativa_tipo_formato) AS tipo_formato,
    a.fuente_administrativa_fecha_documento_fuente AS fecha_documento_fuente, 
    f.fuente_administrativa_tipo_formato || ' ' || f.fuente_administrativa_numero || ' de ' || f.fuente_administrativa_anio AS nombre, 
    NULL AS descripcion, 
    NULL AS url, 
    'hmdr_fuenteadministrativa' AS espacio_de_nombres, 
    f.uab_identificador AS local_id
FROM 
    estructura_intermedia.hmdr_fuenteadministrativa f,
    estructura_intermedia.hmdr_uab_zonificacion a
WHERE 
    f.uab_identificador = a.uab_identificador;


--================================================================================
-- 7. Migración de col_rrrfuente
--================================================================================
INSERT INTO ladm.col_rrrfuente(
	t_basket, 
	fuente_administrativa, 
	rrr_hmdr_derecho, 
	rrr_hmdr_responsabilidad)
select
(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' limit 1) as t_basket,
*
from (
	select distinct 
	f.t_id as fuente_administrativa,
	null::int8 as rrr_hmdr_derecho,
	r.t_id as rrr_hmdr_responsabilidad
	from ladm.hmdr_fuenteadministrativa f,
	ladm.hmdr_responsabilidad r
	where f.local_id=r.local_id 
	union  
	select distinct 
	f.t_id as fuente_administrativa,
	d.t_id as rrr_hmdr_derecho,
	null::int4 as rrr_hmdr_responsabilidad
	from ladm.hmdr_fuenteadministrativa f,
	ladm.hmdr_derecho d
	where f.local_id=d.local_id
) t;

--================================================================================
-- 8. Migración de col_uebaunit
--================================================================================
INSERT INTO ladm.col_uebaunit(
    t_basket, 
    ue_hmdr_ue_zonificacion, 
    ue_hmdr_ue_areahumedal, 
    baunit_hmdr_uab_zonificacion,	 
    baunit_hmdr_uab_areahumedal
)
SELECT
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_HMDR.HMDR' LIMIT 1) AS t_basket,
    *
FROM (
    -- Zonificación
    SELECT 
        uez.t_id::int8 AS ue_hmdr_ue_zonificacion,
        NULL::int8 AS ue_hmdr_ue_areaparamo,
        uabz.t_id::int8 AS baunit_hmdr_uab_zonificacion,
        NULL::int8 AS baunit_hmdr_uab_areahumedal
    FROM 
        ladm.hmdr_ue_zonificacion uez
    JOIN 
        ladm.hmdr_uab_zonificacion uabz
    ON 
        uez.local_id = uabz.local_id
    UNION ALL	
    -- Área Páramo o Reserva
    SELECT 
        NULL AS ue_hmdr_ue_zonificacion,
        uez.t_id AS ue_hmdr_ue_areaparamo,
        NULL AS baunit_hmdr_uab_zonificacion,
        uabz.t_id AS baunit_hmdr_uab_areahumedal
    FROM 
        ladm.hmdr_ue_areahumedal uez
    JOIN 
        ladm.hmdr_uab_areahumedal uabz
    ON 
        uez.local_id = uabz.local_id
) t;




    """

    return sql_script


