def estructura_intermedia():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    

/***********************************************************************************
             Creación de estructura de datos intermedia 
        	Migración del PARAMO  al modelo LADM_COL-PRM
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
-- Crear tabla prm_interesado
CREATE TABLE prm_interesado (
    id VARCHAR(255) PRIMARY KEY, -- INT_NUMERO
    tipo VARCHAR(255),
    observacion VARCHAR(255),
    nombre VARCHAR(255),
    tipo_interesado VARCHAR(255),
    tipo_documento VARCHAR(255),
    numero_documento VARCHAR(255)
);

-- Crear tabla prm_agrupacioninteresados
CREATE TABLE prm_agrupacioninteresados (
    id VARCHAR(255) PRIMARY KEY, -- AGR_numero
    tipo VARCHAR(255),
    nombre VARCHAR(255),
    tipo_interesado VARCHAR(255)
);


-- Crear tabla col_miembros sin claves foráneas
CREATE TABLE col_miembros (
    interesado_prm_interesado VARCHAR(255), -- Nombre del interesado
    agrupacion VARCHAR(255),               -- Nombre de la agrupación
    area_paramo VARCHAR(255)               -- Nombre del área del páramo, 
);

--=============================================
-- Área de reserva PARAMO
--=============================================
--DROP TABLE IF exists prm_uab_areaparamo;
CREATE TABLE prm_uab_areaparamo (
	uab_identificador varchar(7) NOT NULL, 
	uab_identificador_insumo varchar(255) not null, 
    uab_nombre_paramo varchar(255) NOT null,
	uab_nombre varchar(255) NULL, 
	ue_geometria public.geometry(multipolygonz, 9377) NULL, 
	interesado_tipo varchar(255) NULL,
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255)NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
    prm_interesado VARCHAR(1000),
	prm_agrupacioninteresados VARCHAR(1000), -- Nueva columna para agrupación del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) null, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);

--=============================================
-- Regimen de Uso de Actividades PARAMO
--=============================================
CREATE TABLE prm_regimenusosactividades (
    uab_identificador varchar(1000) NOT NULL, -- Identificador de la unidad administrativa básica (relación con prm_uab_zonificacion).
    uab_zona varchar(50000) NULL, -- TIpo Zona
    uab_principal varchar(50000)  NULL, -- Usos principales permitidos.
    uab_compatible varchar(50000) NULL, -- Usos compatibles permitidos.
    uab_condicionado varchar(50000) NULL, -- Usos condicionados permitidos.
    uab_prohibido varchar(50000) NULL -- Usos prohibidos.
);


--=============================================
-- Zonificacion PARAMO
--=============================================
--DROP TABLE IF exists prm_uab_zonificacion;
CREATE TABLE estructura_intermedia.prm_uab_zonificacion (
	uab_identificador varchar(1000) NOT NULL, 
	uab_tipo_zona varchar(1000) NULL,
	uab_detalle_zona varchar(1000) NULL,
	uab_regimen_usos varchar(1000) NOT NULL,
	uab_areaparamo varchar(255) NOT NULL,
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
    interesado_tipo VARCHAR(1000),
	prm_interesado VARCHAR(1000), -- Nueva columna para agrupación del interesado
	prm_agrupacioninteresados VARCHAR(1000), -- Nueva columna para agrupación del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) null, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);


--=============================================
-- Fuente Administrativa PARAMO
--=============================================
--DROP TABLE IF exists prm_fuenteadministrativa;
CREATE TABLE prm_fuenteadministrativa (
	uab_identificador varchar(15) NOT NULL, -- Interno con el cual se identifica la reserva a la cual se está haciendo la compensación
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_numero int4 null,
	fuente_administrativa_anio int4 null
);

--=============================================
-----------------------------------------------
-- Funciones de homologacion PARAMO
-----------------------------------------------
--=============================================

--=============================================
-- Funcion de homologacion a texto
--=============================================
--DROP FUNCTION IF exists homologar_texto;
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
--DROP FUNCTION IF exists estructura_intermedia.homologar_numero;
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
        			Migración de Páramos al modelo LADM_COL-PRM
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        email           : contacto@ceicol.com
 **********************************************************************************/
/***************************************************************************
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 ***************************************************************************/
---------------------------------------------------------------------------
-- Fijar esquema
---------------------------------------------------------------------------
SET search_path TO public,estructura_intermedia;

---------------------------------------------------------------------------
-- INTERESADOS
---------------------------------------------------------------------------
-- TRUNCATE TABLE prm_interesado;
-- Insertar datos en ladm_prm_interesado con un SELECT
INSERT INTO prm_interesado (
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
    MAX(nit)::varchar(255) AS numero_documento
FROM insumos.corporaciones
GROUP BY nombre;


-- TRUNCATE TABLE estructura_intermedia.prm_agrupacioninteresados;
INSERT INTO estructura_intermedia.prm_agrupacioninteresados (
    id, 
    tipo, 
    nombre, 
    tipo_interesado
)
SELECT 
    ('AGR_' || row_number() OVER ())::varchar(255) AS id,
    'Grupo_Empresarial'::varchar(255) AS tipo, 
    REPLACE(REPLACE(MAX(comision_conjunta), '*', ''), ' - ', '-')::varchar(255) AS nombre,
    'Persona_Juridica'::varchar(255) AS tipo_interesado
FROM insumos.corporaciones
WHERE comision_conjunta IS NOT NULL
GROUP BY paramo;

-- TRUNCATE TABLE estructura_intermedia.col_miembros;
INSERT INTO estructura_intermedia.col_miembros (
    interesado_prm_interesado, 
    agrupacion, 
    area_paramo
)
SELECT 
    nombre::varchar(255) AS interesado_prm_interesado,
    comision_conjunta::varchar(255) AS agrupacion,
    paramo::varchar(255) AS area_paramo
FROM insumos.corporaciones
WHERE nombre IS NOT NULL
  AND comision_conjunta IS NOT NULL
  AND paramo IS NOT NULL;

---------------------------------------------------------------------------
-- Área de reserva PRM
---------------------------------------------------------------------------
-- TRUNCATE TABLE estructura_intermedia.prm_uab_areaparamo;

WITH agrupacion_interesado AS ( 
    SELECT 
        CASE 
            WHEN TRIM(REPLACE(REPLACE(REPLACE(paramo, E'\n', ''), '“', '"'), '”', '"')) = 'Frontino – Urrao "Del Sol Las Alegrías' 
                 THEN 'Frontino – Urrao "Del Sol Las Alegrías"' 
            ELSE TRIM(REPLACE(REPLACE(REPLACE(paramo, E'\n', ''), '“', '"'), '”', '"')) 
        END AS area_paramo,
        STRING_AGG(DISTINCT comision_conjunta, ', ') AS prm_agrupacioninteresados,
        CASE 
            WHEN MAX(TRIM(comision_conjunta)) IS NULL THEN MAX(TRIM(nombre)) 
            ELSE NULL 
        END AS prm_interesado
    FROM insumos.corporaciones
    GROUP BY paramo
),
agrupacion_paramo AS (
    SELECT 
        id,
        UPPER(id) AS uab_identificador, 
        CASE 
            WHEN TRIM(REPLACE(REPLACE(REPLACE(nombre, E'\n', ''), '“', '"'), '”', '"')) = 'Frontino – Urrao "Del Sol Las Alegrías'
                 THEN 'Frontino – Urrao "Del Sol Las Alegrías"' 
            ELSE TRIM(REPLACE(REPLACE(REPLACE(nombre, E'\n', ''), '“', '"'), '”', '"')) 
        END AS uab_nombre_paramo,
        fecha_acto,
        acto_admin,
        ST_Force3D(
            ST_Multi(
                ST_Union(
                    ST_CollectionExtract(ST_Force3D(geom), 3)
                )
            )
        ) AS ue_geometria
    FROM insumos.area_paramo
    GROUP BY id, nombre, fecha_acto, acto_admin
)
INSERT INTO estructura_intermedia.prm_uab_areaparamo (
    uab_identificador, 
    uab_identificador_insumo,
    uab_nombre_paramo, 
    uab_nombre, 
    ue_geometria, 
    interesado_tipo, 
    interesado_nombre, 
    interesado_tipo_interesado, 
    interesado_tipo_documento, 
    interesado_numero_documento, 
    prm_interesado,
    prm_agrupacioninteresados,
    fuente_administrativa_tipo, 
    fuente_administrativa_estado_disponibilidad, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_nombre, 
    ddr_tipo_resposabilidad,
    ddr_tipo_derecho
)
SELECT 
    ap.uab_identificador,
    ap.id AS uab_identificador_insumo,
    ap.uab_nombre_paramo,
    'prm_uab_areaparamo'::varchar(255) AS uab_nombre,
    ST_Transform(ap.ue_geometria,9377) AS ue_geometria,
    'Regulador' AS interesado_tipo,
    'Ministerio de Ambiente' AS interesado_nombre,
    'Persona_Juridica' AS interesado_tipo_interesado,
    'NIT' AS interesado_tipo_documento,
    '830.115.395-1' AS interesado_numero_documento,
    ai.prm_interesado,
    ai.prm_agrupacioninteresados,
    'Documento_Publico' AS fuente_administrativa_tipo, 
    'Disponible' AS fuente_administrativa_estado_disponibilidad,
    'Documento' AS fuente_administrativa_tipo_formato,
    ap.fecha_acto AS fuente_administrativa_fecha_documento_fuente,
    REGEXP_REPLACE(
        ap.acto_admin, 
        'Resoluci[oó]n\\s+No\\.\\s*(\\d+).*?de\\s+(\\d{4})', 
        'Resolucion \\1 de \\2'
    ) AS fuente_administrativa_nombre,
    'Administrar' AS ddr_tipo_resposabilidad,
    'Delimitar'  AS ddr_tipo_derecho
FROM agrupacion_paramo ap
LEFT JOIN agrupacion_interesado ai
       ON ap.uab_nombre_paramo = ai.area_paramo
ORDER BY ap.uab_nombre_paramo;

---------------------------------------------------------------------------
-- REGIMEN DE USOS
---------------------------------------------------------------------------
-- TRUNCATE TABLE estructura_intermedia.prm_regimenusosactividades;
INSERT INTO estructura_intermedia.prm_regimenusosactividades (
    uab_identificador, 
    uab_zona,
    uab_principal, 
    uab_compatible, 
    uab_condicionado, 
    uab_prohibido
)
SELECT 
    ('RUA_' || ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS uab_identificador,
    zonificacion AS uab_zona,
    uso_principal AS uab_principal,
    NULL AS uab_compatible,
    uso_condicionado AS uab_condicionado,
    uso_prohibido AS uab_prohibido
FROM insumos.zonificacion_uso;

---------------------------------------------------------------------------
-- Zonificación
---------------------------------------------------------------------------
-- TRUNCATE TABLE estructura_intermedia.prm_uab_zonificacion;
WITH agrupacion_interesado AS ( 
    SELECT 
        paramo AS uab_nombre_paramo,
        STRING_AGG(DISTINCT comision_conjunta, ', ') AS prm_agrupacioninteresados,
        CASE 
            WHEN MAX(TRIM(comision_conjunta)) IS NULL THEN MAX(TRIM(nombre)) 
            ELSE NULL 
        END AS prm_interesado
    FROM insumos.corporaciones
    GROUP BY paramo
)
INSERT INTO estructura_intermedia.prm_uab_zonificacion (
    uab_identificador, 
    uab_tipo_zona, 
    uab_detalle_zona,
    uab_regimen_usos,
    uab_areaparamo, 
    uab_nombre, 
    ue_geometria, 
    prm_interesado,
	prm_agrupacioninteresados,
    fuente_administrativa_tipo, 
    fuente_administrativa_estado_disponibilidad, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_nombre, 
    ddr_tipo_resposabilidad, 
    ddr_tipo_derecho
)
SELECT 
    ('ZNPRM_' || row_number() OVER ()) AS uab_identificador,
    CASE 
        WHEN subczon::text = '1B' THEN 'Zona_Transito_Reconversion_Y_Sustitucion.Reconversion_Y_Sustitucion_Medios_Vida_Base_Agropecuaria'
        WHEN subczon::text = '2A' THEN 'Area_Prioritaria_Restauracion_Ecologica'
        WHEN subczon::text = '3A' THEN 'Area_Prioritaria_Preservacion'
        WHEN subczon::text = '1A' THEN 'Zona_Transito_Reconversion_Y_Sustitucion.Sustitucion_Prioritaria'
        ELSE 'Otro'
    END AS uab_tipo_zona,
    CASE 
        WHEN detallezon::text = '1Aa' THEN 'Áreas con actividades agropecuarias introducidas sobre áreas de vegetación natural desde el año 2011 en adelante'
        WHEN detallezon::text = '1Ab' THEN 'Áreas en las que se venían desarrollando actividades agropecuarias antes del 16 de junio de 2011 pero que se consideran de alta importancia para el suministro de servicios ecosistémicos'
        WHEN detallezon::text = '1Ac' THEN 'Áreas que fueron intervenidas por procesos de exploración y explotación de recursos minero energéticos de manera previa a la entrada en vigencia de la normativa'
        WHEN detallezon::text = '1B'  THEN 'Reconversión y sustitución de los medios de vida de base agropecuaria'
        WHEN detallezon::text IN ('2Aa','2a') THEN 'Áreas que actualmente no se encuentran bajo uso agropecuario pero que pudieron ser objeto de alteraciones ...'
        WHEN detallezon::text IN ('2Ab','2b') THEN 'Áreas afectadas por remoción en masa, incendios, fenómenos hidrometeorológicos, etc.'
        WHEN detallezon::text IN ('3Aa','3a') THEN 'Zonas de alta importancia ambiental o fragilidad ecológica'
        WHEN detallezon::text IN ('3Ab','3b') THEN 'Zonas de especial importancia para la provisión de servicios ecosistémicos'
        ELSE 'Sin Datos'
    END AS uab_detalle_zona,
    (
        SELECT uab_identificador 
        FROM estructura_intermedia.prm_regimenusosactividades 
        WHERE uab_zona = CASE 
                            WHEN subczon::text = '1B' THEN 'Zona_Transito_Reconversion_Y_Sustitucion.Reconversion_Y_Sustitucion_Medios_Vida_Base_Agropecuaria'
                            WHEN subczon::text = '2A' THEN 'Area_Prioritaria_Restauracion_Ecologica'
                            WHEN subczon::text = '3A' THEN 'Area_Prioritaria_Preservacion'
                            WHEN subczon::text = '1A' THEN 'Zona_Transito_Reconversion_Y_Sustitucion.Sustitucion_Prioritaria'
                            ELSE 'Otro'
                         END
        LIMIT 1
    ) AS uab_regimen_usos,
    (
        SELECT uab_identificador 
        FROM estructura_intermedia.prm_uab_areaparamo 
        WHERE uab_nombre_paramo IN ('Los Nevados')
        LIMIT 1
    ) AS uab_areaparamo ,
    'prm_uab_zonificacion' AS uab_nombre,
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
    (
        SELECT prm_interesado
        FROM agrupacion_interesado 
        WHERE uab_nombre_paramo IN ('Los Nevados')
        LIMIT 1
    ) AS prm_interesado, 
    (
        SELECT prm_agrupacioninteresados
        FROM agrupacion_interesado 
        WHERE uab_nombre_paramo IN ('Los Nevados')
        LIMIT 1
    ) AS prm_agrupacioninteresados,
    'Documento_Publico.' AS fuente_administrativa_tipo,
    'Disponible' AS fuente_administrativa_estado_disponibilidad,
    'Documento' AS fuente_administrativa_tipo_formato,
    '2018-05-18'::date AS fuente_administrativa_fecha_documento_fuente,
    'Resolución 886 de 2018' AS fuente_administrativa_nombre,
    'Zonificar' AS ddr_tipo_resposabilidad,
    NULL AS ddr_tipo_derecho
FROM insumos.zonificacion;

---------------------------------------------------------------------------
-- Fuentes Administrativas de Áreas de Reserva
---------------------------------------------------------------------------
-- TRUNCATE TABLE estructura_intermedia.prm_fuenteadministrativa;
WITH areareserva_acto AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        CASE 
            WHEN position(' ' IN cl.fuente_administrativa_nombre) > 0 THEN
                substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1)
            ELSE 
                NULL
        END AS fuente_administrativa_tipo,
        CASE 
            WHEN position(' ' IN cl.fuente_administrativa_nombre) > 0
                 AND position(' DE ' IN upper(cl.fuente_administrativa_nombre)) 
                     > position(' ' IN cl.fuente_administrativa_nombre) THEN
                substring(
                  cl.fuente_administrativa_nombre,
                  position(' ' IN cl.fuente_administrativa_nombre) + 1,
                  position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre) - 1
                )
            ELSE 
                NULL
        END AS numero_acto,
        CASE 
            WHEN position(' DE ' IN upper(cl.fuente_administrativa_nombre)) > 0 THEN
                substring(
                  cl.fuente_administrativa_nombre, 
                  position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4
                )
            ELSE 
                NULL
        END AS anio_acto
    FROM estructura_intermedia.prm_uab_areaparamo cl
)
INSERT INTO estructura_intermedia.prm_fuenteadministrativa (
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
        WHEN fuente_administrativa_tipo = 'Resolución' THEN 'Resolucion'
        ELSE fuente_administrativa_tipo
    END AS fuente_administrativa_tipo,
    CASE 
        WHEN numero_acto IS NOT NULL THEN 
            NULLIF(estructura_intermedia.homologar_numero(numero_acto::text), '')::int4
        ELSE 
            NULL
    END AS fuente_administrativa_numero,
    CASE 
        WHEN anio_acto IS NOT NULL THEN
            NULLIF(estructura_intermedia.homologar_numero(anio_acto::text), '')::int4
        ELSE 
            NULL
    END AS fuente_administrativa_anio
FROM areareserva_acto;


---------------------------------------------------------------------------
-- Fuentes Administrativas de Zonificación
---------------------------------------------------------------------------
-- TRUNCATE TABLE estructura_intermedia.prm_fuenteadministrativa;
WITH zonificacion_acto AS (
	SELECT 
        uab_identificador,
	    fuente_administrativa_fecha_documento_fuente,
	    CASE 
           WHEN position(' ' IN cl.fuente_administrativa_nombre) > 0 THEN
               substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1)
           ELSE
               NULL
        END AS tipo_acto,
	    CASE
           WHEN position(' ' IN cl.fuente_administrativa_nombre) > 0
                AND position(' DE ' IN upper(cl.fuente_administrativa_nombre)) 
                    > position(' ' IN cl.fuente_administrativa_nombre) THEN
               substring(
                 cl.fuente_administrativa_nombre,
                 position(' ' IN cl.fuente_administrativa_nombre) + 1,
                 position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)
               )
           ELSE
               NULL
        END AS numero_acto,
	    CASE 
           WHEN position(' DE ' IN upper(cl.fuente_administrativa_nombre)) > 0 THEN
               substring(
                 cl.fuente_administrativa_nombre,
                 position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4
               )
           ELSE 
               NULL
        END AS anio_acto
	FROM estructura_intermedia.prm_uab_zonificacion cl
)
INSERT INTO estructura_intermedia.prm_fuenteadministrativa(
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
    NULLIF(estructura_intermedia.homologar_numero(numero_acto::text), '')::int4 AS fuente_administrativa_acto,
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
        			Migración de páramos  al modelo LADM_COL-PRM
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
-- Área de paramo
--========================================

SELECT *
FROM estructura_intermedia.prm_uab_areaparamo;

SELECT count(*)
FROM estructura_intermedia.prm_uab_areaparamo;
--36 area de Reserva
---- interesados y agrupaciones
SELECT 
    p.uab_nombre_paramo,
    CASE 
        WHEN COUNT(DISTINCT c.interesado_prm_interesado) = 0 THEN 
            STRING_AGG(DISTINCT c.agrupacion, ', ') || ' (Agrupaciones)'
        WHEN COUNT(DISTINCT c.agrupacion) = 0 THEN 
            STRING_AGG(DISTINCT c.interesado_prm_interesado, ', ') || ' (Interesados)'
        ELSE 'Datos Inconsistentes'
    END AS detalle,
    COUNT(DISTINCT c.agrupacion) AS total_agrupaciones,
    COUNT(DISTINCT c.interesado_prm_interesado) AS total_interesados
FROM 
    estructura_intermedia.prm_uab_areaparamo p
LEFT JOIN 
    estructura_intermedia.col_miembros c
ON 
    p.uab_nombre_paramo = c.area_paramo
GROUP BY 
    p.uab_nombre_paramo;

--========================================
-- Zonificación PRM
--========================================
select * 
FROM estructura_intermedia.prm_uab_zonificacion;

select count(*) 
FROM estructura_intermedia.prm_uab_zonificacion;

select uab_areaparamo,count(*) 
FROM estructura_intermedia.prm_uab_zonificacion
group by uab_areaparamo;

select uab_tipo_zona,count(*) 
FROM estructura_intermedia.prm_uab_zonificacion
group by uab_tipo_zona
order by uab_tipo_zona;

select uab_areaparamo,uab_tipo_zona,count(*) 
FROM estructura_intermedia.prm_uab_zonificacion
group by uab_areaparamo,uab_tipo_zona
order by uab_areaparamo,uab_tipo_zona;

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
        			Migración del PRM  al modelo LADM_COL-PRM
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
	estructura_intermedia,	-- Esquema de estructura de datos intermedia
	ladm,		-- Esquema modelo LADM-PRM
	public;


--================================================================================
-- 1. Define Basket si este no existe
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
	'LADM_COL_v_1_0_0_Ext_PRM.PRM',
	uuid_generate_v4(),
	'ETL de importación de datos',
	NULL );

--================================================================================
-- 2. Migración de prm_interesado
--================================================================================
--truncate table ladm.prm_interesado cascade
INSERT INTO ladm.prm_interesado(
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
    local_id
)
SELECT 
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    NULL AS observacion,
    nombre AS nombre, 
    (SELECT t_id FROM ladm.col_interesadotipo WHERE ilicode LIKE tipo_interesado) AS tipo_interesado,
    (SELECT t_id FROM ladm.col_documentotipo WHERE ilicode LIKE tipo_documento) AS tipo_documento,
    numero_documento AS numero_documento,
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'prm_interesado' AS espacio_de_nombres, 
    ROW_NUMBER() OVER (ORDER BY nombre) AS local_id    ------------ identificador texto tal cual 
FROM estructura_intermedia.prm_interesado;


----------------------------------------------------------------------------------------------------------
---2.1 migración de agrupacioninteresados
----------------------------------------------------------------------------------------------------------
--truncate table prm_agrupacioninteresados cascade
INSERT INTO ladm.prm_agrupacioninteresados (
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
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id FROM ladm.col_grupointeresadotipo WHERE ilicode = ei.tipo) AS tipo, -- Ajuste para usar igualdad exacta
    ei.nombre AS nombre,
    (SELECT t_id FROM ladm.col_interesadotipo WHERE ilicode = ei.tipo_interesado) AS tipo_interesado, -- Igualdad exacta
    NULL AS tipo_documento, -- Define un valor si es obligatorio
    NULL AS numero_documento, -- Define un valor si es obligatorio
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'prm_interesado' AS espacio_de_nombres,
    ei.id AS local_id
FROM estructura_intermedia.prm_agrupacioninteresados ei
WHERE ei.nombre IS NOT NULL -- Excluir nombres nulos
  AND TRIM(ei.nombre) <> ''; -- Excluir nombres vacíos o con solo espacios


----------------------------------------------------------------------------------------------------------
---2.2 migración de col_miembros
----------------------------------------------------------------------------------------------------------
INSERT INTO ladm.col_miembros(
    t_basket,
    interesado_prm_interesado,
    interesado_prm_agrupacioninteresados,
    agrupacion,
    participacion        
)
SELECT
    -- Obtener el t_basket
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
    -- Obtener el t_id del interesado_prm_interesado
    (SELECT t_id 
     FROM ladm.prm_interesado 
     WHERE nombre = e.interesado_prm_interesado 
     LIMIT 1) AS interesado_prm_interesado,
    -- Agrupación de interesados (nulo)
    NULL AS interesado_prm_agrupacioninteresados,
    -- Obtener el t_id de la agrupación
    (SELECT t_id 
     FROM ladm.prm_agrupacioninteresados 
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
--3.1 diligenciamiento de la tabla  prm_uab_areaparamo
--truncate table ladm.prm_uab_areaparamo cascade
INSERT INTO ladm.prm_uab_areaparamo(
	t_basket, 
	t_ili_tid, 
	identificador,
	nombre_paramo,
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_PRM.PRM' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	uab_identificador_insumo,
	uab_nombre_paramo,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'prm_uab_areaparamo' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.prm_uab_areaparamo; 

--3.2 diligenciamiento de la tabla  prm_ue_areaparamo
INSERT INTO ladm.prm_ue_areaparamo(
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_PRM.PRM' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	uab_nombre_paramo as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'prm_ue_areaparamo' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.prm_uab_areaparamo; 

--3.3 diligenciamiento de derecho
INSERT INTO ladm.prm_derecho(
    t_basket, 
    t_ili_tid, 
    tipo, 
    descripcion, 
    interesado_prm_interesado, 
    interesado_prm_agrupacioninteresados, 
    unidad_prm_uab_zonificacion, 
    unidad_prm_uab_areaparamo, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
)
SELECT
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id FROM ladm.prm_derechotipo WHERE ilicode ILIKE ddr_tipo_derecho) AS tipo,
    NULL AS descripcion,
    -- Vinculamos con prm_interesado utilizando ILIKE para buscar coincidencias con Ministerio de Ambiente
    (SELECT t_id 
     FROM ladm.prm_interesado 
     WHERE nombre ILIKE '%Ministerio de Ambiente%' LIMIT 1) AS interesado_prm_interesado,
    NULL AS interesado_prm_agrupacioninteresados,
    NULL AS unidad_prm_uab_zonificacion, 
    (SELECT t_id 
     FROM ladm.prm_uab_areaparamo 
     WHERE local_id ILIKE uab_identificador LIMIT 1) AS unidad_prm_uab_areaparamo,
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'prm_responsabilidad' AS espacio_de_nombres, 
    uab_identificador AS local_id
FROM 
    estructura_intermedia.prm_uab_areaparamo;


--3.4 diligenciamiento de responsabilidad
INSERT INTO ladm.prm_responsabilidad(
    t_basket, 
    t_ili_tid, 
    tipo, 
    descripcion, 
    interesado_prm_interesado, 
    interesado_prm_agrupacioninteresados, 
    unidad_prm_uab_zonificacion, 
    unidad_prm_uab_areaparamo, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id)
SELECT
    -- Obtener el t_basket
    (SELECT t_id 
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
    -- Generar un nuevo UUID para t_ili_tid
    uuid_generate_v4() AS t_ili_tid,
    -- Obtener el tipo de responsabilidad desde ladm.prm_responsabilidadtipo
    (SELECT t_id 
     FROM ladm.prm_responsabilidadtipo 
     WHERE ilicode LIKE ea.ddr_tipo_resposabilidad) AS tipo,
    -- Descripción nula
    NULL AS descripcion,
    -- Obtener el t_id del interesado usando prm_interesado = nombre
    (SELECT t_id 
     FROM ladm.prm_interesado 
     WHERE nombre = ea.prm_interesado
     LIMIT 1) AS interesado_prm_interesado,
    -- Obtener el t_id de agrupacioninteresados utilizando el nombre como referencia
    (SELECT t_id 
     FROM ladm.prm_agrupacioninteresados 
     WHERE nombre = ea.prm_agrupacioninteresados
     LIMIT 1) AS interesado_prm_agrupacioninteresados,
    -- Dejar nulo el campo de zonificación
    NULL AS unidad_prm_uab_zonificacion,
    -- Obtener el t_id del área páramo
    (SELECT t_id 
     FROM ladm.prm_uab_areaparamo 
     WHERE local_id = ea.uab_identificador
     LIMIT 1) AS unidad_prm_uab_areaparamo,
    -- Fecha de comienzo de la vida útil
    NOW() AS comienzo_vida_util_version,
    -- Fecha de fin de la vida útil como NULL
    NULL AS fin_vida_util_version,
    -- Espacio de nombres fijo
    'prm_responsabilidad' AS espacio_de_nombres,
    -- Identificador local
    ea.uab_identificador AS local_id
FROM 
    estructura_intermedia.prm_uab_areaparamo ea;


--3.5 diligenciamiento de la tabla  fuente administrativa
INSERT INTO ladm.prm_fuenteadministrativa(
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
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
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
    'prm_fuenteadministrativa' AS espacio_de_nombres, 
    f.uab_identificador AS local_id
FROM 
    estructura_intermedia.prm_fuenteadministrativa f,
    estructura_intermedia.prm_uab_areaparamo a
WHERE 
    f.uab_identificador = a.uab_identificador;

--================================================================================
-- 4. Migración de  zonificacion
--================================================================================
SET CONSTRAINTS ALL IMMEDIATE;
ALTER TABLE ladm.prm_regimenusosactividades ADD COLUMN local_id VARCHAR(255);

INSERT INTO ladm.prm_regimenusosactividades (
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
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
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
    estructura_intermedia.prm_regimenusosactividades a;


--4.2 diligenciamiento de la tabla   zonificación
INSERT INTO ladm.prm_uab_zonificacion(
    t_basket, 
    t_ili_tid, 
    tipo_zona, 
    detalle_zona,
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
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
    -- Generar un nuevo UUID para t_ili_tid
    uuid_generate_v4() AS t_ili_tid,
    -- Obtener el tipo de zona desde ladm.prm_zonificaciontipo
    (SELECT t_id 
     FROM ladm.prm_zonificaciontipo 
     WHERE ilicode LIKE z.uab_tipo_zona LIMIT 1) AS tipo_zona,
    -- Detalle de la zona directamente desde la tabla origen
    z.uab_detalle_zona AS detalle_zona,
    -- Extraer el t_id de ladm.prm_regimenusosactividades basado en el id_local de estructura_intermedia.prm_uab_zonificacion
    (SELECT l.t_id 
     FROM ladm.prm_regimenusosactividades l
     WHERE l.local_id = z.uab_regimen_usos 
     LIMIT 1) AS regimen_usos,
    -- Obtener el área páramo desde ladm.prm_uab_areaparamo
    (SELECT t_id 
     FROM ladm.prm_uab_areaparamo 
     WHERE nombre_paramo IN 
         (SELECT a.uab_nombre_paramo 
          FROM estructura_intermedia.prm_uab_areaparamo a 
          WHERE a.uab_identificador = z.uab_areaparamo)
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
    'prm_uab_zonificacion' AS espacio_de_nombres, 
    -- Identificador local
    z.uab_identificador AS local_id
FROM 
    estructura_intermedia.prm_uab_zonificacion z;

    SET CONSTRAINTS ALL IMMEDIATE;
ALTER table ladm.prm_regimenusosactividades DROP COLUMN local_id;


--4.3 diligenciamiento de la tabla  prm_ue_zonificacion
INSERT INTO ladm.prm_ue_zonificacion (
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_PRM.PRM' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	null as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'prm_ue_zonificacion' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.prm_uab_zonificacion; 

------------------------------------------------------------------------------------
--4.4 diligenciamiento de la tabla  prm_responsabilidad para prm_uab_zonificacion
INSERT INTO ladm.prm_responsabilidad(
    t_basket, 
    t_ili_tid, 
    tipo, 
    descripcion, 
    interesado_prm_interesado, 
    interesado_prm_agrupacioninteresados, 
    unidad_prm_uab_zonificacion, 
    unidad_prm_uab_areaparamo, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
)
SELECT
    -- Obtener el t_basket
    (SELECT t_id 
     FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
    -- Generar un nuevo UUID para t_ili_tid
    uuid_generate_v4() AS t_ili_tid,
    -- Obtener el tipo de responsabilidad desde ladm.prm_responsabilidadtipo
    (SELECT t_id 
     FROM ladm.prm_responsabilidadtipo 
     WHERE ilicode LIKE ea.ddr_tipo_resposabilidad) AS tipo,
    -- Descripción nula
    NULL AS descripcion,
    -- Obtener el t_id del interesado usando prm_interesado = nombre
    (SELECT t_id 
     FROM ladm.prm_interesado 
     WHERE nombre = ea.prm_interesado
     LIMIT 1) AS interesado_prm_interesado,
    -- Obtener el t_id de agrupacioninteresados utilizando el nombre como referencia
    (SELECT t_id 
     FROM ladm.prm_agrupacioninteresados 
     WHERE nombre = ea.prm_agrupacioninteresados
     LIMIT 1) AS interesado_prm_agrupacioninteresados,
    -- Relacionar con zonificación
    (SELECT t_id 
     FROM ladm.prm_uab_zonificacion 
     WHERE local_id = ea.uab_identificador
     LIMIT 1) AS unidad_prm_uab_zonificacion,
    -- Dejar nulo el campo de área páramo
    NULL AS unidad_prm_uab_areaparamo,
    -- Fecha de comienzo de la vida útil
    NOW() AS comienzo_vida_util_version,
    -- Fecha de fin de la vida útil como NULL
    NULL AS fin_vida_util_version,
    -- Espacio de nombres fijo
    'prm_responsabilidad' AS espacio_de_nombres,
    -- Identificador local
    ea.uab_identificador AS local_id
FROM 
    estructura_intermedia.prm_uab_zonificacion ea;




--4.5 diligenciamiento de la tabla prm_fuenteadministrativa para prm_uab_areaparamo
INSERT INTO ladm.prm_fuenteadministrativa(
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
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
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
    'prm_fuenteadministrativa' AS espacio_de_nombres, 
    f.uab_identificador AS local_id
FROM 
    estructura_intermedia.prm_fuenteadministrativa f,
    estructura_intermedia.prm_uab_zonificacion a
WHERE 
    f.uab_identificador = a.uab_identificador;
--- Importa que se repita la fuente o no
--- col_unidad_fuente= relacion la fuente con la unnidad administrativa
--- col_resposable_fuente= puede ser el ministerio

--================================================================================
-- 7. Migración de col_rrrfuente
--================================================================================
INSERT INTO ladm.col_rrrfuente(
	t_basket, 
	fuente_administrativa, 
	rrr_prm_derecho, 
	rrr_prm_responsabilidad)
select
(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_PRM.PRM' limit 1) as t_basket,
fuente_administrativa,
rrr_prm_derecho,
rrr_prm_responsabilidad
from (
	select distinct 
	f.t_id as fuente_administrativa,
	null::int8 as rrr_prm_derecho,
	r.t_id as rrr_prm_responsabilidad
	from ladm.prm_fuenteadministrativa f,
	ladm.prm_responsabilidad r
	where f.local_id=r.local_id 
	union  
	select distinct 
	f.t_id as fuente_administrativa,
	d.t_id as rrr_prm_derecho,
	null::int4 as rrr_prm_responsabilidad
	from ladm.prm_fuenteadministrativa f,
	ladm.prm_derecho d
	where f.local_id=d.local_id
) t;

--================================================================================
-- 8. Migración de col_uebaunit
--================================================================================
INSERT INTO ladm.col_uebaunit(
    t_basket, 
    ue_prm_ue_zonificacion, 
    ue_prm_ue_areaparamo, 
    baunit_prm_uab_zonificacion,	 
    baunit_prm_uab_areaparamo
)
SELECT
    (SELECT t_id FROM ladm.t_ili2db_basket WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_PRM.PRM' LIMIT 1) AS t_basket,
    ue_prm_ue_zonificacion, 
    ue_prm_ue_areaparamo, 
    baunit_prm_uab_zonificacion,	 
    baunit_prm_uab_areaparamo
FROM (
    -- Zonificación
    SELECT 
        uez.t_id::int8 AS ue_prm_ue_zonificacion,
        NULL::int8     AS ue_prm_ue_areaparamo,
        uabz.t_id::int8 AS baunit_prm_uab_zonificacion,
        NULL::int8     AS baunit_prm_uab_areaparamo
    FROM ladm.prm_ue_zonificacion uez
    JOIN ladm.prm_uab_zonificacion uabz
      ON uez.local_id = uabz.local_id

    UNION ALL	

    -- Área Páramo o Reserva
    SELECT 
        NULL::int8     AS ue_prm_ue_zonificacion,
        uez.t_id::int8 AS ue_prm_ue_areaparamo,
        NULL::int8     AS baunit_prm_uab_zonificacion,
        uabz.t_id::int8 AS baunit_prm_uab_areaparamo
    FROM ladm.prm_ue_areaparamo uez
    JOIN ladm.prm_uab_areaparamo uabz
      ON uez.local_id = uabz.local_id
) t;


    """

    return sql_script


