def estructura_intermedia():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/*****************************
             Creación de estructura de datos intermedia 
        	Migración del RFPP  al modelo LADM_COL-RFPP
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
-- Área de reserva RFPP
--=============================================
DROP TABLE IF exists rfpp_uab_areareserva;

CREATE TABLE rfpp_uab_areareserva (
	uab_identificador varchar(7) NOT NULL, -- Identificador interno con el cual se identifica el área de reserva de RFPP
	uab_nombre_reserva varchar(150) NOT NULL, -- Nombre de la reserva de acuerdo con la RFPPª de 1959
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	interesado_tipo varchar(255) NOT NULL,
	interesado_nombre varchar(255) NULL, -- Nombre del interesado.
	interesado_tipo_interesado varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento varchar(255) NULL, -- Tipo de documento de identificación del interesado
	interesado_numero_documento varchar(255) NULL, -- Número del documento del interesado
	fuente_administrativa_tipo varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL, -- Indica si la fuente está o no disponible y en qué condiciones. También puede indicar porqué ha dejado de estar disponible, si ha ocurrido.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_nombre varchar(255) null, -- Nombre de la fuente, ejemplo: número de la resolución, número de la escritura pública o número de radicado de una sentencia.
	ddr_tipo_resposabilidad varchar(255) null,
	ddr_tipo_derecho varchar(255) NULL
);

--=============================================
-- Compensacion RFPP
--=============================================
DROP TABLE IF exists rfpp_uab_compensacion;

CREATE TABLE rfpp_uab_compensacion (
	uab_identificador varchar(15) NOT NULL, -- Interno con el cual se identifica la reserva a la cual se está haciendo la compensación
	uab_expediente varchar(100) NOT NULL, -- Número del expediente interno relacionado con la compensación
	uab_observaciones text NULL, -- Nombre del proyecto objeto de la sustracción
	uab_areareserva varchar(255) NOT NULL,
	uab_sustraccion varchar(255) NULL,
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	interesado_tipo varchar(255) NOT NULL,
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
-- Sustraccion RFPP
--=============================================
DROP TABLE IF exists rfpp_uab_sustraccion;

CREATE TABLE rfpp_uab_sustraccion (
	uab_identificador varchar(15) NOT NULL, -- Identificador interno para identificación de la sustracción definitiva o temporal de la Reserva RFPPª de 1959
	uab_expediente varchar(100) NOT NULL,
	uab_tipo_sustraccion varchar(255) NOT NULL, -- Dominio que define el tipo de sustracción realikzado a la Reserva RFPPª de 1959
	uab_fin_sustraccion date NULL, -- Corresponde a la fecha en la que finaliza la sustracción (aplica para las sustracciones temporales)
	uab_tipo_sector varchar(255) NOT NULL, -- Identifica el sector que realizó la solicitud de sustracción
	uab_detalle_sector varchar(255) NULL, -- Cuando el tipo de sector sea Otro, se podrá especificar el detalle de este sector
	uab_solicitante varchar(255) null,
	uab_observaciones text NULL, -- Observaciones generales de la sustracción
	uab_areareserva varchar(255) NULL,
	uab_nombre_areareserva varchar(255) NULL,
	uab_compensacion varchar(255) NULL,	
	uab_nombre varchar(255) NULL, -- Nombre que recibe la unidad administrativa básica, en muchos casos toponímico, especialmente en terrenos rústicos.
	ue_geometria public.geometry(multipolygonz, 9377) NULL, -- Materialización del método createArea(). Almacena de forma permanente la geometría de tipo poligonal.
	interesado_tipo varchar(255) NOT NULL,
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
-- Fuente Administrativa RFPP
--=============================================
DROP TABLE IF exists rfpp_fuenteadministrativa;

CREATE TABLE rfpp_fuenteadministrativa (
	uab_identificador varchar(15) NOT NULL, -- Interno con el cual se identifica la reserva a la cual se está haciendo la compensación
	fuente_administrativa_fecha_documento_fuente date NULL, -- Fecha de expedición del documento de la fuente.
	fuente_administrativa_tipo_formato varchar(255) NULL, -- Tipo de formato en el que es presentada la fuente, de acuerdo con el registro de metadatos.
	fuente_administrativa_numero int4 null,
	fuente_administrativa_anio int4 null
);

--=============================================
-----------------------------------------------
-- Funciones de homologacion RFPP
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
/****
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración del RFPP  al modelo LADM_COL-RFPP
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
        email           : contacto@ceicol.com
 ***/
/***
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 ****/

--========================================
--Fijar esquema
--========================================
set search_path to 
	estructura_intermedia, -- Esquema de estructura de datos intermedia
	public;


--========================================
-- Área de reserva
--========================================
INSERT INTO estructura_intermedia.rfpp_uab_areareserva(
	uab_identificador, 
	uab_nombre_reserva, 
	uab_nombre, 
	ue_geometria, 
	interesado_tipo, 
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
	ddr_tipo_derecho)		
SELECT 
	'RFPP_01' AS uab_identificador, 
	rfpp AS uab_nombre_reserva, 
	'rfpp_uab_areareserva'::varchar(255) AS uab_nombre, 
	ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
	'Regulador'::varchar(255) AS interesado_tipo,
	'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre, 
	'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
	'NIT'::varchar(255) AS interesado_tipo_documento, 
	'830.115.395-1'::varchar(255) AS interesado_numero_documento, 
	'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
	'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
	'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
	to_date('1959-01-17','YYYY-MM-DD') AS fuente_administrativa_fecha_documento_fuente, 
	'Resolución 0138 de 2014'::varchar(255) AS fuente_administrativa_nombre,
	null AS ddr_tipo_resposabilidad,
	'Realinderar'::varchar(255) AS ddr_tipo_derecho
FROM insumos.area_reserva;

--========================================
-- Compensación RFPP
--========================================
INSERT INTO estructura_intermedia.rfpp_uab_compensacion(
	uab_identificador, 
	uab_expediente, 
	uab_observaciones, 
	uab_areareserva, 
	uab_sustraccion,
	uab_nombre, 
	ue_geometria, 
	interesado_tipo, 
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
	ddr_tipo_derecho)
SELECT 
	('CMRFPP_' || row_number() OVER ())::varchar(255) AS uab_identificador,
	expediente AS uab_expediente,
	proyecto AS uab_observaciones,
	upper(id_reserva)::varchar(255) AS uab_areareserva,
	null as uab_sustraccion,
	'rfpp_uab_compensacion' AS uab_nombre,
	ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
	'Solicitante'::varchar(255) AS interesado_tipo,
	null AS interesado_nombre, 
	'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
	null AS interesado_tipo_documento, 
	null AS interesado_numero_documento, 
	'Documento_Publico.' AS fuente_administrativa_tipo,
	'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
	'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
	fecha_acto AS fuente_administrativa_fecha_documento_fuente, 
	acto_admin::varchar(255) AS fuente_administrativa_nombre,
	'Compensar'::varchar(255) AS ddr_tipo_resposabilidad,
	null AS ddr_tipo_derecho
FROM insumos.compensaciones
WHERE left(id_reserva, 4) = 'RFPP';


--========================================
-- Sustracción RFPP
--========================================
INSERT INTO estructura_intermedia.rfpp_uab_sustraccion(
    uab_identificador,
    uab_expediente,
    uab_tipo_sustraccion,
    uab_fin_sustraccion,
    uab_tipo_sector,
    uab_detalle_sector,
    uab_solicitante,
    uab_observaciones,
    uab_areareserva,
    uab_nombre_areareserva,
    uab_compensacion,
    uab_nombre,
    ue_geometria,
    interesado_tipo,
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
WITH sustracciones_definitivas_clean AS (
    SELECT 
        ('SDRFPP_' || row_number() OVER ())::varchar(255) AS uab_identificador,
        'RFPP_01' AS uab_areareserva,
        CASE 
            WHEN acto_admin = 'Resolución 2427 del 27 de noviembre de 2015' THEN 'Resolución 2427 de 2015'
            WHEN acto_admin = 'Res 1705 - 2016 MODIFICA Res 1166 - 2013' THEN 'Resolución 1705 de 2016/ Resolución 1166 de 2013'
            ELSE acto_admin
        END AS uab_acto_admin,   
        CASE 
            WHEN sector::integer = 0 THEN 'Infraestructura_Transporte'
            WHEN sector::integer = 1 THEN 'Mineria'
            WHEN sector::integer = 2 THEN 'Energia'
            WHEN sector::integer = 3 THEN 'Hidrocarburos'
            WHEN sector::integer = 4 THEN 'Area_Urbana_Expansion_Rural'
            WHEN sector::integer = 5 THEN 'Vivienda_VIS_VIP'
            WHEN sector::integer = 6 THEN 'Restitucion_Tierras'
            WHEN sector::integer = 7 THEN 'Reforma_Agraria'
            WHEN sector::integer = 8 THEN 'Inciso_Segundo'
            ELSE 'Otro'
        END AS uab_tipo_sector,
        *
    FROM insumos.sustracciones
)
SELECT 
    uab_identificador,                                                          -- 1. Identificador
    COALESCE(expediente, 'Sin información') AS uab_expediente,                  -- 2. Expediente
    'Definitiva'::varchar(255) AS uab_tipo_sustraccion,                         -- 3. Tipo de sustracción
    NULL AS uab_fin_sustraccion,                                                -- 4. Fin de sustracción
    uab_tipo_sector,                                                            -- 5. Tipo de sector
    CASE 
        WHEN uab_tipo_sector = 'Otro' THEN sector::TEXT
        ELSE NULL
    END AS uab_detalle_sector,                                                  -- 6. Detalle del sector
    COALESCE(solicitant::varchar(255), 'Sin información') AS uab_solicitante,     -- 7. Solicitante
    detalle AS uab_observaciones,                                               -- 8. Observaciones
    uab_areareserva,                                                            -- 9. Área reserva (código)
    'Cuenca Alta del Río Bogotá' AS uab_nombre_areareserva,                    -- 10. Nombre del área reserva
    NULL AS uab_compensacion,                                                   -- 11. Compensación
    'rfpp_uab_sustraccion' AS uab_nombre,                                       -- 12. Nombre de la sustracción
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,                       -- 13. Geometría
    'Solicitante'::varchar(255) AS interesado_tipo,                             -- 14. Tipo de interesado
    'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre,  -- 15. Nombre del interesado
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado,             -- 16. Tipo de interesado (detalle)
    'NIT'::varchar(255) AS interesado_tipo_documento,                           -- 17. Tipo de documento
    '830.115.395-1'::varchar(255) AS interesado_numero_documento,               -- 18. Número de documento
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,           -- 19. Tipo de fuente
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad,  -- 20. Estado de disponibilidad de la fuente
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato,            -- 21. Tipo de formato de la fuente
    NULL AS fuente_administrativa_fecha_documento_fuente,                       -- 22. Fecha de expedición de la fuente
    uab_acto_admin::varchar(255) AS fuente_administrativa_nombre,               -- 23. Nombre de la fuente
    NULL AS ddr_tipo_resposabilidad,                                            -- 24. Tipo de resposabilidad
    'Sustraer'::varchar(255) AS ddr_tipo_derecho                                -- 25. Tipo de derecho
FROM sustracciones_definitivas_clean;



--========================================
-- Fuentes Administrativas de Áreas de Reserva
--========================================
INSERT INTO estructura_intermedia.rfpp_fuenteadministrativa(
	uab_identificador, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_numero, 
	fuente_administrativa_anio
)
WITH areareserva_acto AS (
	SELECT uab_identificador,
	       fuente_administrativa_fecha_documento_fuente,
	       substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS fuente_administrativa_tipo,
	       substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)) AS numero_acto,
	       substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
	FROM estructura_intermedia.rfpp_uab_areareserva cl
)
SELECT uab_identificador,
       fuente_administrativa_fecha_documento_fuente,
       CASE 
		    WHEN fuente_administrativa_tipo= 'Resolución' THEN 'Resolucion'
		    ELSE fuente_administrativa_tipo
		END AS fuente_administrativa_tipo,
       estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_numero,
       estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM areareserva_acto;

--========================================
-- Fuentes Administrativas de Sustracciones
--========================================
INSERT INTO estructura_intermedia.rfpp_fuenteadministrativa(
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
		trim(unnest(string_to_array(cl.fuente_administrativa_nombre, '/'))) AS fuente_administrativa_nombre
	FROM estructura_intermedia.rfpp_uab_sustraccion cl   
), homologado_acto_sustraccion AS (
	SELECT 
		uab_identificador,
		acto,
		fuente_administrativa_fecha_documento_fuente,
		CASE 
			WHEN fuente_administrativa_nombre ILIKE 'Res%' THEN 'Resolucion' || substring(fuente_administrativa_nombre FROM 4)
			WHEN fuente_administrativa_nombre ILIKE 'Resoluci%' THEN 'Resolucion' || substring(fuente_administrativa_nombre FROM 10)
			ELSE fuente_administrativa_nombre
		END AS fuente_administrativa_nombre
	FROM split_acto_sustraccion
), sustraccion_acto AS (
	SELECT 
		uab_identificador,
		acto,
		fuente_administrativa_fecha_documento_fuente,
		substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS tipo_acto,
		substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)) AS numero_acto,
		substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
	FROM homologado_acto_sustraccion cl  
)
SELECT 
	uab_identificador,
	fuente_administrativa_fecha_documento_fuente,
	CASE 
		WHEN tipo_acto ILIKE 'Resoluc%' THEN 'Resolucion'
		WHEN tipo_acto ILIKE 'Res' THEN 'Resolucion'
		ELSE tipo_acto
	END AS fuente_administrativa_tipo,
	estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_numero,
	estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM sustraccion_acto;


--========================================
-- Fuentes Administrativas de Compensaciones
--========================================
INSERT INTO estructura_intermedia.rfpp_fuenteadministrativa(
	uab_identificador, 
	fuente_administrativa_fecha_documento_fuente, 
	fuente_administrativa_tipo_formato, 
	fuente_administrativa_numero, 
	fuente_administrativa_anio
)
WITH clean_acto_compensacion AS (
	SELECT 
		uab_identificador,
		uab_areareserva,
		fuente_administrativa_fecha_documento_fuente,
		fuente_administrativa_nombre
	FROM estructura_intermedia.rfpp_uab_compensacion cl 
), split_acto_compensacion AS (
	SELECT 
		uab_identificador,
		uab_areareserva,
		fuente_administrativa_fecha_documento_fuente,
		cl.fuente_administrativa_nombre AS acto,
		trim(unnest(string_to_array(cl.fuente_administrativa_nombre, '/'))) AS fuente_administrativa_nombre
	FROM clean_acto_compensacion cl 
), compensacion_acto AS (
	SELECT 
		uab_identificador,
		fuente_administrativa_fecha_documento_fuente,
		substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS fuente_administrativa_tipo,
		substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)) AS fuente_administrativa_numero,
		substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS fuente_administrativa_anio
	FROM split_acto_compensacion cl
	WHERE left(uab_areareserva, 4) = 'RFPP' 
)
SELECT 
	uab_identificador,
	fuente_administrativa_fecha_documento_fuente,
	CASE 
	    WHEN fuente_administrativa_tipo = 'Resolución' THEN 'Resolucion'
	    ELSE fuente_administrativa_tipo
	END AS fuente_administrativa_tipo,
	estructura_intermedia.homologar_numero(fuente_administrativa_numero::text)::int4 AS fuente_administrativa_numero,
	estructura_intermedia.homologar_numero(fuente_administrativa_anio::text)::int4 AS fuente_administrativa_anio
FROM compensacion_acto;


--========================================
-- Relacionar Compensaciones a las Sustracciones
--========================================
WITH fuente_s AS (
	SELECT *
	FROM estructura_intermedia.rfpp_fuenteadministrativa
	WHERE left(uab_identificador, 1) = 'S'
), fuente_c AS (
	SELECT *
	FROM estructura_intermedia.rfpp_fuenteadministrativa
	WHERE left(uab_identificador, 1) = 'C'
), fuente_sustraccion_compensacion as (
	SELECT DISTINCT 
		s.uab_identificador AS uab_sustraccion,
		c.uab_identificador AS uab_compensacion
	FROM fuente_c c  
	LEFT JOIN fuente_s s
		ON s.fuente_administrativa_tipo_formato = c.fuente_administrativa_tipo_formato
		AND s.fuente_administrativa_numero = c.fuente_administrativa_numero
		AND s.fuente_administrativa_anio = c.fuente_administrativa_anio
	WHERE s.uab_identificador IS NOT null
), asigna_expendiente_sustraccion as (
	select c.*,uc.uab_expediente
	from fuente_sustraccion_compensacion c
	left join estructura_intermedia.rfpp_uab_compensacion uc
	on c.uab_compensacion=uc.uab_identificador
)
update estructura_intermedia.rfpp_uab_sustraccion
set uab_compensacion=sc.uab_compensacion
from asigna_expendiente_sustraccion sc
where sc.uab_sustraccion=rfpp_uab_sustraccion.uab_identificador;



--========================================
-- Relacionar Sustracciones a las Compensaciones
--========================================
WITH fuente_s AS (
	SELECT *
	FROM estructura_intermedia.rfpp_fuenteadministrativa
	WHERE left(uab_identificador, 1) = 'S'
), fuente_c AS (
	SELECT *
	FROM estructura_intermedia.rfpp_fuenteadministrativa
	WHERE left(uab_identificador, 1) = 'C'
), fuente_sustraccion_compensacion as (
	SELECT DISTINCT 
		s.uab_identificador AS uab_sustraccion,
		c.uab_identificador AS uab_compensacion
	FROM  fuente_s  s
	LEFT JOIN fuente_c c
		ON s.fuente_administrativa_tipo_formato = c.fuente_administrativa_tipo_formato
		AND s.fuente_administrativa_numero = c.fuente_administrativa_numero
		AND s.fuente_administrativa_anio = c.fuente_administrativa_anio
	WHERE s.uab_identificador IS NOT null
), asigna_solicitante_compensacion as (
	select c.*,s.interesado_nombre
	from fuente_sustraccion_compensacion c
	left join estructura_intermedia.rfpp_uab_sustraccion s 
	on c.uab_sustraccion=s.uab_identificador
)
update estructura_intermedia.rfpp_uab_compensacion
set uab_sustraccion=sc.uab_sustraccion,
interesado_nombre=sc.interesado_nombre
from asigna_solicitante_compensacion sc
where sc.uab_compensacion=rfpp_uab_compensacion.uab_identificador;

--========================================
-- Se eliminan sustraciones que tienen relacionada dos o mas compensaciones.
--========================================
delete
from estructura_intermedia.rfpp_uab_sustraccion
where uab_compensacion in (
	select uab_compensacion
	from estructura_intermedia.rfpp_uab_sustraccion
	where uab_compensacion is not null
	group by uab_compensacion
	having count(*)>1
);

--========================================
-- Eliminan  Compensaciones que no tienen relacionada sustraccion.
--========================================
delete
from estructura_intermedia.rfpp_uab_compensacion
where uab_sustraccion not in(
	select uab_identificador
	from estructura_intermedia.rfpp_uab_sustraccion 
) 
or uab_sustraccion is null 
or uab_sustraccion  in(
	select uab_sustraccion
	from estructura_intermedia.rfpp_uab_compensacion
	group by uab_sustraccion
	having count(*)>1
);

 

    """

    return sql_script



def validar_estructura():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/**********
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración de RFPP  al modelo LADM_COL-RFPP
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)        
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
--Fijar esquema
--========================================
set search_path to 
	estructura_intermedia, -- Esquema de estructura de datos intermedia
	public;


--========================================
-- 1 Área de reserva
--========================================
SELECT *
FROM estructura_intermedia.rfpp_uab_areareserva;

SELECT count(*)
FROM estructura_intermedia.rfpp_uab_areareserva;


select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rfpp_fuenteadministrativa
where left(uab_identificador,1) ='R'
group by fuente_administrativa_tipo_formato;

--========================================
-- 2 Compensación 
--========================================
SELECT *		
FROM estructura_intermedia.rfpp_uab_compensacion;

select count(*) 
FROM estructura_intermedia.rfpp_uab_compensacion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rfpp_uab_compensacion
group by uab_areareserva
order by uab_areareserva;


select uab_sustraccion,count(*)
FROM estructura_intermedia.rfpp_uab_compensacion
group by uab_sustraccion
having count(*)>1;

select *
from estructura_intermedia.rfpp_uab_compensacion
where uab_sustraccion is null;



select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rfpp_fuenteadministrativa
where left(uab_identificador,1) ='C'
group by fuente_administrativa_tipo_formato;


-- Se listan Compensaciones que no tienen relacionada sustraccion
select *
from estructura_intermedia.rfpp_uab_compensacion
where uab_sustraccion is null;


-- Eliminan Compensaciones que no tienen relacionada sustraccion
delete
from estructura_intermedia.rfpp_uab_compensacion
where uab_sustraccion is null;

--========================================
-- 3. Sustracción 
--========================================
SELECT *		
FROM estructura_intermedia.rfpp_uab_sustraccion;

select count(*) 
FROM estructura_intermedia.rfpp_uab_sustraccion;

select uab_tipo_sustraccion,count(*) 
FROM estructura_intermedia.rfpp_uab_sustraccion
group by uab_tipo_sustraccion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rfpp_uab_sustraccion
group by uab_areareserva
order by uab_areareserva;


select uab_compensacion ,count(*)
from estructura_intermedia.rfpp_uab_sustraccion
group by uab_compensacion 
order by count(*) desc;

select *
from estructura_intermedia.rfpp_uab_sustraccion
where uab_compensacion in (
select uab_compensacion
from estructura_intermedia.rfpp_uab_sustraccion
where uab_compensacion is not null
group by uab_compensacion
having count(*)>1
);


delete
from estructura_intermedia.rfpp_uab_sustraccion
where uab_compensacion in (
select uab_compensacion
from estructura_intermedia.rfpp_uab_sustraccion
where uab_compensacion is not null
group by uab_compensacion
having count(*)>1
);

select uab_compensacion, count(*)
from ladm.rfpp_uab_sustraccion rus 
where uab_compensacion is not null 
group by rus.uab_compensacion 
having count(*) > 1;

select rd.* 
from ladm.rfpp_derecho rd
left join (
	select * from ladm.col_rrrfuente 
	where rrr_rfpp_responsabilidad is null
) as cr 
on rd.t_id = cr.rrr_rfpp_derecho 
where cr.t_id is null;


select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rfpp_fuenteadministrativa
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
/**********************************************************************************
            ETL de tranformación de insumos a estructura de datos intermedia
        			Migración del Ley 2  al modelo LADM_COL-LEY2
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

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'arfw_etl_rfpp' AND pid <> pg_backend_pid();

--========================================
--Fijar esquema
--========================================
set search_path to
	estructura_intermedia,	-- Esquema de estructura de datos intermedia
	ladm,		-- Esquema modelo LADM-LEY2
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
	'LADM_COL_v_1_0_0_Ext_RFPP.RFPP',
	uuid_generate_v4(),
	'ETL de importación de datos',
	NULL );

--================================================================================
-- 2. Migración de rfpp_interesado
--================================================================================
INSERT INTO ladm.rfpp_interesado(
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	null as observacion,
	interesado_nombre, 
	(select t_id from ladm.col_interesadotipo where ilicode like interesado_tipo_interesado) as tipo_interesado,
	(select t_id from ladm.col_documentotipo where ilicode like interesado_tipo_documento) as interesado_tipo_documento,
	interesado_numero_documento,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpp_interesado' as espacio_de_nombres, 
	row_number() OVER (ORDER BY interesado_nombre)   local_id
from (
	select 
		interesado_tipo,
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rfpp_uab_areareserva
	union
	select 
		interesado_tipo,
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rfpp_uab_sustraccion
	union
	select 
		interesado_tipo,
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rfpp_uab_compensacion
) t;

--================================================================================
-- 3. Migración de  Area de Reserva
--================================================================================
--3.1 diligenciamiento de la tabla  rfpp_uab_areareserva
INSERT INTO ladm.rfpp_uab_areareserva(
	t_basket, 
	t_ili_tid, 
	nombre_reserva, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)	
select
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	uab_nombre_reserva,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpp_uab_areareserva' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rfpp_uab_areareserva; 

--3.2 diligenciamiento de la tabla  rfpp_ue_areareserva
INSERT INTO ladm.rfpp_ue_areareserva(
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	uab_nombre_reserva as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpp_ue_areareserva' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rfpp_uab_areareserva; 

--3.3 diligenciamiento de la tabla  rfpp_derecho para rfpp_uab_areareserva
INSERT INTO ladm.rfpp_derecho (
    t_basket, 
    t_ili_tid, 
    tipo, 
    descripcion, 
    interesado_rfpp_interesado, 
    interesado_rfpp_agrupacioninteresados, 
    unidad_rfpp_uab_compensacion, 
    unidad_rfpp_uab_sustraccion, 
    unidad_rfpp_uab_areareserva, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
) 
SELECT 
    (SELECT t_id FROM ladm.t_ili2db_basket 
     WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' 
     LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id FROM ladm.rfpp_derechotipo 
     WHERE ilicode = ddr_tipo_derecho 
     LIMIT 1) AS tipo,
    NULL AS descripcion,
    (SELECT t_id FROM ladm.rfpp_interesado 
     WHERE nombre = interesado_nombre 
     LIMIT 1) AS interesado_rfpp_interesado, 
    NULL AS interesado_rfpp_agrupacioninteresados, 
    NULL AS unidad_rfpp_uab_compensacion, 
    NULL AS unidad_rfpp_uab_sustraccion, 
    (SELECT t_id FROM ladm.rfpp_uab_areareserva 
     WHERE local_id = uab_identificador 
     LIMIT 1) AS unidad_rfpp_uab_areareserva,
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'rfpp_derecho' AS espacio_de_nombres, 
    uab_identificador AS local_id
FROM estructura_intermedia.rfpp_uab_areareserva;



--3.4 diligenciamiento de la tabla rfpp_fuenteadministrativa para rfpp_uab_areareserva
INSERT INTO ladm.rfpp_fuenteadministrativa(
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
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
from estructura_intermedia.rfpp_fuenteadministrativa f,estructura_intermedia.rfpp_uab_areareserva a
where f.uab_identificador =a.uab_identificador;


--================================================================================
-- 5. Migración de  compensacion
--================================================================================
INSERT INTO ladm.rfpp_uab_compensacion(
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	c.uab_expediente,
	c.uab_observaciones,
	(select t_id from ladm.rfpp_uab_areareserva where nombre_reserva in (select a.uab_nombre_reserva from estructura_intermedia.rfpp_uab_areareserva a where a.uab_identificador=c.uab_areareserva)) as tipo,
	uab_nombre,
	(select t_id from ladm.col_unidadadministrativabasicatipo where ilicode like 'Ambiente_Desarrollo_Sostenible') as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpp_uab_compensacion' as espacio_de_nombres, 
	c.uab_identificador as local_id
from  estructura_intermedia.rfpp_uab_compensacion c;	

--5.2 diligenciamiento de la tabla  rfpp_uab_compensacion
INSERT INTO ladm.rfpp_ue_compensacion (
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(st_area(ue_geometria)/10000)::numeric(13, 4) as area_ha,
	null as etiqueta,
	(select t_id from ladm.col_relacionsuperficietipo where ilicode like 'En_Rasante') as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rfpp_ue_compensacion' as espacio_de_nombres, 
	uab_identificador  local_id
from  estructura_intermedia.rfpp_uab_compensacion; 

--5.3 diligenciamiento de la tabla  rfpp_responsabilidad para rfpp_uab_compensacion
INSERT INTO ladm.rfpp_responsabilidad (
    t_basket, 
    t_ili_tid, 
    tipo, 
    descripcion, 
    interesado_rfpp_interesado, 
    interesado_rfpp_agrupacioninteresados, 
    unidad_rfpp_uab_compensacion, 
    unidad_rfpp_uab_sustraccion, 
    unidad_rfpp_uab_zonificacion, 
    unidad_rfpp_uab_areareserva, 
    comienzo_vida_util_version, 
    fin_vida_util_version, 
    espacio_de_nombres, 
    local_id
) 
SELECT 
    (SELECT t_id FROM ladm.t_ili2db_basket 
     WHERE topic = 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' 
     LIMIT 1) AS t_basket,
    uuid_generate_v4() AS t_ili_tid,
    (SELECT t_id FROM ladm.rfpp_responsabilidadtipo 
     WHERE ilicode = ddr_tipo_resposabilidad 
     LIMIT 1) AS tipo,
    NULL AS descripcion,
    (SELECT t_id FROM ladm.rfpp_interesado 
     WHERE nombre = interesado_nombre 
     LIMIT 1) AS interesado_rfpp_interesado, 
    NULL AS interesado_rfpp_agrupacioninteresados, 
    (SELECT t_id FROM ladm.rfpp_uab_compensacion 
     WHERE local_id = uab_identificador 
     LIMIT 1) AS unidad_rfpp_uab_compensacion, 
    NULL AS unidad_rfpp_uab_sustraccion, 
    NULL AS unidad_rfpp_uab_zonificacion, 
    NULL AS unidad_rfpp_uab_areareserva,
    NOW() AS comienzo_vida_util_version,
    NULL AS fin_vida_util_version,
    'rfpp_responsabilidad' AS espacio_de_nombres, 
    uab_identificador AS local_id
FROM estructura_intermedia.rfpp_uab_compensacion;

--5.4 diligenciamiento de la tabla rfpp_fuenteadministrativa para rfpp_uab_compensacion
INSERT INTO ladm.rfpp_fuenteadministrativa(
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
	(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	(select t_id from ladm.col_fuenteadministrativatipo cf where ilicode like c.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato) as tipo,
	null as fecha_fin, 
	(select t_id from ladm.col_estadodisponibilidadtipo ce  where ilicode like c.fuente_administrativa_estado_disponibilidad) as estado_disponibilidad,
	(select t_id from ladm.col_formatotipo cf  where ilicode like c.fuente_administrativa_tipo_formato) as tipo_formato,
	c.fuente_administrativa_fecha_documento_fuente  fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rfpp_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador  local_id
from estructura_intermedia.rfpp_fuenteadministrativa f,estructura_intermedia.rfpp_uab_compensacion c
where f.uab_identificador =c.uab_identificador;

--================================================================================
-- 6. Migración de sustracción
--================================================================================
INSERT INTO ladm.rfpp_uab_sustraccion(
	t_basket, 
	t_ili_tid, 
	expediente,
	tipo_sustraccion, 
	fin_sustraccion, 
	tipo_sector, 
	detalle_sector, 
	observaciones, 
	solicitante,
	uab_areareserva,
	uab_compensacion, 
	nombre, 
	tipo,
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id)
SELECT
	(SELECT t_id FROM ladm.t_ili2db_basket 
	 WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	s.uab_expediente,
	(SELECT t_id FROM ladm.rfpp_sustraccionreservatipo 
	 WHERE ilicode LIKE s.uab_tipo_sustraccion LIMIT 1) AS tipo_sustraccion,
	s.uab_fin_sustraccion,
	(SELECT t_id FROM ladm.rfpp_sectortipo 
	 WHERE ilicode LIKE s.uab_tipo_sector LIMIT 1) AS tipo_sector,
	s.uab_detalle_sector,
	s.uab_observaciones,
	s.uab_solicitante,
	(SELECT t_id FROM ladm.rfpp_uab_areareserva 
	 WHERE local_id LIKE s.uab_areareserva LIMIT 1) AS uab_areareserva,
	(SELECT t_id FROM ladm.rfpp_uab_compensacion 
	 WHERE local_id LIKE s.uab_compensacion LIMIT 1) AS uab_compensacion,
	s.uab_nombre,
	(SELECT t_id FROM ladm.col_unidadadministrativabasicatipo 
	 WHERE ilicode LIKE 'Ambiente_Desarrollo_Sostenible' LIMIT 1) AS tipo,
	NOW() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'rfpp_uab_sustraccion' AS espacio_de_nombres, 
	s.uab_identificador AS local_id
FROM estructura_intermedia.rfpp_uab_sustraccion s;

-- 6.2 Diligenciamiento de la tabla rfpp_ue_sustraccion

INSERT INTO ladm.rfpp_ue_sustraccion (
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
SELECT
	(SELECT t_id FROM ladm.t_ili2db_basket 
	 WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	(ST_Area(ue_geometria)/10000)::numeric(13, 4) AS area_ha,
	NULL AS etiqueta,
	(SELECT t_id FROM ladm.col_relacionsuperficietipo 
	 WHERE ilicode LIKE 'En_Rasante') AS relacion_superficie,
	ue_geometria,	
	NOW() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'rfpp_ue_sustraccion' AS espacio_de_nombres, 
	uab_identificador AS local_id
FROM estructura_intermedia.rfpp_uab_sustraccion;

-- 6.3 Diligenciamiento de la tabla rfpp_derecho para rfpp_uab_sustraccion

INSERT INTO ladm.rfpp_derecho (
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rfpp_interesado, 
	interesado_rfpp_agrupacioninteresados, 
	unidad_rfpp_uab_compensacion, 
	unidad_rfpp_uab_sustraccion, 
	unidad_rfpp_uab_zonificacion, 
	unidad_rfpp_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
) 
SELECT 
	(SELECT t_id FROM ladm.t_ili2db_basket 
	 WHERE topic = 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,    
	(SELECT t_id FROM ladm.rfpp_derechotipo 
	 WHERE ilicode = ddr_tipo_derecho LIMIT 1) AS tipo,
	NULL AS descripcion,
	(SELECT t_id FROM ladm.rfpp_interesado 
	 WHERE nombre = interesado_nombre LIMIT 1) AS interesado_rfpp_interesado,
	NULL AS interesado_rfpp_agrupacioninteresados, 
	NULL AS unidad_rfpp_uab_compensacion, 
	(SELECT t_id FROM ladm.rfpp_uab_sustraccion 
	 WHERE local_id = uab_identificador LIMIT 1) AS unidad_rfpp_uab_sustraccion, 
	NULL AS unidad_rfpp_uab_zonificacion, 
	NULL AS unidad_rfpp_uab_areareserva,
	NOW() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'rfpp_derecho' AS espacio_de_nombres, 
	uab_identificador AS local_id
FROM estructura_intermedia.rfpp_uab_sustraccion;

-- 6.4 Diligenciamiento de la tabla rfpp_fuenteadministrativa para rfpp_uab_sustraccion

INSERT INTO ladm.rfpp_fuenteadministrativa(
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
	(SELECT t_id FROM ladm.t_ili2db_basket 
	 WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	(SELECT t_id FROM ladm.col_fuenteadministrativatipo cf 
	 WHERE ilicode LIKE s.fuente_administrativa_tipo || f.fuente_administrativa_tipo_formato) AS tipo,
	NULL AS fecha_fin, 
	(SELECT t_id FROM ladm.col_estadodisponibilidadtipo ce  
	 WHERE ilicode LIKE s.fuente_administrativa_estado_disponibilidad) AS estado_disponibilidad,
	(SELECT t_id FROM ladm.col_formatotipo cf  
	 WHERE ilicode LIKE s.fuente_administrativa_tipo_formato) AS tipo_formato,
	s.fuente_administrativa_fecha_documento_fuente AS fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato || ' ' || f.fuente_administrativa_numero || ' de ' || f.fuente_administrativa_anio AS nombre, 
	NULL AS descripcion, 
	NULL AS url, 
	'rfpp_fuenteadministrativa' AS espacio_de_nombres, 
	f.uab_identificador AS local_id
FROM estructura_intermedia.rfpp_fuenteadministrativa f,
     estructura_intermedia.rfpp_uab_sustraccion s
WHERE f.uab_identificador = s.uab_identificador;


--================================================================================
-- 7. Migración de col_rrrfuente
--================================================================================
INSERT INTO ladm.col_rrrfuente(
	t_basket, 
	fuente_administrativa, 
	rrr_rfpp_derecho, 
	rrr_rfpp_responsabilidad)
select
(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
*
from (
	select distinct 
	f.t_id as fuente_administrativa,
	null::int8 as rrr_rfpp_derecho,
	r.t_id as rrr_rfpp_responsabilidad
	from ladm.rfpp_fuenteadministrativa f,
	ladm.rfpp_responsabilidad r
	where f.local_id=r.local_id 
	union  
	select distinct 
	f.t_id as fuente_administrativa,
	d.t_id as rrr_rfpp_derecho,
	null::int4 as rrr_rfpp_responsabilidad
	from ladm.rfpp_fuenteadministrativa f,
	ladm.rfpp_derecho d
	where f.local_id=d.local_id
) t;

--================================================================================
-- 8. Migración de col_uebaunit
--================================================================================
INSERT INTO ladm.col_uebaunit(
	t_basket, 
	ue_rfpp_ue_zonificacion, 
	ue_rfpp_ue_compensacion, 
	ue_rfpp_ue_sustraccion, 
	ue_rfpp_ue_areareserva, 
	baunit_rfpp_uab_zonificacion,
	baunit_rfpp_uab_compensacion, 
	baunit_rfpp_uab_sustraccion, 	 
	baunit_rfpp_uab_areareserva)
select
(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_RFPP.RFPP' limit 1) as t_basket,
*
from (
	select uez.t_id::int8 as ue_rfpp_ue_zonificacion,
	null::int8 as ue_rfpp_ue_compensacion,
	null::int8 as ue_rfpp_ue_sustraccion,
	null::int8 as ue_rfpp_ue_areareserva,
	uabz.t_id::int8 as baunit_rfpp_uab_zonificacion,
	null::int8 as baunit_rfpp_uab_compensacion,
	null::int8 as baunit_rfpp_uab_sustraccion,
	null::int8 as baunit_rfpp_uab_areareserva
	from ladm.rfpp_ue_zonificacion uez,
	ladm.rfpp_uab_zonificacion uabz
	where uez.local_id=uabz.local_id
	union	
	select null as ue_rfpp_ue_zonificacion,
	uez.t_id as ue_rfpp_ue_compensacion,
	null as ue_rfpp_ue_sustraccion,
	null as ue_rfpp_ue_areareserva,
	null as baunit_rfpp_uab_zonificacion,
	uabz.t_id as baunit_rfpp_uab_compensacion,
	null as baunit_rfpp_uab_sustraccion,
	null as baunit_rfpp_uab_areareserva
	from ladm.rfpp_ue_compensacion uez,
	ladm.rfpp_uab_compensacion uabz
	where uez.local_id=uabz.local_id
	union  
	select null as ue_rfpp_ue_zonificacion,
	null as ue_rfpp_ue_compensacion,
	uez.t_id as ue_rfpp_ue_sustraccion,
	null as ue_rfpp_ue_areareserva,
	null as baunit_rfpp_uab_zonificacion,
	null as baunit_rfpp_uab_compensacion,
	uabz.t_id as baunit_rfpp_uab_sustraccion,
	null as baunit_rfpp_uab_areareserva
	from ladm.rfpp_ue_sustraccion uez,
	ladm.rfpp_uab_sustraccion uabz
	where uez.local_id=uabz.local_id
	union  
	select null as ue_rfpp_ue_zonificacion,
	null as ue_rfpp_ue_compensacion,
	null as ue_rfpp_ue_sustraccion,
	uez.t_id as ue_rfpp_ue_areareserva,
	null as baunit_rfpp_uab_zonificacion,
	null as baunit_rfpp_uab_compensacion,
	null as baunit_rfpp_uab_sustraccion,
	uabz.t_id as baunit_rfpp_uab_areareserva
	from ladm.rfpp_ue_areareserva uez,
	ladm.rfpp_uab_areareserva uabz
	where uez.local_id=uabz.local_id
) t;

    """

    return sql_script


