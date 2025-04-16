def estructura_intermedia():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/***********************************************************************************
             Creación de estructura de datos intermedia 
        	Migración de Áreas Protegidas SINAP  al modelo LADM_COL-AP
              ----------------------------------------------------------
        begin           : 2024-10-21
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)
        email           : contacto@ceicol.com
***********************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 ***************************************************************************/

/*====================================================
  Creación de extensiones necesarias
====================================================*/
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

/*====================================================
  Inserción del sistema origen único nacional
====================================================*/
INSERT INTO spatial_ref_sys (
  srid, auth_name, auth_srid, proj4text, srtext
)
VALUES (
  9377,
  'EPSG',
  9377,
  '+proj=tmerc +lat_0=4.0 +lon_0=-73.0 +k=0.9992 +x_0=5000000 +y_0=2000000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs',
  'PROJCRS["MAGNA-SIRGAS / Origen-Nacional", BASEGEOGCRS["MAGNA-SIRGAS", DATUM["Marco Geocentrico Nacional de Referencia", ELLIPSOID["GRS 1980",6378137,298.257222101, LENGTHUNIT["metre",1]]], PRIMEM["Greenwich",0, ANGLEUNIT["degree",0.0174532925199433]], ID["EPSG",4686]], CONVERSION["Colombia Transverse Mercator", METHOD["Transverse Mercator", ID["EPSG",9807]], PARAMETER["Latitude of natural origin",4, ANGLEUNIT["degree",0.0174532925199433], ID["EPSG",8801]], PARAMETER["Longitude of natural origin",-73, ANGLEUNIT["degree",0.0174532925199433], ID["EPSG",8802]], PARAMETER["Scale factor at natural origin",0.9992, SCALEUNIT["unity",1], ID["EPSG",8805]], PARAMETER["False easting",5000000, LENGTHUNIT["metre",1], ID["EPSG",8806]], PARAMETER["False northing",2000000, LENGTHUNIT["metre",1], ID["EPSG",8807]]], CS[Cartesian,2], AXIS["northing (N)",north, ORDER[1], LENGTHUNIT["metre",1]], AXIS["easting (E)",east, ORDER[2], LENGTHUNIT["metre",1]], USAGE[ SCOPE["unknown"], AREA["Colombia"], BBOX[-4.23,-84.77,15.51,-66.87]], ID["EPSG",9377]]'
) ON CONFLICT (srid) DO NOTHING;

/*========================================
  Creación de esquema
========================================*/
CREATE SCHEMA IF NOT EXISTS estructura_intermedia;
SET search_path TO estructura_intermedia, public;

/*=============================================
  Área Protegida AP
=============================================*/
DROP TABLE IF EXISTS ap_uab_areaprotegida;

CREATE TABLE ap_uab_areaprotegida (
	uab_identificador                    varchar(255) NOT NULL,
	uab_id_ap                            varchar(255) NOT NULL,
	uab_nombre_area_protegida            varchar(255) NULL,
	uab_categoria                        varchar(255) NULL,
	uab_ambito_gestion                   varchar(255) NULL,
	uab_estado                           varchar(255) NULL,
	uab_fecha_inscripcion_runap         varchar(255) NULL,
	uab__fecha_registro_runap           date NULL,
	uab_area_total_ha                   numeric(15,6) NULL,
	uab_area_terrestre_ha               numeric(15,6) NULL,
	uab_area_maritima_ha                numeric(15,6) NULL,
	nombre                               varchar(255) NOT NULL,
	interesado_nombre                    varchar(255) NULL,
	interesado_tipo_interesado           varchar(255) NOT NULL,
	interesado_tipo_documento            varchar(255) NULL,
	interesado_numero_documento          varchar(255) NULL,
	fuente_administrativa_tipo           varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL,
	fuente_administrativa_tipo_formato   varchar(255) NULL,
	fuente_administrativa_fecha_documento_fuente date NULL,
	fuente_administrativa_nombre         varchar(255) NULL,
	ddr_tipo_resposabilidad              varchar(255) NULL,
	ddr_tipo_derecho                     varchar(255) NULL
);

/*=============================================
  UE Área Protegida 
=============================================*/
DROP TABLE IF EXISTS ap_ue_areaprotegida;

CREATE TABLE ap_ue_areaprotegida (
	id_AP                varchar(255) null,
	ue_geometria         public.geometry(multipolygonz, 9377) NULL,
	ue_area_total_ha     varchar(255) NULL,
	ue_area_terrestre_ha varchar(255) NULL,
	ue_area_maritima_ha  varchar(255) NULL

);

/*=============================================
  Fuente Administrativa AP
=============================================*/
DROP TABLE IF EXISTS ap_fuenteadministrativa;

CREATE TABLE ap_fuenteadministrativa (
	uab_identificador                      varchar(15) NOT NULL,
	fuente_administrativa_fecha_documento_fuente date NULL,
	fuente_administrativa_tipo_formato     varchar(255) NULL,
	fuente_administrativa_numero           bigint NULL,
	fuente_administrativa_anio             bigint NULL
);

/*=============================================
  Función de homologación a texto
=============================================*/
DROP FUNCTION IF EXISTS homologar_texto;

CREATE OR REPLACE FUNCTION homologar_texto(texto_input TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN UPPER(
        regexp_replace(
            translate(trim(texto_input), 'áéíóúÁÉÍÓÚ', 'aeiouAEIOU'), 
            '\s+', '', 
            'g'
        )
    );
END;
$$ LANGUAGE plpgsql;

/*=============================================
  Función de homologación a número
=============================================*/
DROP FUNCTION IF EXISTS estructura_intermedia.homologar_numero;

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
                    Migración de Áreas protegidas SINAP  al modelo LADM_COL-AP
              ----------------------------------------------------------
        begin           : 2024-10-21 
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)
        email           : contacto@ceicol.com
***********************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 ***************************************************************************/

/*========================================
  Fijar esquema
========================================*/
set search_path to 
    estructura_intermedia, -- Esquema de estructura de datos intermedia
    public;


/*========================================
  Área Protegida
========================================*/
INSERT INTO estructura_intermedia.ap_uab_areaprotegida (
    uab_identificador, 
    uab_id_ap,
    uab_nombre_area_protegida,
    uab_categoria,
    uab_ambito_gestion,
    uab_estado,
    uab_fecha_inscripcion_runap,
    uab__fecha_registro_runap,
    uab_area_total_ha,
    uab_area_terrestre_ha,
    uab_area_maritima_ha,
    nombre, 
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
    ('AP_' || row_number() OVER ())::varchar(255) AS uab_identificador, 
    ap.ap_id AS uab_id_ap,
    ap.ap_nombre AS uab_nombre_area_protegida,
    CASE
        WHEN ap.ap_categor = 'Reserva Natural de la Sociedad Civil' THEN 'Privado.Reserva_Natural_Sociedad_Civil'
        WHEN ap.ap_categor = 'Parque Nacional Natural' THEN 'Publico.Sistema_Parques_Nacionales_Naturales.Parque_Nacional'
        WHEN ap.ap_categor = 'Parques Naturales Regionales' THEN 'Publico.Parque_Natural_Regional'
        WHEN ap.ap_categor = 'Vía Parque' THEN 'Publico.Sistema_Parques_Nacionales_Naturales.Via_Parque'
        WHEN ap.ap_categor = 'Área Natural Única' THEN 'Publico.Sistema_Parques_Nacionales_Naturales.Area_Natural_Unica'
        WHEN ap.ap_categor = 'Distritos de Conservación de Suelos' THEN 'Publico.Distrito_Conservacion_Suelos'
        WHEN ap.ap_categor IN ('Distritos Nacionales de Manejo Integrado', 'Distritos Regionales de Manejo Integrado') THEN 'Publico.Distrito_Manejo_Integrado'
        WHEN ap.ap_categor = 'Áreas de Recreación' THEN 'Publico.Area_Recreacion'
        WHEN ap.ap_categor = 'Reserva Natural' THEN 'Publico.Sistema_Parques_Nacionales_Naturales.Reserva_Natural'
        WHEN ap.ap_categor IN ('Reservas Forestales Protectoras Nacionales', 'Reservas Forestales Protectoras Regionales') THEN 'Publico.Reserva_Forestal_Protectora'
        WHEN ap.ap_categor = 'Santuario de Flora' THEN 'Publico.Sistema_Parques_Nacionales_Naturales.Santuario_Flora'
        WHEN ap.ap_categor = 'Santuario de Fauna' THEN 'Publico.Sistema_Parques_Nacionales_Naturales.Santuario_Fauna'
        WHEN ap.ap_categor = 'Santuario de Fauna y Flora' THEN 'Publico.Sistema_Parques_Nacionales_Naturales.Santuario_Flora_y_Fauna'
        ELSE NULL
    END AS uab_categoria,
    CASE 
        WHEN info."Ámbito de gestión" ILIKE '%privada%' THEN 'Local' 
        WHEN info."Ámbito de gestión" ILIKE '%protegidas nacionales%' THEN 'Nacional' 
        WHEN info."Ámbito de gestión" ILIKE '%protegidas regional%' THEN 'Regional' 
        ELSE NULL 
    END AS uab_ambito_gestion,
CASE
    /* 1. Casos explícitos de “inscrita” (o vacío / NULL)           */
    WHEN LOWER(TRIM(COALESCE(ap.condicion, ''))) = 'inscrita' 
         OR TRIM(COALESCE(ap.condicion, '')) = ''               -- vacío
    THEN 'Inscrita'

    /* 2. Caso explícito de “registrada”                           */
    WHEN LOWER(TRIM(ap.condicion)) = 'registrada'
    THEN 'Registrada'

    /* 3. Cualquier otro valor en “condicion”:
          ─ Si fecha_regi es NULL o el string 'NaT'  → Inscrita
          ─ De lo contrario                         → Registrada */
    WHEN ap.fecha_regi IS NULL
         OR ap.fecha_regi::text ILIKE 'nat'        -- por si llegó como texto
    THEN 'Inscrita'
    
    ELSE 'Registrada'
END AS uab_estado,

    NULLIF(ap.fecha_insc, 'NaT')::date AS uab_fecha_inscripcion_runap,
    NULLIF(ap.fecha_regi, 'NaT')::date AS uab__fecha_registro_runap,
    COALESCE(ap.area_ha_to, 0) AS uab_area_total_ha,
    COALESCE(ap.area_ha_te, 0) AS uab_area_terrestre_ha,
    COALESCE(ap.area_ha_ma, 0) AS uab_area_maritima_ha,
    'ap_uab_areaprotegida'::varchar(255) AS nombre,
    INITCAP(ap.organizaci) AS interesado_nombre,
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
    'NIT'::varchar(255) AS interesado_tipo_documento, 
    replace(to_char(to_number(ap.nit, '999999999999'), '9G999G999G999'), ',', '.') AS interesado_numero_documento,
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad,
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato,
    NULLIF(info."Fecha del acto", 'NaT')::date AS fuente_administrativa_fecha_documento_fuente,
    CONCAT(
        info."Tipo de acto administrativo", ' ',
        info."Número del acto", ' de ',
        to_char(NULLIF(info."Fecha del acto", 'NaT')::date, 'YYYY')
    ) AS fuente_administrativa_nombre,
    'Administrar'::varchar(255) AS ddr_tipo_resposabilidad,
    CASE 
        WHEN ap.ap_categor = 'Reserva Natural de la Sociedad Civil' THEN 'Destinar'
        ELSE 'Declarar'
    END AS ddr_tipo_derecho
FROM 
    insumos.area_protegida ap
INNER JOIN 
    insumos.informacion_runap_excel info
    ON ap.ap_id = info."Id del área protegida";

/*========================================
 Unidad Espacial
========================================*/
INSERT INTO estructura_intermedia.ap_ue_areaprotegida (
	id_ap,
	ue_geometria,
	ue_area_total_ha,
	ue_area_terrestre_ha,
	ue_area_maritima_ha
)
SELECT
	ap.ap_id AS id_ap,
	ST_Force3D(ST_Transform(ap.geom, 9377)) AS ue_geometria,
	COALESCE(ap.area_ha__1::varchar, '0') AS ue_area_total_ha,
	COALESCE(ap.area_ha__3::varchar, '0') AS ue_area_terrestre_ha,
	COALESCE(ap.area_ha__2::varchar, '0') AS ue_area_maritima_ha
FROM insumos.area_protegida ap
INNER JOIN insumos.informacion_runap_excel info
	ON ap.ap_id = info."Id del área protegida";

/*========================================
  Fuentes Administrativas de Área protegida
========================================*/
TRUNCATE TABLE estructura_intermedia.ap_fuenteadministrativa;

INSERT INTO estructura_intermedia.ap_fuenteadministrativa (
    uab_identificador, 
    fuente_administrativa_fecha_documento_fuente, 
    fuente_administrativa_tipo_formato, 
    fuente_administrativa_numero, 
    fuente_administrativa_anio
)
SELECT 
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,

    -- Homologación del tipo de acto
    CASE 
        WHEN fuente_administrativa_nombre ILIKE 'acuerdo%' THEN 'Acuerdo'
        WHEN fuente_administrativa_nombre ILIKE 'acuerdos%' THEN 'Acuerdo'
        WHEN fuente_administrativa_nombre ILIKE 'resolución%' THEN 'Resolucion'
        WHEN fuente_administrativa_nombre ILIKE 'decreto%' THEN 'Decreto'
        ELSE split_part(fuente_administrativa_nombre, ' ', 1)
    END AS fuente_administrativa_tipo_formato,

    -- Captura número del acto desde Directivo, Directiva, Conjunta o tipo de acto (singular y plural)
    CASE 
        WHEN fuente_administrativa_nombre ~* '(Directivo|Directiva|Conjunta)\s\d+' THEN
            substring(fuente_administrativa_nombre FROM '(?<=Directivo\s|Directiva\s|Conjunta\s)\d+')::bigint
        WHEN fuente_administrativa_nombre ~* '(Acuerdo|Acuerdos|Resolución|Decreto)\s\d+' THEN
            substring(fuente_administrativa_nombre FROM '(?<=Acuerdo\s|Acuerdos\s|Resolución\s|Decreto\s)\d+')::bigint
        ELSE NULL
    END AS fuente_administrativa_numero,

    -- Año (después de "de")
    CASE 
        WHEN fuente_administrativa_nombre ~* 'de\s+\d{4}' THEN
            substring(fuente_administrativa_nombre FROM 'de\s+(\d{4})')::bigint
        ELSE NULL
    END AS fuente_administrativa_anio

FROM estructura_intermedia.ap_uab_areaprotegida;


    """

    return sql_script



def validar_estructura():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/**********************************************************************************
            ETL de Validación de insumos a estructura de datos intermedia
        		Migración de Áreas protegidas SINAP  al modelo LADM_COL-AP
              ----------------------------------------------------------
        begin           : 2024-10-21 
        git sha         : :%H$
        copyright       : (C) 2024 by Leo Cardona (CEICOL SAS)
                          (C) 2024 by Cesar Alfonso Basurto (CEICOL SAS)
        email           : contacto@ceicol.com
***********************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License v3.0 as          *
 *   published by the Free Software Foundation.                            *
 *                                                                         *
 **********************************************************************************/

--========================================
-- Fijar esquema
--========================================
SET search_path TO 
	estructura_intermedia, -- Esquema de estructura de datos intermedia
	public;


--========================================
-- Área Protegida
--========================================

SELECT *
FROM estructura_intermedia.ap_uab_areaprotegida;

SELECT count(*)
FROM estructura_intermedia.ap_uab_areaprotegida;

-- Validación de interesados
SELECT 
    uab_nombre_area_protegida,
    interesado_nombre,
    COUNT(*) AS total
FROM estructura_intermedia.ap_uab_areaprotegida
GROUP BY uab_nombre_area_protegida, interesado_nombre
ORDER BY uab_nombre_area_protegida;


--========================================
-- Fuentes Administrativas
--========================================

SELECT *
FROM estructura_intermedia.ap_fuenteadministrativa;

SELECT COUNT(*)
FROM estructura_intermedia.ap_fuenteadministrativa;

-- Verificación por tipo de acto
SELECT fuente_administrativa_tipo_formato, COUNT(*)
FROM estructura_intermedia.ap_fuenteadministrativa
GROUP BY fuente_administrativa_tipo_formato
ORDER BY fuente_administrativa_tipo_formato;

-- Validación de actos por año
SELECT fuente_administrativa_anio, COUNT(*)
FROM estructura_intermedia.ap_fuenteadministrativa
GROUP BY fuente_administrativa_anio
ORDER BY fuente_administrativa_anio;

-- Número de actos por tipo y año
SELECT fuente_administrativa_tipo_formato, fuente_administrativa_anio, COUNT(*)
FROM estructura_intermedia.ap_fuenteadministrativa
GROUP BY fuente_administrativa_tipo_formato, fuente_administrativa_anio
ORDER BY fuente_administrativa_tipo_formato, fuente_administrativa_anio;

-- Validar si hay números de acto administrativos que exceden el rango de int4
SELECT uab_identificador, fuente_administrativa_numero
FROM estructura_intermedia.ap_fuenteadministrativa
WHERE fuente_administrativa_numero > 10000;
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

/* SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'etl_ap' AND pid <> pg_backend_pid();
DROP SCHEMA ladm CASCADE;*/
 
--========================================
-- Fijar esquema
--========================================
set search_path to
	estructura_intermedia,	-- Esquema de estructura de datos intermedia
	ladm,		-- Esquema modelo LADM-LEY2
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
	'LADM_COL_v_1_0_0_Ext_AP.AP',
	uuid_generate_v4(),
	'ETL de importación de datos',
	NULL );

--================================================================================
-- 2. Migración de AP_interesado
--================================================================================
truncate table ladm.ap_interesado cascade;
INSERT INTO ladm.ap_interesado (
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
	(SELECT t_id FROM ladm.t_ili2db_basket 
	 WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_AP.AP' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	NULL AS observacion,
	interesado_nombre,
	(SELECT t_id FROM ladm.col_interesadotipo 
	 WHERE ilicode = interesado_tipo_interesado) AS tipo_interesado,
	(SELECT t_id FROM ladm.col_documentotipo 
	 WHERE ilicode = interesado_tipo_documento) AS tipo_documento,
	interesado_numero_documento,
	NOW() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'ap_interesado' AS espacio_de_nombres, 
	ROW_NUMBER() OVER (ORDER BY interesado_nombre) AS local_id
FROM (
	SELECT DISTINCT
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	FROM estructura_intermedia.ap_uab_areaprotegida
	WHERE interesado_nombre IS NOT NULL
) t;


--================================================================================
-- 3. Migración de Area de Protegida
--================================================================================
-- 3.1 Diligenciamiento de la tabla ap_uab_areareserva
truncate table ladm.ap_uab_areaprotegida cascade;
INSERT INTO ladm.ap_uab_areaprotegida (
	t_basket, 
	t_ili_tid, 
	id_ap,
	nombre_area_protegida,
	categoria,
	ambito_gestion,
	estado,
	fecha_inscripcion_runap,
	fecha_registro_runap,
	area_total_ha,
	area_terrestre_ha,
	area_maritima_ha,
	nombre,
	tipo,
	comienzo_vida_util_version,
	fin_vida_util_version,
	espacio_de_nombres,
	local_id
)
SELECT
	(SELECT t_id FROM ladm.t_ili2db_basket 
	 WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_AP.AP' LIMIT 1) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	uab_id_ap::bigint AS id_ap,
	uab_nombre_area_protegida,
	(SELECT t_id FROM ladm.ap_categoriaareaprotegidatipo 
	 WHERE ilicode = uab_categoria) AS categoria,
	(SELECT t_id FROM ladm.ap_ambitogestiontipo 
	 WHERE ilicode = uab_ambito_gestion) AS ambito_gestion,
	(SELECT t_id FROM ladm.ap_estadoareaprotegidatipo 
	 WHERE ilicode = uab_estado) AS estado,
	CASE 
	    WHEN uab_fecha_inscripcion_runap ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
	    THEN uab_fecha_inscripcion_runap::date 
	    ELSE NULL 
	END AS fecha_inscripcion_runap,
	uab__fecha_registro_runap,
	uab_area_total_ha,
	uab_area_terrestre_ha,
	uab_area_maritima_ha,
	nombre,
	(SELECT t_id FROM ladm.col_unidadadministrativabasicatipo 
	 WHERE ilicode = 'Ambiente_Desarrollo_Sostenible') AS tipo,
	now() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'ap_uab_areaprotegida' AS espacio_de_nombres, 
	uab_identificador AS local_id
FROM estructura_intermedia.ap_uab_areaprotegida;


-- 3.2 Diligenciamiento de la tabla ap_ue_areareserva

INSERT INTO ladm.ap_ue_areaprotegida (
	t_basket, 
	t_ili_tid, 
	area_total_ha,
	area_terrestre_ha,
	area_maritima_ha,
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
SELECT
	(
		SELECT t_id 
		FROM ladm.t_ili2db_basket 
		WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_AP.AP' 
		LIMIT 1
	) AS t_basket,
	uuid_generate_v4() AS t_ili_tid,
	COALESCE(e.ue_area_total_ha::numeric, 0)::numeric(15,6) AS area_total_ha,
	COALESCE(e.ue_area_terrestre_ha::numeric, 0)::numeric(15,6) AS area_terrestre_ha,
	COALESCE(e.ue_area_maritima_ha::numeric, 0)::numeric(15,6) AS area_maritima_ha,
	a.nombre_area_protegida AS etiqueta,
	(
		SELECT t_id 
		FROM ladm.col_relacionsuperficietipo 
		WHERE ilicode = 'En_Rasante'
	) AS relacion_superficie,
	e.ue_geometria,	
	NOW() AS comienzo_vida_util_version,
	NULL AS fin_vida_util_version,
	'ap_ue_areaprotegida' AS espacio_de_nombres, 
	e.id_ap AS id_ap
FROM estructura_intermedia.ap_ue_areaprotegida e
LEFT JOIN ladm.ap_uab_areaprotegida a
	ON e.id_ap::int =a.id_ap;

-- 3.3 Diligenciamiento de la tabla ap_derecho para ap_uab_areareserva
INSERT INTO ladm.ap_derecho(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_ap_interesado, 
	interesado_ap_agrupacioninteresados, 
	unidad, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)	
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_AP.AP' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.ap_derechotipo 
	  where ilicode like ddr_tipo_derecho ) as tipo,
	null as descripcion,
	( select t_id from ladm.ap_interesado 
	  where nombre like interesado_nombre ) as interesado_ap_interesado,
	 null as interesado_rl2_agrupacioninteresados, 
	( select t_id from ladm.ap_uab_areaprotegida
	  where local_id like uab_identificador ) as unidad_ap_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'ap_derecho' as espacio_de_nombres, 
	uab_identificador as local_id
from  estructura_intermedia.ap_uab_areaprotegida; 
-- 4.3 Diligenciamiento de la tabla rl2_responsabilidad para rl2_uab_zonificacion
INSERT INTO ladm.ap_responsabilidad(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_ap_interesado, 
	interesado_ap_agrupacioninteresados, 
	unidad,
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_AP.AP' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.ap_responsabilidadtipo  
	  where ilicode like ddr_tipo_resposabilidad ) as tipo,
	null as descripcion,
	( select t_id from ladm.ap_interesado 
	  where nombre like interesado_nombre ) as interesado_ap_interesado, 
	null as interesado_ap_agrupacioninteresados, 
	( select t_id from ladm.ap_uab_areaprotegida
	  where local_id like uab_identificador ) as unidad,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'ap_responsabilidad' as espacio_de_nombres, 
	uab_identificador local_id
from estructura_intermedia.ap_uab_areaprotegida ; 

-- 3.4 Diligenciamiento de la tabla ap_fuenteadministrativa para ap_uab_areareserva

INSERT INTO ladm.ap_fuenteadministrativa(
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
		(select t_id from ladm.t_ili2db_basket where topic like 'LADM_COL_v_1_0_0_Ext_AP.AP' limit 1) as t_basket,
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
from estructura_intermedia.ap_fuenteadministrativa f,estructura_intermedia.ap_uab_areaprotegida a
where f.uab_identificador =a.uab_identificador;



--================================================================================
-- 7. Migración de col_rrrfuente
--================================================================================
INSERT INTO ladm.col_rrrfuente(
	t_basket, 
	fuente_administrativa, 
	rrr_ap_derecho, 
	rrr_ap_responsabilidad
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_AP.AP' limit 1 ) as t_basket,
	*
from (
	select distinct 
		f.t_id as fuente_administrativa,
		null::int8 as rrr_ap_derecho,
		r.t_id as rrr_ap_responsabilidad
	from ladm.ap_fuenteadministrativa f,
		ladm.ap_responsabilidad r
	where f.local_id = r.local_id 
	union  
	select distinct 
		f.t_id as fuente_administrativa,
		d.t_id as rrr_ap_derecho,
		null::int4 as rrr_ap_responsabilidad
	from ladm.ap_fuenteadministrativa f,
		ladm.ap_derecho d
	where f.local_id = d.local_id
) t;

--================================================================================
-- 8. Migración de col_uebaunit
--================================================================================
INSERT INTO ladm.col_uebaunit (
	t_id,
	t_basket,
	ue,
	baunit
)
SELECT
	nextval('t_ili2db_seq') AS t_id,
	(
		SELECT t_id 
		FROM ladm.t_ili2db_basket 
		WHERE topic LIKE 'LADM_COL_v_1_0_0_Ext_AP.AP' 
		LIMIT 1
	) AS t_basket,
	uez.t_id::int8 AS ue,
	uabz.t_id::int8 AS baunit
FROM ladm.ap_ue_areaprotegida uez
JOIN ladm.ap_uab_areaprotegida uabz 
	ON uez.local_id::int = uabz.id_ap;

    """

    return sql_script


