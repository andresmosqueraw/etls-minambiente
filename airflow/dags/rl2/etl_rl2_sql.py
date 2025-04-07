def estructura_intermedia():
    """
    Guarda el script SQL de la estructura de datos intermedia
    en un archivo llamado 'estructura_intermedia.sql'.
    """
    sql_script = """    
/***********************************************************************************
             Creación de estructura de datos intermedia 
        	Migración del Ley 2  al modelo LADM_COL-LEY2
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
  Creación de la extensión postgis
====================================================*/
CREATE EXTENSION IF NOT EXISTS postgis;

/*====================================================
  Creación de la extensión uuid
====================================================*/
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

/*====================================================
  Inserción del sistema origen unico nacional
====================================================*/
INSERT INTO spatial_ref_sys (
  srid, auth_name, auth_srid, proj4text, srtext
)
VALUES
  (
    9377,
    'EPSG',
    9377,
    '+proj=tmerc +lat_0=4.0 +lon_0=-73.0 +k=0.9992 +x_0=5000000 +y_0=2000000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs ',
    'PROJCRS["MAGNA-SIRGAS / Origen-Nacional", BASEGEOGCRS["MAGNA-SIRGAS", DATUM["Marco Geocentrico Nacional de Referencia", ELLIPSOID["GRS 1980",6378137,298.257222101, LENGTHUNIT["metre",1]]], PRIMEM["Greenwich",0, ANGLEUNIT["degree",0.0174532925199433]], ID["EPSG",4686]], CONVERSION["Colombia Transverse Mercator", METHOD["Transverse Mercator", ID["EPSG",9807]], PARAMETER["Latitude of natural origin",4, ANGLEUNIT["degree",0.0174532925199433], ID["EPSG",8801]], PARAMETER["Longitude of natural origin",-73, ANGLEUNIT["degree",0.0174532925199433], ID["EPSG",8802]], PARAMETER["Scale factor at natural origin",0.9992, SCALEUNIT["unity",1], ID["EPSG",8805]], PARAMETER["False easting",5000000, LENGTHUNIT["metre",1], ID["EPSG",8806]], PARAMETER["False northing",2000000, LENGTHUNIT["metre",1], ID["EPSG",8807]]], CS[Cartesian,2], AXIS["northing (N)",north, ORDER[1], LENGTHUNIT["metre",1]], AXIS["easting (E)",east, ORDER[2], LENGTHUNIT["metre",1]], USAGE[ SCOPE["unknown"], AREA["Colombia"], BBOX[-4.23,-84.77,15.51,-66.87]], ID["EPSG",9377]]'
  ) ON CONFLICT (srid) DO NOTHING;

/*========================================
  Creación de esquema
========================================*/
CREATE SCHEMA IF NOT EXISTS estructura_intermedia;

/*========================================
  Fijar esquema
========================================*/
SET search_path TO 
	estructura_intermedia,  -- Esquema de estructura de datos intermedia
	public;


/*=============================================
  Área de reserva Ley 2
=============================================*/
DROP TABLE IF EXISTS rl2_uab_areareserva;

CREATE TABLE rl2_uab_areareserva (
	uab_identificador              varchar(7) NOT NULL,   -- Identificador interno para el área de reserva de ley 2ª
	uab_nombre_reserva             varchar(150) NOT NULL, -- Nombre de la reserva según la Ley 2ª de 1959
	uab_nombre                     varchar(255) NULL,     -- Nombre de la unidad administrativa
	ue_geometria                   public.geometry(multipolygonz, 9377) NULL,  -- Geometría de la reserva
	-- Se elimina la columna interesado_tipo
	interesado_nombre              varchar(255) NULL,     -- Nombre del interesado
	interesado_tipo_interesado     varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento      varchar(255) NULL,     -- Tipo de documento del interesado
	interesado_numero_documento    varchar(255) NULL,     -- Número de documento del interesado
	fuente_administrativa_tipo     varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL,
	fuente_administrativa_tipo_formato varchar(255) NULL,
	fuente_administrativa_fecha_documento_fuente date NULL,
	fuente_administrativa_nombre   varchar(255) NULL,
	ddr_tipo_resposabilidad        varchar(255) NULL,
	ddr_tipo_derecho               varchar(255) NULL
);


/*=============================================
  Zonificación Ley 2
=============================================*/
DROP TABLE IF EXISTS rl2_uab_zonificacion;

CREATE TABLE rl2_uab_zonificacion (
	uab_identificador              varchar(7) NOT NULL,  -- Identificador interno para la reserva Ley 2ª
	uab_tipo_zona                  varchar(255) NOT NULL, -- Tipo de zonificación
	uab_areareserva                varchar(255) NOT NULL,
	uab_nombre                     varchar(255) NULL,     -- Nombre de la unidad administrativa
	ue_geometria                   public.geometry(multipolygonz, 9377) NULL,
	-- Se elimina la columna interesado_tipo
	interesado_nombre              varchar(255) NULL,     -- Nombre del interesado
	interesado_tipo_interesado     varchar(255) NOT NULL, -- Tipo de interesado
	interesado_tipo_documento      varchar(255) NULL,
	interesado_numero_documento    varchar(255) NULL,
	fuente_administrativa_tipo     varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL,
	fuente_administrativa_tipo_formato varchar(255) NULL,
	fuente_administrativa_fecha_documento_fuente date NULL,
	fuente_administrativa_nombre   varchar(255) NULL,
	ddr_tipo_resposabilidad        varchar(255) NULL,
	ddr_tipo_derecho               varchar(255) NULL
);


/*=============================================
  Compensación Ley 2
=============================================*/
DROP TABLE IF EXISTS rl2_uab_compensacion;

CREATE TABLE rl2_uab_compensacion (
	uab_identificador              varchar(15) NOT NULL, -- Identificador interno de la compensación
	uab_expediente                 varchar(100) NOT NULL, -- Número de expediente de compensación
	uab_observaciones              text NULL,
	uab_areareserva                varchar(255) NOT NULL,
	uab_sustraccion                varchar(255) NULL,
	uab_nombre                     varchar(255) NULL,  -- Nombre de la unidad administrativa
	ue_geometria                   public.geometry(multipolygonz, 9377) NULL,
	-- Se elimina la columna interesado_tipo
	interesado_nombre              varchar(255) NULL,   -- Nombre del interesado
	interesado_tipo_interesado     varchar(255) NOT NULL,
	interesado_tipo_documento      varchar(255) NULL,
	interesado_numero_documento    varchar(255) NULL,
	fuente_administrativa_tipo     varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL,
	fuente_administrativa_tipo_formato varchar(255) NULL,
	fuente_administrativa_fecha_documento_fuente date NULL,
	fuente_administrativa_nombre   varchar(255) NULL,
	ddr_tipo_resposabilidad        varchar(255) NULL,
	ddr_tipo_derecho               varchar(255) NULL
);


/*=============================================
  Sustracción Ley 2
=============================================*/
DROP TABLE IF EXISTS rl2_uab_sustraccion;

CREATE TABLE rl2_uab_sustraccion (
	uab_identificador              varchar(15) NOT NULL, -- Identificador interno de la sustracción
	uab_expediente                 varchar(100) NOT NULL,
	uab_tipo_sustraccion           varchar(255) NOT NULL, -- Tipo de sustracción realizado a la Reserva Ley 2ª
	uab_tipo_causal                varchar(255) NULL,     -- Causal de la sustracción
	uab_sustrajo                   varchar(255) NOT NULL, -- Sigla de la entidad que aprobó la sustracción
	uab_detalle_sustrajo           varchar(255) NULL,
	uab_fin_sustraccion            date NULL,           -- Fecha de finalización (para sustracciones temporales)
	uab_tipo_sector                varchar(255) NOT NULL, -- Sector solicitante
	uab_detalle_sector             varchar(255) NULL,     -- Detalle cuando el sector es "Otro"
	uab_solicitante                varchar(255) NULL,     -- Campo adicional para solicitante
	uab_observaciones              text NULL,
	uab_areareserva                varchar(255) NULL,
	uab_nombre_areareserva         varchar(255) NULL,
	uab_compensacion               varchar(255) NULL,	
	uab_nombre                     varchar(255) NULL,     -- Nombre de la unidad administrativa
	ue_geometria                   public.geometry(multipolygonz, 9377) NULL,
	-- Se elimina la columna interesado_tipo
	interesado_nombre              varchar(255) NULL,     -- Nombre del interesado
	interesado_tipo_interesado     varchar(255) NOT NULL,
	interesado_tipo_documento      varchar(255) NULL,
	interesado_numero_documento    varchar(255) NULL,
	fuente_administrativa_tipo     varchar(255) NOT NULL,
	fuente_administrativa_estado_disponibilidad varchar(255) NOT NULL,
	fuente_administrativa_tipo_formato varchar(255) NULL,
	fuente_administrativa_fecha_documento_fuente date NULL,
	fuente_administrativa_nombre   varchar(255) NULL,
	ddr_tipo_resposabilidad        varchar(255) NULL,
	ddr_tipo_derecho               varchar(255) NULL
);


/*=============================================
  Fuente Administrativa Ley 2
=============================================*/
DROP TABLE IF EXISTS rl2_fuenteadministrativa;

CREATE TABLE rl2_fuenteadministrativa (
	uab_identificador                  varchar(15) NOT NULL, -- Identificador de la compensación o sustracción
	fuente_administrativa_fecha_documento_fuente date NULL,
	fuente_administrativa_tipo_formato varchar(255) NULL,
	fuente_administrativa_numero     int4 NULL,
	fuente_administrativa_anio       int4 NULL
);


/*-----------------------------------------------
  Funciones de homologación Ley 2
-----------------------------------------------*/

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
                    Migración del Ley 2  al modelo LADM_COL-LEY2
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
  Área de reserva Ley 2
========================================*/
INSERT INTO estructura_intermedia.rl2_uab_areareserva(
    uab_identificador, 
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
SELECT 
    upper(id) AS uab_identificador, 
    nom_ley2 AS uab_nombre_reserva, 
    'rl2_uab_areareserva'::varchar(255) AS uab_nombre, 
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
    'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre, 
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
    'NIT'::varchar(255) AS interesado_tipo_documento, 
    '830.115.395-1'::varchar(255) AS interesado_numero_documento, 
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
    to_date('1959-01-17','YYYY-MM-DD') AS fuente_administrativa_fecha_documento_fuente, 
    'Ley 2 de 1959'::varchar(255) AS fuente_administrativa_nombre,
    null AS ddr_tipo_resposabilidad,
    'Realinderar'::varchar(255) AS ddr_tipo_derecho
FROM insumos.area_reserva_ley2;


/*========================================
  Zonificación Ley 2
========================================*/
INSERT INTO estructura_intermedia.rl2_uab_zonificacion(
    uab_identificador, 
    uab_tipo_zona, 
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
SELECT 
    ('ZNL2_' || row_number() OVER ())::varchar(255) AS uab_identificador,
    case
        when homologar_texto(replace(tipo_zoni,' ','_')) = homologar_texto('Tipo_A') then 'Tipo_A'
        when homologar_texto(replace(tipo_zoni,' ','_')) = homologar_texto('Tipo_B') then 'Tipo_B'
        when homologar_texto(replace(tipo_zoni,' ','_')) = homologar_texto('Tipo_C') then 'Tipo_C'
        when homologar_texto(replace(tipo_zoni,' ','_')) = homologar_texto('AREAS_CON_PREVIA_DECISION_DE_ORDENAMIENTO') then 'Area_Previa_Decision_Ordenamiento'
        else 'No aplica'
    end AS uab_tipo_zona,
    upper(id_ley2)::varchar(255) AS uab_areareserva,
    'rl2_uab_zonificacion' AS uab_nombre,
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
    'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre, 
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
    'NIT'::varchar(255) AS interesado_tipo_documento, 
    '830.115.395-1'::varchar(255) AS interesado_numero_documento, 
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,	       
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
    null AS fuente_administrativa_fecha_documento_fuente,
    soporte::varchar(255) AS fuente_administrativa_nombre,
    'Zonificar'::varchar(255) AS ddr_tipo_resposabilidad,
    null AS ddr_tipo_derecho
FROM insumos.zonificacion_ley2;


/*========================================
  Compensación Ley 2
========================================*/
INSERT INTO estructura_intermedia.rl2_uab_compensacion(
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
    ('CML2_' || row_number() OVER ())::varchar(255) AS uab_identificador,
    expediente AS uab_expediente,
    proyecto AS uab_observaciones,
    upper(id_reserva)::varchar(255) AS uab_areareserva,
    null as uab_sustraccion,
    'rl2_uab_compensacion' AS uab_nombre,
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
    'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre, 
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado, 
    'NIT'::varchar(255) AS interesado_tipo_documento, 
    '830.115.395-1'::varchar(255) AS interesado_numero_documento, 
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad, 
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato, 
    fecha_acto AS fuente_administrativa_fecha_documento_fuente, 
    acto_admin::varchar(255) AS fuente_administrativa_nombre,
    'Compensar'::varchar(255) AS ddr_tipo_resposabilidad,
    null AS ddr_tipo_resposabilidad
FROM insumos.compensacion_ley2
WHERE left(id_reserva, 4) = 'LEY2';


/*========================================
  Sustracción Ley 2
========================================*/
INSERT INTO rl2_uab_sustraccion (
    uab_identificador,
    uab_expediente,
    uab_tipo_sustraccion,
    uab_tipo_causal,
    uab_sustrajo,
    uab_detalle_sustrajo,
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
WITH sustracciones_definitivas_limpieza AS (
    SELECT 
        ('SDL2_' || row_number() OVER ())::varchar(255) AS uab_identificador,
        CASE
            WHEN homologar_texto(sustrajo) = homologar_texto('INCODER') THEN 'INCODER'
            WHEN homologar_texto(sustrajo) = homologar_texto('INCORA') THEN 'INCORA'
            WHEN homologar_texto(sustrajo) = homologar_texto('INDERENA') THEN 'INDERENA'
            WHEN homologar_texto(sustrajo) = homologar_texto('MADS') THEN 'MADS'
            WHEN homologar_texto(sustrajo) = homologar_texto('MAVDT') THEN 'MAVDT'
            ELSE 'Otro'
        END AS uab_sustrajo,
        CASE
            WHEN sector = 'Actividad Agropecuaria' THEN 'Reforma_Agraria'
            WHEN sector = 'Adjudicación a Colonos' THEN 'Reforma_Agraria'
            WHEN sector = 'Asuntos indígenas' THEN 'Reforma_Agraria'
            WHEN sector = 'Base militar' THEN 'Otro'
            WHEN sector = 'Colonización' THEN 'Reforma_Agraria'
            WHEN sector = 'Constitución Resguardo indígena' THEN 'Otro'
            WHEN sector = 'Creación del Distrito de Conservación de Suelos y Aguas' THEN 'Otro'
            WHEN sector = 'Destinar tierras actividad agropecuaria' THEN 'Reforma_Agraria'
            WHEN sector = 'Distrito de riego y asentamiento' THEN 'Otro'
            WHEN sector = 'Distrito de riego y reasentamiento' THEN 'Otro'
            WHEN sector = 'Distritos de conservacion' THEN 'Otro'
            WHEN sector = 'Fines especiales zona costera' THEN 'Otro'
            WHEN sector = 'Generación Electrica' THEN 'Energia'
            WHEN sector = 'Generación eléctrica' THEN 'Energia'
            WHEN sector = 'Hidrocarburos' THEN 'Hidrocarburos'
            WHEN sector = 'Inciso 2 Artículo 210 Decreto Ley 2811 de 1974' THEN 'Inciso_Segundo'
            WHEN sector = 'Infraestructura' THEN 'Infraestructura_Transporte'
            WHEN sector = 'Infraestructura sanitaria y ambiental' THEN 'Infraestructura_Transporte'
            WHEN sector = 'Infraestructura vial' THEN 'Infraestructura_Transporte'
            WHEN sector = 'Minería' THEN 'Mineria'
            WHEN sector = 'Reasentamiento' THEN 'Otro'
            WHEN sector = 'Registro Áreas Urbanas, Expansion Urbana, Equipamiento' THEN 'Area_Urbana_Expansion_Rural'
            WHEN sector = 'Restitución de tierras' THEN 'Restitucion_Tierras'
            WHEN sector = 'Titulación / colonos' THEN 'Reforma_Agraria'
            WHEN sector = 'Titulacion de baldíos' THEN 'Reforma_Agraria'
            WHEN sector = 'Titulación de baldíos' THEN 'Reforma_Agraria'
            WHEN sector = 'Titulación de baldíos/ Asuntos indigenas' THEN 'Reforma_Agraria'
            WHEN sector = 'Titulación de baldíos/ Conservación' THEN 'Reforma_Agraria'
            WHEN sector = 'Transmisión eléctrica' THEN 'Energia'
            WHEN sector = 'VIS y VIP' THEN 'Vivienda_VIS_VIP'
            ELSE 'Otro'
        END AS uab_tipo_sector,
        sustrajo,
        sector,
        geom,
        observacio,
        solicitant,
        fecha_acto,
        acto_admin,
        id_ley2,
        nom_ley2
    FROM insumos.sustracciones_definitivas_ley2
    WHERE nom_ley2 NOT LIKE '%/%'
),
sustracciones_temporal_limpieza AS (
    SELECT
        id AS uab_identificador,
        CASE
            WHEN homologar_texto(sustrajo) = homologar_texto('INCODER') THEN 'INCODER'
            WHEN homologar_texto(sustrajo) = homologar_texto('INCORA') THEN 'INCORA'
            WHEN homologar_texto(sustrajo) = homologar_texto('INDERENA') THEN 'INDERENA'
            WHEN homologar_texto(sustrajo) = homologar_texto('MADS') THEN 'MADS'
            WHEN homologar_texto(sustrajo) = homologar_texto('MAVDT') THEN 'MAVDT'
            ELSE 'Otro'
        END AS uab_sustrajo,
        CASE
            WHEN sector = 'Actividad Agropecuaria' THEN 'Reforma_Agraria'
            WHEN sector = 'Adjudicación a Colonos' THEN 'Reforma_Agraria'
            WHEN sector = 'Asuntos indígenas' THEN 'Reforma_Agraria'
            WHEN sector = 'Base militar' THEN 'Otro'
            WHEN sector = 'Colonización' THEN 'Reforma_Agraria'
            WHEN sector = 'Constitución Resguardo indígena' THEN 'Otro'
            WHEN sector = 'Creación del Distrito de Conservación de Suelos y Aguas' THEN 'Otro'
            WHEN sector = 'Destinar tierras actividad agropecuaria' THEN 'Reforma_Agraria'
            WHEN sector = 'Distrito de riego y asentamiento' THEN 'Otro'
            WHEN sector = 'Distrito de riego y reasentamiento' THEN 'Otro'
            WHEN sector = 'Distritos de conservacion' THEN 'Otro'
            WHEN sector = 'Fines especiales zona costera' THEN 'Otro'
            WHEN sector = 'Generación Electrica' THEN 'Energia'
            WHEN sector = 'Generación eléctrica' THEN 'Energia'
            WHEN sector = 'Hidrocarburos' THEN 'Hidrocarburos'
            WHEN sector = 'Inciso 2 Artículo 210 Decreto Ley 2811 de 1974' THEN 'Inciso_Segundo'
            WHEN sector = 'Infraestructura' THEN 'Infraestructura_Transporte'
            WHEN sector = 'Infraestructura sanitaria y ambiental' THEN 'Infraestructura_Transporte'
            WHEN sector = 'Infraestructura vial' THEN 'Infraestructura_Transporte'
            WHEN sector = 'Minería' THEN 'Mineria'
            WHEN sector = 'Reasentamiento' THEN 'Otro'
            WHEN sector = 'Registro Áreas Urbanas, Expansion Urbana, Equipamiento' THEN 'Area_Urbana_Expansion_Rural'
            WHEN sector = 'Restitución de tierras' THEN 'Restitucion_Tierras'
            WHEN sector = 'Titulación / colonos' THEN 'Reforma_Agraria'
            WHEN sector = 'Titulacion de baldíos' THEN 'Reforma_Agraria'
            WHEN sector = 'Titulación de baldíos' THEN 'Reforma_Agraria'
            WHEN sector = 'Titulación de baldíos/ Asuntos indigenas' THEN 'Reforma_Agraria'
            WHEN sector = 'Titulación de baldíos/ Conservación' THEN 'Reforma_Agraria'
            WHEN sector = 'Transmisión eléctrica' THEN 'Energia'
            WHEN sector = 'VIS y VIP' THEN 'Vivienda_VIS_VIP'
            ELSE 'Otro'
        END AS uab_tipo_sector,
        CASE
            WHEN id = 'STL2_0080' THEN 'Resolución 0936 de 2020'
            ELSE acto_admin
        END AS fuente_administrativa_nombre,
        sustrajo,
        sector,
        geom,
        observacio,
        solicitant,
        fecha_acto,
        acto_admin,
        id_ley2,
        nom_ley2,
        fecha_fin
    FROM insumos.sustracciones_temporales_ley2
    WHERE nom_ley2 NOT LIKE '%/%'
)
SELECT 
    -- Bloque para sustracciones temporales
    uab_identificador::varchar(255) AS uab_identificador,
    'Sin información'::varchar(255) AS uab_expediente,
    'Temporal'::varchar(255) AS uab_tipo_sustraccion,
    NULL AS uab_tipo_causal,
    uab_sustrajo,
    CASE WHEN uab_sustrajo = 'Otro' THEN sustrajo ELSE NULL END AS uab_detalle_sustrajo,
    fecha_fin AS uab_fin_sustraccion,
    uab_tipo_sector,
    CASE WHEN uab_tipo_sector = 'Otro' THEN sector ELSE NULL END AS uab_detalle_sector,
    COALESCE(solicitant, 'Sin información')::varchar(255) AS uab_solicitante,
    observacio AS uab_observaciones,
    UPPER(id_ley2) AS uab_areareserva,
    nom_ley2 AS uab_nombre_areareserva,
    NULL AS uab_compensacion,
    'rl2_uab_sustraccion' AS uab_nombre,
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
    'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre,
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado,
    'NIT'::varchar(255) AS interesado_tipo_documento,
    '830.115.395-1'::varchar(255) AS interesado_numero_documento,
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad,
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato,
    fecha_acto AS fuente_administrativa_fecha_documento_fuente,
    fuente_administrativa_nombre::varchar(255) AS fuente_administrativa_nombre,
    NULL AS ddr_tipo_resposabilidad,
    'Sustraer'::varchar(255) AS ddr_tipo_derecho
FROM sustracciones_temporal_limpieza

UNION

SELECT
    -- Bloque para sustracciones definitivas
    uab_identificador,
    'Sin información'::varchar(255) AS uab_expediente,
    'Definitiva'::varchar(255) AS uab_tipo_sustraccion,
    NULL AS uab_tipo_causal,
    uab_sustrajo,
    CASE WHEN uab_sustrajo = 'Otro' THEN sustrajo ELSE NULL END AS uab_detalle_sustrajo,
    NULL AS uab_fin_sustraccion,
    uab_tipo_sector,
    CASE WHEN uab_tipo_sector = 'Otro' THEN sector ELSE NULL END AS uab_detalle_sector,
    COALESCE(solicitant, 'Sin información')::varchar(255) AS uab_solicitante,
    observacio AS uab_observaciones,
    UPPER(id_ley2) AS uab_areareserva,
    nom_ley2 AS uab_nombre_areareserva,
    NULL AS uab_compensacion,
    'rl2_uab_sustraccion' AS uab_nombre,
    ST_Force3D(ST_Transform(geom, 9377)) AS ue_geometria,
    'El Ministerio de Ambiente y Desarrollo Sostenible'::varchar(255) AS interesado_nombre,
    'Persona_Juridica'::varchar(255) AS interesado_tipo_interesado,
    'NIT'::varchar(255) AS interesado_tipo_documento,
    '830.115.395-1'::varchar(255) AS interesado_numero_documento,
    'Documento_Publico.'::varchar(255) AS fuente_administrativa_tipo,
    'Disponible'::varchar(255) AS fuente_administrativa_estado_disponibilidad,
    'Documento'::varchar(255) AS fuente_administrativa_tipo_formato,
    fecha_acto AS fuente_administrativa_fecha_documento_fuente,
    acto_admin::varchar(255) AS fuente_administrativa_nombre,
    NULL AS ddr_tipo_resposabilidad,
    'Sustraer'::varchar(255) AS ddr_tipo_derecho
FROM sustracciones_definitivas_limpieza;

/*========================================
  Fuentes Administrativas de Áreas de Reserva
========================================*/
INSERT INTO estructura_intermedia.rl2_fuenteadministrativa(
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
        substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS fuente_administrativa_tipo,
        substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1,
                  position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)
        ) AS numero_acto,
        substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
    FROM estructura_intermedia.rl2_uab_areareserva cl
)
SELECT 
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE 
         WHEN fuente_administrativa_tipo = 'Resolución' THEN 'Resolucion'
         ELSE fuente_administrativa_tipo
    END AS fuente_administrativa_tipo,
    estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_numero,
    estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM areareserva_acto;


/*========================================
  Fuentes Administrativas de Zonificación
========================================*/
INSERT INTO estructura_intermedia.rl2_fuenteadministrativa(
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
        substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS tipo_acto,
        substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1,
                  position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)
        ) AS numero_acto,
        substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
    FROM estructura_intermedia.rl2_uab_zonificacion cl
)
SELECT 
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE 
        WHEN tipo_acto = 'Resolución' THEN 'Resolucion'
        ELSE tipo_acto
    END AS fuente_administrativa_tipo,
    estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_acto,
    estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM zonificacion_acto;


/*========================================
  Fuentes Administrativas de Sustracciones
========================================*/
INSERT INTO estructura_intermedia.rl2_fuenteadministrativa(
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
    FROM estructura_intermedia.rl2_uab_sustraccion cl   
), homologado_acto_sustraccion AS (
    SELECT 
        uab_identificador,
        acto,
        fuente_administrativa_fecha_documento_fuente,
        CASE 
            WHEN fuente_administrativa_nombre LIKE 'Resolución 41 de 1964 (R. 199 de 1964)' THEN 'Resolución 41 de 1964'	    
            WHEN fuente_administrativa_nombre LIKE 'Res 400_2' THEN 'Resolución 400 de 2017'
            WHEN fuente_administrativa_nombre LIKE 'Res_1526' THEN 'Resolución 1526 de 2017'
            WHEN fuente_administrativa_nombre LIKE 'Res1246_17' THEN 'Resolución 1246 de 2017'
            WHEN fuente_administrativa_nombre LIKE 'Res 1833_2017' THEN 'Resolución 1833 de 2017'
            WHEN fuente_administrativa_nombre LIKE 'Resolución 1743 2014' THEN 'Resolución 1743 de 2014'
            WHEN fuente_administrativa_nombre LIKE 'Res_563_2013' THEN 'Resolución 563 de 2013'
            WHEN fuente_administrativa_nombre LIKE 'Res_503 _2016' THEN 'Resolución 503 de 2016'
            WHEN fuente_administrativa_nombre LIKE 'Resolución 282  2014' THEN 'Resolución 282 de 2014'
            WHEN fuente_administrativa_nombre LIKE 'Res 536 dfe 2019' THEN 'Resolución 536 de 2019'
            WHEN fuente_administrativa_nombre LIKE 'Res 1833_17_' THEN 'Resolución 1833 de 2017'
            WHEN fuente_administrativa_nombre LIKE 'Res 1526_12' THEN 'Resolución 1526 de 2012'
            WHEN fuente_administrativa_nombre LIKE 'Resolución 1085' THEN 'Resolución 1085 de 2012'
            ELSE fuente_administrativa_nombre
        END AS fuente_administrativa_nombre
    FROM split_acto_sustraccion
), sustraccion_acto AS (
    SELECT 
        uab_identificador,
        acto,
        fuente_administrativa_fecha_documento_fuente,
        substring(cl.fuente_administrativa_nombre, 1, position(' ' IN cl.fuente_administrativa_nombre) - 1) AS tipo_acto,
        substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1,
                  position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)
        ) AS numero_acto,
        substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS anio_acto
    FROM homologado_acto_sustraccion cl  
)
SELECT 
    uab_identificador,
    fuente_administrativa_fecha_documento_fuente,
    CASE 
        WHEN tipo_acto IN ('Resolucíon','Resolución','Resoluciión','Res') THEN 'Resolucion'
        ELSE tipo_acto
    END AS fuente_administrativa_tipo,
    estructura_intermedia.homologar_numero(numero_acto::text)::int4 AS fuente_administrativa_numero,
    estructura_intermedia.homologar_numero(anio_acto::text)::int4 AS fuente_administrativa_anio
FROM sustraccion_acto;


/*========================================
  Fuentes Administrativas de Compensaciones
========================================*/
INSERT INTO estructura_intermedia.rl2_fuenteadministrativa(
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
        CASE 	    	
            WHEN fuente_administrativa_nombre LIKE 'Resolucion 033, 350, 1021, 1156, de 2009' 
                THEN 'Resolucion 033 de 2009/Resolucion 350 de 2009/Resolucion 1021 de 2009/Resolucion 1156 de 2009'
            WHEN fuente_administrativa_nombre LIKE 'Resolucion 624 y 1346 de 2009'
                THEN 'Resolucion 624 de 2009/Resolucion 1346 de 2009'
            ELSE fuente_administrativa_nombre
        END AS fuente_administrativa_nombre
    FROM estructura_intermedia.rl2_uab_compensacion cl 
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
        substring(cl.fuente_administrativa_nombre, position(' ' IN cl.fuente_administrativa_nombre) + 1,
                  position(' DE ' IN upper(cl.fuente_administrativa_nombre)) - position(' ' IN cl.fuente_administrativa_nombre)
        ) AS fuente_administrativa_numero,
        substring(cl.fuente_administrativa_nombre, position(' DE ' IN upper(cl.fuente_administrativa_nombre)) + 4) AS fuente_administrativa_anio
    FROM split_acto_compensacion cl
    WHERE left(uab_areareserva, 4) = 'LEY2' 
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


/*========================================
  Relacionar Compensaciones a las Sustracciones
========================================*/
WITH fuente_s AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        fuente_administrativa_tipo_formato,
        fuente_administrativa_numero,
        fuente_administrativa_anio
    FROM estructura_intermedia.rl2_fuenteadministrativa
    WHERE left(uab_identificador, 1) = 'S'
), fuente_c AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        fuente_administrativa_tipo_formato,
        fuente_administrativa_numero,
        fuente_administrativa_anio
    FROM estructura_intermedia.rl2_fuenteadministrativa
    WHERE left(uab_identificador, 1) = 'C'
), fuente_sustraccion_compensacion AS (
    SELECT DISTINCT 
        s.uab_identificador AS uab_sustraccion,
        c.uab_identificador AS uab_compensacion
    FROM fuente_c c  
    LEFT JOIN fuente_s s
        ON s.fuente_administrativa_tipo_formato = c.fuente_administrativa_tipo_formato
        AND s.fuente_administrativa_numero = c.fuente_administrativa_numero
        AND s.fuente_administrativa_anio = c.fuente_administrativa_anio
    WHERE s.uab_identificador IS NOT null
), asigna_expendiente_sustraccion AS (
    SELECT 
        c.uab_sustraccion,
        c.uab_compensacion,
        uc.uab_expediente
    FROM fuente_sustraccion_compensacion c
    LEFT JOIN estructura_intermedia.rl2_uab_compensacion uc
        ON c.uab_compensacion = uc.uab_identificador
)
UPDATE estructura_intermedia.rl2_uab_sustraccion
SET 
    uab_compensacion = sc.uab_compensacion,
    uab_expediente = sc.uab_expediente
FROM asigna_expendiente_sustraccion sc
WHERE sc.uab_sustraccion = rl2_uab_sustraccion.uab_identificador;


/*========================================
  Relacionar Sustracciones a las Compensaciones
========================================*/
WITH fuente_s AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        fuente_administrativa_tipo_formato,
        fuente_administrativa_numero,
        fuente_administrativa_anio
    FROM estructura_intermedia.rl2_fuenteadministrativa
    WHERE left(uab_identificador, 1) = 'S'
), fuente_c AS (
    SELECT 
        uab_identificador,
        fuente_administrativa_fecha_documento_fuente,
        fuente_administrativa_tipo_formato,
        fuente_administrativa_numero,
        fuente_administrativa_anio
    FROM estructura_intermedia.rl2_fuenteadministrativa
    WHERE left(uab_identificador, 1) = 'C'
), fuente_sustraccion_compensacion AS (
    SELECT DISTINCT 
        s.uab_identificador AS uab_sustraccion,
        c.uab_identificador AS uab_compensacion
    FROM fuente_s s
    LEFT JOIN fuente_c c
        ON s.fuente_administrativa_tipo_formato = c.fuente_administrativa_tipo_formato
        AND s.fuente_administrativa_numero = c.fuente_administrativa_numero
        AND s.fuente_administrativa_anio = c.fuente_administrativa_anio
    WHERE s.uab_identificador IS NOT null
), asigna_solicitante_compensacion AS (
    SELECT 
        c.uab_sustraccion,
        c.uab_compensacion,
        s.interesado_nombre
    FROM fuente_sustraccion_compensacion c
    LEFT JOIN estructura_intermedia.rl2_uab_sustraccion s 
        ON c.uab_sustraccion = s.uab_identificador
)
UPDATE estructura_intermedia.rl2_uab_compensacion
SET 
    uab_sustraccion = sc.uab_sustraccion,
    interesado_nombre = sc.interesado_nombre
FROM asigna_solicitante_compensacion sc
WHERE sc.uab_compensacion = rl2_uab_compensacion.uab_identificador;

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


--========================================
--Fijar esquema
--========================================
set search_path to 
	estructura_intermedia, -- Esquema de estructura de datos intermedia
	public;


--========================================
-- Área de reserva Ley 2
--========================================

SELECT *
FROM estructura_intermedia.rl2_uab_areareserva;

SELECT count(*)
FROM estructura_intermedia.rl2_uab_areareserva;
--7 area de Reserva

select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rl2_fuenteadministrativa
where left(uab_identificador,1) ='L'
group by fuente_administrativa_tipo_formato;

--========================================
-- Zonificación Ley 2
--========================================
select * 
FROM estructura_intermedia.rl2_uab_zonificacion;

select count(*) 
FROM estructura_intermedia.rl2_uab_zonificacion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rl2_uab_zonificacion
group by uab_areareserva
order by uab_areareserva;

select uab_tipo_zona,count(*) 
FROM estructura_intermedia.rl2_uab_zonificacion
group by uab_tipo_zona
order by uab_tipo_zona;

select uab_areareserva,uab_tipo_zona,count(*) 
FROM estructura_intermedia.rl2_uab_zonificacion
group by uab_areareserva,uab_tipo_zona
order by uab_areareserva,uab_tipo_zona;

select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rl2_fuenteadministrativa
where left(uab_identificador,1) ='Z'
group by fuente_administrativa_tipo_formato;

--========================================
-- Compensación Ley 
--========================================
SELECT *		
FROM estructura_intermedia.rl2_uab_compensacion;

select count(*) 
FROM estructura_intermedia.rl2_uab_compensacion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rl2_uab_compensacion
group by uab_areareserva
order by uab_areareserva;


select uab_sustraccion,count(*)
FROM estructura_intermedia.rl2_uab_compensacion
group by uab_sustraccion
having count(*)>1;

select *
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion is null;

select *
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion ='SDL2_189';


select fuente_administrativa_tipo_formato,count(*)
from estructura_intermedia.rl2_fuenteadministrativa
where left(uab_identificador,1) ='C'
group by fuente_administrativa_tipo_formato;

--========================================
-- Se listan Compensaciones que no tienen relacionada sustraccion, se relacionan en la Bitacora
--========================================
select *
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion is null;



--========================================
-- Sustracción Ley 2
--========================================
SELECT *		
FROM estructura_intermedia.rl2_uab_sustraccion;

select count(*) 
FROM estructura_intermedia.rl2_uab_sustraccion;

select uab_tipo_sustraccion,count(*) 
FROM estructura_intermedia.rl2_uab_sustraccion
group by uab_tipo_sustraccion;

select uab_areareserva,count(*) 
FROM estructura_intermedia.rl2_uab_sustraccion
group by uab_areareserva
order by uab_areareserva;
--3 sustraciones con dos areas de reserva




--========================================
-- Se eliminan sustraciones que tienen relacionada dos o mas compensaciones, se relacionan en la Bitacora
--========================================
select *
from estructura_intermedia.rl2_uab_sustraccion
where uab_compensacion in (
	select uab_compensacion
	from estructura_intermedia.rl2_uab_sustraccion
	where uab_compensacion is not null
	group by uab_compensacion
	having count(*)>1
);

delete
from estructura_intermedia.rl2_uab_sustraccion
where uab_compensacion in (
	select uab_compensacion
	from estructura_intermedia.rl2_uab_sustraccion
	where uab_compensacion is not null
	group by uab_compensacion
	having count(*)>1
);

--========================================
-- Eliminan  Compensaciones que no tienen relacionada sustraccion, se relacionan en la Bitacora
--========================================
select *
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion not in(
	select uab_identificador
	from estructura_intermedia.rl2_uab_sustraccion 
) 
or uab_sustraccion is null 
or uab_sustraccion  in(
	select uab_sustraccion
	from estructura_intermedia.rl2_uab_compensacion
	group by uab_sustraccion
	having count(*)>1
);

delete 
from estructura_intermedia.rl2_uab_compensacion
where uab_sustraccion not in(
	select uab_identificador
	from estructura_intermedia.rl2_uab_sustraccion 
) 
or uab_sustraccion is null 
or uab_sustraccion  in(
	select uab_sustraccion
	from estructura_intermedia.rl2_uab_compensacion
	group by uab_sustraccion
	having count(*)>1
);
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
WHERE datname = 'etl_rl2' AND pid <> pg_backend_pid();
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
	'LADM_COL_v_1_0_0_Ext_RL2.RL2',
	uuid_generate_v4(),
	'ETL de importación de datos',
	NULL );

--================================================================================
-- 2. Migración de rl2_interesado
--================================================================================
INSERT INTO ladm.rl2_interesado(
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
select 
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	-- Se elimina la obtención de interesado_tipo; se asigna NULL para la columna "tipo"
	null as observacion,
	interesado_nombre, 
	( select t_id from ladm.col_interesadotipo 
	  where ilicode like interesado_tipo_interesado ) as tipo_interesado,
	( select t_id from ladm.col_documentotipo 
	  where ilicode like interesado_tipo_documento ) as tipo_documento,
	interesado_numero_documento,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_interesado' as espacio_de_nombres, 
	row_number() OVER (ORDER BY interesado_nombre) local_id
from (
	-- Se elimina la columna interesado_tipo de la unión
	select 
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rl2_uab_areareserva
	union
	select 
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rl2_uab_sustraccion
	union
	select 
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rl2_uab_zonificacion
	union
	select 
		interesado_nombre,
		interesado_tipo_interesado,
		interesado_tipo_documento,
		interesado_numero_documento
	from estructura_intermedia.rl2_uab_compensacion
) t;


--================================================================================
-- 3. Migración de Area de Reserva
--================================================================================
-- 3.1 Diligenciamiento de la tabla rl2_uab_areareserva
INSERT INTO ladm.rl2_uab_areareserva(
	t_basket, 
	t_ili_tid, 
	nombre_reserva, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)	
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	uab_nombre_reserva,
	uab_nombre,
	( select t_id from ladm.col_unidadadministrativabasicatipo 
	  where ilicode like 'Ambiente_Desarrollo_Sostenible' ) as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_uab_areareserva' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rl2_uab_areareserva; 

-- 3.2 Diligenciamiento de la tabla rl2_ue_areareserva
INSERT INTO ladm.rl2_ue_areareserva(
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( st_area(ue_geometria)/10000 )::numeric(13, 4) as area_ha,
	uab_nombre_reserva as etiqueta,
	( select t_id from ladm.col_relacionsuperficietipo 
	  where ilicode like 'En_Rasante' ) as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_ue_areareserva' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rl2_uab_areareserva; 

-- 3.3 Diligenciamiento de la tabla rl2_derecho para rl2_uab_areareserva
INSERT INTO ladm.rl2_derecho(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rl2_interesado, 
	interesado_rl2_agrupacioninteresados, 
	unidad_rl2_uab_compensacion, 
	unidad_rl2_uab_sustraccion, 
	unidad_rl2_uab_zonificacion, 
	unidad_rl2_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)	
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.rl2_derechotipo 
	  where ilicode like ddr_tipo_derecho ) as tipo,
	null as descripcion,
	( select t_id from ladm.rl2_interesado 
	  where nombre like interesado_nombre ) as interesado_rl2_interesado, 
	null as interesado_rl2_agrupacioninteresados, 
	null as unidad_rl2_uab_compensacion, 
	null as unidad_rl2_uab_sustraccion, 
	null as unidad_rl2_uab_zonificacion, 
	( select t_id from ladm.rl2_uab_areareserva 
	  where local_id like uab_identificador ) as unidad_rl2_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_derecho' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rl2_uab_areareserva; 

-- 3.4 Diligenciamiento de la tabla rl2_fuenteadministrativa para rl2_uab_areareserva
INSERT INTO ladm.rl2_fuenteadministrativa(
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
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.col_fuenteadministrativatipo cf 
	  where ilicode like a.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato ) as tipo,
	null as fecha_fin, 
	( select t_id from ladm.col_estadodisponibilidadtipo ce  
	  where ilicode like a.fuente_administrativa_estado_disponibilidad ) as estado_disponibilidad,
	( select t_id from ladm.col_formatotipo cf  
	  where ilicode like a.fuente_administrativa_tipo_formato ) as tipo_formato,
	a.fuente_administrativa_fecha_documento_fuente fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rl2_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador local_id
from  estructura_intermedia.rl2_fuenteadministrativa f,
	estructura_intermedia.rl2_uab_areareserva a; 


--================================================================================
-- 4. Migración de Zonificación
--================================================================================
INSERT INTO ladm.rl2_uab_zonificacion(
	t_basket, 
	t_ili_tid, 
	tipo_zona, 
	uab_areareserva, 
	nombre, 
	tipo, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.rl2_zonatipo 
	  where ilicode like z.uab_tipo_zona ) as uab_tipo_zona,
	( select t_id from ladm.rl2_uab_areareserva 
	  where nombre_reserva in (select a.uab_nombre_reserva from estructura_intermedia.rl2_uab_areareserva a 
	  where a.uab_identificador=z.uab_areareserva) ) as tipo,
	uab_nombre,
	( select t_id from ladm.col_unidadadministrativabasicatipo 
	  where ilicode like 'Ambiente_Desarrollo_Sostenible' ) as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_uab_zonificacion' as espacio_de_nombres, 
	z.uab_identificador as local_id
from  estructura_intermedia.rl2_uab_zonificacion z; 

-- 4.2 Diligenciamiento de la tabla rl2_ue_zonificacion
INSERT INTO ladm.rl2_ue_zonificacion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( st_area(ue_geometria)/10000 )::numeric(13, 4) as area_ha,
	null as etiqueta,
	( select t_id from ladm.col_relacionsuperficietipo 
	  where ilicode like 'En_Rasante' ) as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_ue_zonificacion' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rl2_uab_zonificacion; 

-- 4.3 Diligenciamiento de la tabla rl2_responsabilidad para rl2_uab_zonificacion
INSERT INTO ladm.rl2_responsabilidad(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rl2_interesado, 
	interesado_rl2_agrupacioninteresados, 
	unidad_rl2_uab_compensacion, 
	unidad_rl2_uab_sustraccion, 
	unidad_rl2_uab_zonificacion, 
	unidad_rl2_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.rl2_responsabilidadtipo  
	  where ilicode like ddr_tipo_resposabilidad ) as tipo,
	null as descripcion,
	( select t_id from ladm.rl2_interesado 
	  where nombre like interesado_nombre ) as interesado_rl2_interesado, 
	null as interesado_rl2_agrupacioninteresados, 
	null as unidad_rl2_uab_compensacion, 
	null as unidad_rl2_uab_sustraccion,
	( select t_id from ladm.rl2_uab_zonificacion 
	  where local_id like uab_identificador ) as unidad_rl2_uab_zonificacion, 
	null as unidad_rl2_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_responsabilidad' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rl2_uab_zonificacion; 

-- 4.4 Diligenciamiento de la tabla rl2_fuenteadministrativa para rl2_uab_areareserva
INSERT INTO ladm.rl2_fuenteadministrativa(
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
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.col_fuenteadministrativatipo cf 
	  where ilicode like z.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato ) as tipo,
	null as fecha_fin, 
	( select t_id from ladm.col_estadodisponibilidadtipo ce  
	  where ilicode like z.fuente_administrativa_estado_disponibilidad ) as estado_disponibilidad,
	( select t_id from ladm.col_formatotipo cf  
	  where ilicode like z.fuente_administrativa_tipo_formato ) as tipo_formato,
	z.fuente_administrativa_fecha_documento_fuente fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rl2_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador local_id
from estructura_intermedia.rl2_fuenteadministrativa f,
	estructura_intermedia.rl2_uab_zonificacion z
where f.uab_identificador = z.uab_identificador;


--================================================================================
-- 5. Migración de Compensación
--================================================================================
INSERT INTO ladm.rl2_uab_compensacion(
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
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	c.uab_expediente,
	c.uab_observaciones,
	( select t_id from ladm.rl2_uab_areareserva 
	  where nombre_reserva in ( select a.uab_nombre_reserva from estructura_intermedia.rl2_uab_areareserva a 
	  where a.uab_identificador = c.uab_areareserva ) ) as tipo,
	uab_nombre,
	( select t_id from ladm.col_unidadadministrativabasicatipo 
	  where ilicode like 'Ambiente_Desarrollo_Sostenible' ) as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_uab_compensacion' as espacio_de_nombres, 
	c.uab_identificador as local_id
from  estructura_intermedia.rl2_uab_compensacion c;	

-- 5.2 Diligenciamiento de la tabla rl2_ue_compensacion
INSERT INTO ladm.rl2_ue_compensacion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
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
from  estructura_intermedia.rl2_uab_compensacion; 

-- 5.3 Diligenciamiento de la tabla rl2_responsabilidad para rl2_uab_compensacion
INSERT INTO ladm.rl2_responsabilidad(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rl2_interesado, 
	interesado_rl2_agrupacioninteresados, 
	unidad_rl2_uab_compensacion, 
	unidad_rl2_uab_sustraccion, 
	unidad_rl2_uab_zonificacion, 
	unidad_rl2_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.rl2_responsabilidadtipo 
	  where ilicode like ddr_tipo_resposabilidad ) as tipo,
	null as descripcion,
	( select t_id from ladm.rl2_interesado 
	  where nombre like interesado_nombre ) as interesado_rl2_interesado, 
	null as interesado_rl2_agrupacioninteresados, 
	( select t_id from ladm.rl2_uab_compensacion 
	  where local_id like uab_identificador ) as unidad_rl2_uab_compensacion, 
	null as unidad_rl2_uab_sustraccion, 
	null as unidad_rl2_uab_zonificacion, 
	null as unidad_rl2_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_responsabilidad' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rl2_uab_compensacion; 

-- 5.4 Diligenciamiento de la tabla rl2_fuenteadministrativa para rl2_uab_areareserva
INSERT INTO ladm.rl2_fuenteadministrativa(
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
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.col_fuenteadministrativatipo cf 
	  where ilicode like c.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato ) as tipo,
	null as fecha_fin, 
	( select t_id from ladm.col_estadodisponibilidadtipo ce  
	  where ilicode like c.fuente_administrativa_estado_disponibilidad ) as estado_disponibilidad,
	( select t_id from ladm.col_formatotipo cf  
	  where ilicode like c.fuente_administrativa_tipo_formato ) as tipo_formato,
	c.fuente_administrativa_fecha_documento_fuente fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre, 
	null as descripcion, 
	null as url, 
	'rl2_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador local_id
from estructura_intermedia.rl2_fuenteadministrativa f,
	estructura_intermedia.rl2_uab_compensacion c
where f.uab_identificador = c.uab_identificador;


--================================================================================
-- 6. Migración de Sustracción
--================================================================================
INSERT INTO ladm.rl2_uab_sustraccion(
	t_basket, 
	t_ili_tid, 
	expediente,
	tipo_sustraccion, 
	tipo_causal, 
	sustrajo, 
	detalle_sustrajo, 
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
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	s.uab_expediente,
	( select t_id from ladm.rl2_sustraccionreservatipo 
	  where ilicode like s.uab_tipo_sustraccion ) as tipo_sustraccion,
	( select t_id from ladm.rl2_causalsustracciontipo 
	  where ilicode like s.uab_tipo_causal ) as tipo_causal,
	( select t_id from ladm.rl2_sustrajotipo 
	  where ilicode like s.uab_sustrajo ) as sustrajo,
	s.uab_detalle_sustrajo,
	s.uab_fin_sustraccion,
	( select t_id from ladm.rl2_sectortipo 
	  where ilicode like s.uab_tipo_sector ) as tipo_sector,
	s.uab_detalle_sector,
	s.uab_observaciones,
	( select t_id from ladm.rl2_uab_areareserva 
	  where local_id like s.uab_areareserva ) as uab_areareserva,
	( select t_id from ladm.rl2_uab_compensacion 
	  where local_id like s.uab_compensacion ) as uab_compensacion,
	uab_nombre,
	( select t_id from ladm.col_unidadadministrativabasicatipo 
	  where ilicode like 'Ambiente_Desarrollo_Sostenible' ) as tipo,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_uab_sustraccion' as espacio_de_nombres, 
	s.uab_identificador local_id
from  estructura_intermedia.rl2_uab_sustraccion s;	

-- 6.2 Diligenciamiento de la tabla rl2_ue_sustraccion
INSERT INTO ladm.rl2_ue_sustraccion (
	t_basket, 
	t_ili_tid, 
	area_ha, 
	etiqueta, 
	relacion_superficie, 
	geometria, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( st_area(ue_geometria)/10000 )::numeric(13, 4) as area_ha,
	null as etiqueta,
	( select t_id from ladm.col_relacionsuperficietipo 
	  where ilicode like 'En_Rasante' ) as relacion_superficie,
	ue_geometria,	
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_ue_sustraccion' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rl2_uab_sustraccion ; 

-- 6.3 Diligenciamiento de la tabla rl2_derecho para rl2_uab_sustraccion
INSERT INTO ladm.rl2_derecho(
	t_basket, 
	t_ili_tid, 
	tipo, 
	descripcion, 
	interesado_rl2_interesado, 
	interesado_rl2_agrupacioninteresados, 
	unidad_rl2_uab_compensacion, 
	unidad_rl2_uab_sustraccion, 
	unidad_rl2_uab_zonificacion, 
	unidad_rl2_uab_areareserva, 
	comienzo_vida_util_version, 
	fin_vida_util_version, 
	espacio_de_nombres, 
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,	
	( select t_id from ladm.rl2_derechotipo rd 
	  where ilicode like ddr_tipo_derecho ) as tipo,
	null as descripcion,
	( select t_id from ladm.rl2_interesado 
	  where nombre like interesado_nombre ) as interesado_rl2_interesado,
	null as interesado_rl2_agrupacioninteresados, 
	null as unidad_rl2_uab_compensacion, 
	( select t_id from ladm.rl2_uab_sustraccion 
	  where local_id like uab_identificador ) as unidad_rl2_uab_sustraccion, 
	null as unidad_rl2_uab_zonificacion, 
	null as unidad_rl2_uab_areareserva,
	now() as comienzo_vida_util_version,
	null as fin_vida_util_version,
	'rl2_derecho' as espacio_de_nombres, 
	uab_identificador local_id
from  estructura_intermedia.rl2_uab_sustraccion ; 

-- 6.4 Diligenciamiento de la tabla rl2_fuenteadministrativa para rl2_uab_areareserva
INSERT INTO ladm.rl2_fuenteadministrativa(
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
	local_id
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	uuid_generate_v4() as t_ili_tid,
	( select t_id from ladm.col_fuenteadministrativatipo cf 
	  where ilicode like s.fuente_administrativa_tipo||f.fuente_administrativa_tipo_formato ) as tipo,
	null as fecha_fin, 
	( select t_id from ladm.col_estadodisponibilidadtipo ce  
	  where ilicode like s.fuente_administrativa_estado_disponibilidad ) as estado_disponibilidad,
	( select t_id from ladm.col_formatotipo cf  
	  where ilicode like s.fuente_administrativa_tipo_formato ) as tipo_formato,
	s.fuente_administrativa_fecha_documento_fuente fecha_documento_fuente, 
	f.fuente_administrativa_tipo_formato||' '||f.fuente_administrativa_numero||' de '||f.fuente_administrativa_anio as nombre,
	null as descripcion, 
	null as url, 
	'rl2_fuenteadministrativa' as espacio_de_nombres, 
	f.uab_identificador local_id
from estructura_intermedia.rl2_fuenteadministrativa f,
	estructura_intermedia.rl2_uab_sustraccion s
where f.uab_identificador = s.uab_identificador;


--================================================================================
-- 7. Migración de col_rrrfuente
--================================================================================
INSERT INTO ladm.col_rrrfuente(
	t_basket, 
	fuente_administrativa, 
	rrr_rl2_derecho, 
	rrr_rl2_responsabilidad
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	*
from (
	select distinct 
		f.t_id as fuente_administrativa,
		null::int8 as rrr_rl2_derecho,
		r.t_id as rrr_rl2_responsabilidad
	from ladm.rl2_fuenteadministrativa f,
		ladm.rl2_responsabilidad r
	where f.local_id = r.local_id 
	union  
	select distinct 
		f.t_id as fuente_administrativa,
		d.t_id as rrr_rl2_derecho,
		null::int4 as rrr_rl2_responsabilidad
	from ladm.rl2_fuenteadministrativa f,
		ladm.rl2_derecho d
	where f.local_id = d.local_id
) t;

--================================================================================
-- 8. Migración de col_uebaunit
--================================================================================
INSERT INTO ladm.col_uebaunit(
	t_basket, 
	ue_rl2_ue_zonificacion, 
	ue_rl2_ue_compensacion, 
	ue_rl2_ue_sustraccion, 
	ue_rl2_ue_areareserva, 
	baunit_rl2_uab_zonificacion,
	baunit_rl2_uab_compensacion, 
	baunit_rl2_uab_sustraccion, 	 
	baunit_rl2_uab_areareserva
)
select
	( select t_id from ladm.t_ili2db_basket 
	  where topic like 'LADM_COL_v_1_0_0_Ext_RL2.RL2' limit 1 ) as t_basket,
	*
from (
	select 
		uez.t_id::int8 as ue_rl2_ue_zonificacion,
		null::int8 as ue_rl2_ue_compensacion,
		null::int8 as ue_rl2_ue_sustraccion,
		null::int8 as ue_rl2_ue_areareserva,
		uabz.t_id::int8 as baunit_rl2_uab_zonificacion,
		null::int8 as baunit_rl2_uab_compensacion,
		null::int8 as baunit_rl2_uab_sustraccion,
		null::int8 as baunit_rl2_uab_areareserva
	from ladm.rl2_ue_zonificacion uez,
		ladm.rl2_uab_zonificacion uabz
	where uez.local_id = uabz.local_id
	union	
	select 
		null as ue_rl2_ue_zonificacion,
		uez.t_id as ue_rl2_ue_compensacion,
		null as ue_rl2_ue_sustraccion,
		null as ue_rl2_ue_areareserva,
		null as baunit_rl2_uab_zonificacion,
		uabz.t_id as baunit_rl2_uab_compensacion,
		null as baunit_rl2_uab_sustraccion,
		null as baunit_rl2_uab_areareserva
	from ladm.rl2_ue_compensacion uez,
		ladm.rl2_uab_compensacion uabz
	where uez.local_id = uabz.local_id
	union  
	select 
		null as ue_rl2_ue_zonificacion,
		null as ue_rl2_ue_compensacion,
		uez.t_id as ue_rl2_ue_sustraccion,
		null as ue_rl2_ue_areareserva,
		null as baunit_rl2_uab_zonificacion,
		null as baunit_rl2_uab_compensacion,
		uabz.t_id as baunit_rl2_uab_sustraccion,
		null as baunit_rl2_uab_areareserva
	from ladm.rl2_ue_sustraccion uez,
		ladm.rl2_uab_sustraccion uabz
	where uez.local_id = uabz.local_id
	union  
	select 
		null as ue_rl2_ue_zonificacion,
		null as ue_rl2_ue_compensacion,
		null as ue_rl2_ue_sustraccion,
		uez.t_id as ue_rl2_ue_areareserva,
		null as baunit_rl2_uab_zonificacion,
		null as baunit_rl2_uab_compensacion,
		null as baunit_rl2_uab_sustraccion,
		uabz.t_id as baunit_rl2_uab_areareserva
	from ladm.rl2_ue_areareserva uez,
		ladm.rl2_uab_areareserva uabz
	where uez.local_id = uabz.local_id
) t;


    """

    return sql_script


