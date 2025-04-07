
# Bitácora de Transformación y Limpieza de Datos
## ETL de Migración de Ley 2 al Modelo LADM_COL-LEY2
- **Fecha de inicio:** 2024-10-01  
- **Propósito:** Realizar la migración de los insumos entregados hacia el modelo estándar LADM_COL-RL2, garantizando la calidad y consistencia de los datos transformados.

---

## Clases y Transformaciones
### Consideraciones Generales
- En el campo `interesado_tipo_interesado`, que es obligatorio, se asigna por defecto el valor `'Persona_Juridica'` en los casos donde no se cuenta con información específica.
- Para garantizar la consistencia del modelo, se establecen los siguientes valores predeterminados en los campos relacionados:
  - `interesado_tipo_documento`: Se asigna `NULL` en ausencia de información.
  - `interesado_numero_documento`: Se asigna `NULL` en ausencia de información.
  - `fuente_administrativa_estado_disponibilidad`: Se asigna el valor `'Disponible'` dado que los documentos administrativos se encuentran disponibles.
  - `fuente_administrativa_tipo_formato`: Se asigna el valor `'Documento'` ya que las fuentes corresponden a documentos formales.
- Las geometrías se procesan mediante transformación a formato 3D y proyección al sistema de referencia espacial SRID 9377, asegurando que cumplan con los estándares establecidos.


### 1. Áreas de Reserva Ley 2
- **Fuente:** `insumos.area_reserva_ley2`
- **Tabla de destino:** `estructura_intermedia.rl2_uab_areareserva`
- **Transformaciones:**
  - Se genera un identificador único (`uab_identificador`) basado en el atributo `id` de origen (ej. `LEY2_01`, `LEY2_02`).
  - Asignación de valores fijos en campos relacionados con el interesado:
    - **interesado_tipo**: `'Regulador'`
    - **interesado_nombre**: `'El Ministerio de Ambiente y Desarrollo Sostenible'`
    - **interesado_tipo_interesado**: `'Persona_Juridica'`
    - **interesado_tipo_documento**: `'NIT'`
    - **interesado_numero_documento**: `'830.115.395-1'`.
  - Fechas y nombres de documentos administrativos transformados y normalizados a la ley origen:
    - **fuente_administrativa_tipo**: `'Documento_Publico.'`
    - **fuente_administrativa_estado_disponibilidad**: `'Disponible'`
    - **fuente_administrativa_tipo_formato**: `'Documento'`
    - **fuente_administrativa_fecha_documento_fuente**: `to_date('1959-01-17', 'YYYY-MM-DD')`
    - **fuente_administrativa_nombre**: `'Ley 2 de 1959'`
  - Se define que el El Ministerio de Ambiente y Desarrollo Sostenible tiene el derecho Realinderar la areas de reserva. 

---

### 2. Zonificación Ley 2
- **Fuente:** `insumos.zonificacion_ley2`
- **Tabla de destino:** `estructura_intermedia.rl2_uab_zonificacion`
- **Transformaciones:**
  - Se genera identificador único (`uab_identificador`) con prefijo `ZNL2_`.
  - Homologación de tipos de zona (`uab_tipo_zona`) utilizando las siguientes reglas:

    Se homologa los valores presentes en la columna `tipo_zoni` de una tabla. El propósito principal es transformar y estandarizar los datos, asignando valores consistentes.

    1. **Transformación inicial:**
       Se aplica la función `replace(tipo_zoni, ' ', '_')` para reemplazar los espacios en blanco por guiones bajos (`_`) en los valores de la columna `tipo_zoni`. Esto asegura que los valores comparados sean uniformes y no contengan diferencias debidas a la presencia de espacios.

    2. **Normalización de texto:**
       Se utiliza la función `homologar_texto` para convertir los valores a un formato uniforme. Por ejemplo, puede convertir texto a minúsculas, eliminar caracteres especiales o aplicar otras reglas de normalización.

    3. **Condiciones:**
       Se realizan comparaciones entre el valor transformado de `tipo_zoni` y los valores homologados esperados. Las condiciones son las siguientes:
        - Si el valor normalizado equivale a `'Tipo_A'`, se asigna `'Tipo_A'`.
        - Si el valor normalizado equivale a `'Tipo_B'`, se asigna `'Tipo_B'`.
        - Si el valor normalizado equivale a `'Tipo_C'`, se asigna `'Tipo_C'`.
        - Si el valor normalizado equivale a `'AREAS_CON_PREVIA_DECISION_DE_ORDENAMIENTO'`, se asigna `'Area_Previa_Decision_Ordenamiento'`.

    4. **Valor por defecto:**
       Si el valor no coincide con ninguna de las opciones anteriores, se asigna `'No aplica'`.

    5. **Resultado final:**
       El valor resultante se almacena en una nueva columna o campo con el nombre `uab_tipo_zona`.

    #### **Ejemplo de Transformación**

    | `tipo_zoni` Original                   | Transformación (`replace` + `homologar_texto`)     | Resultado (`uab_tipo_zona`)           |
    |----------------------------------------|----------------------------------------------------|---------------------------------------|
    | `Tipo A`                               | `Tipo_A`                                           | `Tipo_A`                              |
    | `Tipo B`                               | `Tipo_B`                                           | `Tipo_B`                              |
    | `Tipo C`                               | `Tipo_C`                                           | `Tipo_C`                              |
    | `ÁREAS CON PREVIA DECISIÓN DE ORDENAMIENTO` | `AREAS_CON_PREVIA_DECISION_DE_ORDENAMIENTO`        | `Area_Previa_Decision_Ordenamiento`   |
    | `Otro tipo de zonificación`            | *(No coincide con ninguna opción)*                | `No aplica`                           |
    
    - Relación con áreas de reserva establecida mediante el atributo `id_ley2` existente en el insumo.
    - Asignación de valores fijos en campos relacionados con el interesado:
      - **interesado_tipo**: `'Regulador'`
      - **interesado_nombre**: `'El Ministerio de Ambiente y Desarrollo Sostenible'`
      - **interesado_tipo_interesado**: `'Persona_Juridica'`
      - **interesado_tipo_documento**: `'NIT'`
      - **interesado_numero_documento**: `'830.115.395-1'`.
    - Asignación de valores fijos en campos relacionados con la fuente administrativa:
      - **fuente_administrativa_tipo**: `'Documento_Publico.'`
      - **fuente_administrativa_estado_disponibilidad**: `'Disponible'`
      - **fuente_administrativa_tipo_formato**: `'Documento'`
    - Campos variables en fuente administrativa:
      - **fuente_administrativa_fecha_documento_fuente**: `Se deja nula debido a que no se encuentra en el insumo y el modelo lo permite`
      - **fuente_administrativa_nombre**: `Se toma el campo de (`soporte`) exitente en el insumo`
    - Se define que El Ministerio de Ambiente y Desarrollo Sostenible tiene la responsabilidad Zonificar la areas de reserva. 


---

### 3. Compensaciones Ley 2
- **Fuente:** `insumos.compensacion_ley2`
- **Tabla de destino:** `estructura_intermedia.rl2_uab_compensacion`
- **Transformaciones:**
  - Generación de identificadores únicos (`uab_identificador`) con prefijo `CML2_`.
  - Asignación de valores fijos en campos relacionados con el interesado:
    - **interesado_tipo**: `'Solicitante'`
    - **interesado_tipo_interesado**: `'Persona_Juridica'`
  - En (`interesado_nombre`) de la compensacion  esta null ya que estos se asignaran el proceso de relacion con los actos administrativos las sustraciones
  - En (`interesado_tipo_documento`,`interesado_numero_documento`) de la compensacion  esta null, ya que no se cuentan con esta información.
  - Asignación de valores fijos en campos relacionados con la fuente administrativa:
    - **fuente_administrativa_tipo**: `'Documento_Publico.'`
    - **fuente_administrativa_estado_disponibilidad**: `'Disponible'`
    - **fuente_administrativa_tipo_formato**: `'Documento'`
  - Asignación de valores variables en campos relacionados con la fuente administrativa:
    - **fuente_administrativa_fecha_documento_fuente**: `Se toma el campo de (`fecha_acto`) que se encuentra en el insumo`
    - **fuente_administrativa_nombre**: `Se toma el campo de (`soporte`) exitente en el insumo`
  - Se define que Solicitante tiene la responsabilidad de Compensar. 
---

### 4. Sustracciones Ley 2
- **Fuente:** 
  - `insumos.sustracciones_definitivas_ley2`
  - `insumos.sustracciones_temporales_ley2`
- **Limpieza de datos en las consultas:**
  - `sustracciones_definitivas_limpieza`
  - `sustracciones_temporal_limpieza`
  - Se procede a eliminar los nombres de reserva que tiene `/`, ya que esta tiene poligonos que se encuentran en dos reservas.
- **Tabla de destino:** `estructura_intermedia.rl2_uab_sustraccion`
- **** `estructura_intermedia.rl2_uab_sustraccion`
- **Transformaciones:**
  - Identificadores únicos generados con prefijo `SDL2_`, este no se genera en Temporales, ya que este viene en el insumo.
  - Limpieza y homologación los atributos del campo (`uab_sustrajo`):

    Se homologa los valores presentes en la columna `sustrajo`, asignándolos a una categoría estandarizada.

    #### **Detalles del Bloque**

    1. **Normalización de texto:**
       Se aplica la función `homologar_texto` para asegurar que los valores sean comparados en un formato uniforme, por ejemplo, eliminando diferencias en mayúsculas/minúsculas o caracteres especiales.

    2. **Condiciones:**
       Comparación de los valores normalizados de `sustrajo` con un conjunto de categorías definidas:
        - Si el valor equivale a `'INCODER'`, se asigna `'INCODER'`.
        - Si el valor equivale a `'INCORA'`, se asigna `'INCORA'`.
        - Si el valor equivale a `'INDERENA'`, se asigna `'INDERENA'`.
        - Si el valor equivale a `'MADS'`, se asigna `'MADS'`.
        - Si el valor equivale a `'MAVDT'`, se asigna `'MAVDT'`.
        - Si no coincide con ninguna de las opciones anteriores, se asigna `'Otro'`.

    3. **Resultado final:**
       El resultado se almacena en la columna `uab_sustrajo`.

    #### **Ejemplo de Transformación**

    | `sustrajo` Original | Transformación (`homologar_texto`) | Resultado (`uab_sustrajo`) |
    |----------------------|-----------------------------------|----------------------------|
    | `INCODER`           | `INCODER`                        | `INCODER`                 |
    | `Incoder`           | `INCODER`                        | `INCODER`                 |
    | `INDERENA`          | `INDERENA`                       | `INDERENA`                |
    | `MAVDT`             | `MAVDT`                          | `MAVDT`                   |
    | `Entidad X`         | *(No coincide con ninguna opción)*| `Otro`                    |

  - Se utiliza un bloque `CASE` para determinar el contenido del campo `uab_detalle_sustrajo` basándose en el valor del campo `uab_sustrajo`, proporcionar un nivel adicional de detalle cuando el valor del campo `uab_sustrajo` es genérico (`'Otro'`) y garantizar que el modelo conserve información específica en estos casos excepcionales.
      - Si el valor de `uab_sustrajo` es `'Otro'`, se asigna el contenido del campo original `sustrajo` al campo `uab_detalle_sustrajo`.
  - Este bloque se utiliza para clasificar los valores de la columna `sector` en categorías predefinidas.

    1. **Condiciones:**
      - Asigna categorías basadas en los valores del campo `sector`. Las categorías incluyen:
        - `'Reforma_Agraria'`: Incluye sectores relacionados con actividades agropecuarias, adjudicación de colonos, asuntos indígenas, colonización, y titulaciones de baldíos.
        - `'Energia'`: Incluye generación y transmisión eléctrica.
        - `'Infraestructura_Transporte'`: Incluye sectores de infraestructura vial, sanitaria y ambiental.
        - `'Restitucion_Tierras'`: Se asigna para sectores con actividades de restitución de tierras.
        - `'Mineria'`: Clasificación para el sector de minería.
        - `'Vivienda_VIS_VIP'`: Aplica para sectores relacionados con vivienda VIS y VIP.
        - `'Area_Urbana_Expansion_Rural'`: Incluye áreas urbanas y de expansión rural.
        - `'Inciso_Segundo'`: Específico para un caso particular en la legislación.
        - `'Otro'`: Para sectores que no coinciden con ninguna categoría específica.

    2. **Resultado final:**
      - El resultado se almacena en la columna `uab_tipo_sector`.

    #### **Ejemplo de Transformación**

    | `sector` Original                                           | Resultado (`uab_tipo_sector`)           |
    |-------------------------------------------------------------|-----------------------------------------|
    | `Actividad Agropecuaria`                                    | `Reforma_Agraria`                      |
    | `Generación Electrica`                                      | `Energia`                              |
    | `Minería`                                                   | `Mineria`                              |
    | `Infraestructura vial`                                      | `Infraestructura_Transporte`           |
    | `Titulación / colonos`                                      | `Reforma_Agraria`                      |
    | `Registro Áreas Urbanas, Expansion Urbana, Equipamiento`    | `Area_Urbana_Expansion_Rural`          |
    | `Sector no especificado`                                    | `Otro`                                 |

  - Unión de datos definitivos y temporales, diferenciados por el tipo de sustracción (`uab_tipo_sustraccion`).  
---
### 5. Fuentes Administrativas
La relación entre las sustracciones y compensaciones se realiza por medio de los estándares definidos en las fuentes administrativas. Esto se debe a que las compensaciones asociadas a las sustracciones están vinculadas mediante el mismo acto administrativo. Por ello, se centralizan y estandarizan las fuentes administrativas provenientes de diferentes insumos para garantizar la coherencia en el modelo.

- **Tablas involucradas:** 
  - `rl2_uab_areareserva`, `rl2_uab_zonificacion`, `rl2_uab_sustraccion`, `rl2_uab_compensacion`
- **Tabla de destino:** `estructura_intermedia.rl2_fuenteadministrativa`
- **Transformaciones:**
  - Extracción de tipo, número y año de documentos administrativos desde el nombres de la fuente administrativa.
  - Homologación de nombres de documentos con valores estándar. 
  - En algunos casos, se encuentran varios actos administrativos asociados a una única resolución o sustracción, separados por el carácter `/`. Para solucionar esto, se procede a dividirlos en filas individuales, permitiendo establecer una relación de uno a uno entre cada entidad administrativa y su correspondiente fuente administrativa. Esto asegura una representación más precisa y compatible con el modelo.
  - Se homologa el atributo Resolución a Resolucion para estandarizar con el dominio de tipo de fuente administrativa.
---

### 6. Relación Sustracciones y Compensaciones
- **Tablas involucradas:**
  - `estructura_intermedia.rl2_uab_sustraccion`
  - `estructura_intermedia.rl2_uab_compensacion`
- **Transformaciones:**
  - Identificación de correspondencias basadas en documentos administrativos.
  - Actualización de relaciones (`uab_sustraccion`, `uab_compensacion`) en ambas tablas utilizando el acto administrativo.
---

### 7. Eliminar Sustracciones y Compensaciones no validas. 
- **Tablas involucradas:**
  - `estructura_intermedia.rl2_uab_sustraccion`
  - `estructura_intermedia.rl2_uab_compensacion`
- **Transformaciones:**
    1. **Sustracciones que cuentan con dos o más compensacionesrelacionada:**
      - Se eliminan las sustracciones que cuentan con dos o más compensaciones, ya que no cumplen con el modelo y corresponden a sustracciones que están en proceso de validación.

    2. **Compensaciones sin sustracción relacionada:**
      - Se eliminan las compensaciones que no están relacionadas en la tabla `rl2_uab_sustraccion`.
      - Esto evita la existencia de registros huérfanos o inconsistentes.

    3. **Sustracciones con múltiples compensaciones:**
      - Se eliminan las compensaciones asociadas a sustracciones que tienen más de una compensación registrada.
      - Esto garantiza que cada sustracción esté relacionada con una única compensación.
  

### Notas Adicionales

- Estas eliminaciones aseguran la consistencia entre las tablas relacionadas.
- El registro de los cambios realizados puede documentarse en una bitácora, lo cual permite llevar trazabilidad sobre las acciones de depuración realizadas en la base de datos.

---

## Notas adicionales
1. **Homologación de datos:** Se implementaron funciones específicas como `homologar_texto` y `homologar_numero` para normalizar atributos clave.
2. **Geometrías:** Todas las geometrías cumplen con el estándar del SRID 9377, siendo convertidas a 3D mediante `ST_Force3D`.
3. **Campos faltantes:** En ausencia de datos, se asignaron valores predeterminados o se dejaron como `NULL` cuando aplicaba.

---

