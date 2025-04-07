# ETL_RL2
ETL para transformar los datos de reserva ley 2 al modelo extendido LADM_COL de Reservas Ley 2

# Documentación de Shapes de Reservas Forestales Ley 2 de 1959

Este repositorio proporciona enlaces para descargar y almacenar localmente los datos geográficos de las Reservas Forestales según la Ley 2 de 1959 en Colombia. Los shapes descargados se encuentran en la carpeta `Insumos` en formato comprimido (ZIP).

## Estructura de la Carpeta `Insumos`

Dentro de la carpeta `Insumos`, encontrarás los archivos ZIP correspondientes a cada capa:

1. **COMP_SUSTR_RESERVAS_20240815.zip** - 848 KB  
   Contiene la capa de **Compensación Reservas**.

2. **Reservas_ley2_Noviembre_2023.zip** - 9.380 KB  
   Contiene la capa de **Reservas Forestales Ley 2 de 1959**.

3. **Sustracciones_definitivas_Ley2_Septiembre_2023.zip** - 11.451 KB  
   Contiene la capa de **Sustracciones Definitivas Reservas Forestales**.

4. **Sustracciones_Temporales_Ley2_septiembre_2021.zip** - 3.661 KB  
   Contiene la capa de **Sustracciones Temporales Reservas Forestales**.

5. **Zonificacion_Ley2_Septiembre_2023.zip** - 35.061 KB  
   Contiene la capa de **Zonificación Reservas Forestales**.

## Enlaces de Descarga

Para acceder a la última versión de cada shape en línea, puedes utilizar los siguientes enlaces:

1. **Reservas Forestales Ley 2 de 1959**  
   [Descargar Shape](https://siac-datosabiertos-mads.hub.arcgis.com/datasets/MADS::reservas-de-ley2-noviembre-2023-escala-1100-000/about)

2. **Zonificación Reservas Forestales Ley 2 de 1959**  
   [Descargar Shape](https://siac-datosabiertos-mads.hub.arcgis.com/datasets/MADS::zonificaci%C3%B3n-reservas-de-ley2-septiembre-de-2023/about)

3. **Sustracciones Definitivas Reservas Forestales Ley 2 de 1959**  
   [Descargar Shape](https://siac-datosabiertos-mads.hub.arcgis.com/datasets/sustracciones-definitivas-reservas-de-ley2-septiembre-de-2023/about)

4. **Sustracciones Temporales Reservas Forestales Ley 2 de 1959**  
   [Descargar Shape](https://siac-datosabiertos-mads.hub.arcgis.com/datasets/MADS::sustraccion-temporal-ley2-septiembre-2021/about)

5. **Compensación Reservas**  
   [Descargar Shape](https://siac-datosabiertos-mads.hub.arcgis.com/datasets/75a38dc85b664c3ead307b65b21e8900/about)

## Descripción de los Datos

- **Reservas Forestales Ley 2 de 1959**: Representa las áreas protegidas bajo la Ley 2 de 1959.
- **Zonificación Reservas Forestales Ley 2 de 1959**: Clasificación interna de las zonas protegidas.
- **Sustracciones Definitivas**: Áreas excluidas permanentemente de la reserva.
- **Sustracciones Temporales**: Áreas con exclusión temporal de la reserva.
- **Compensación Reservas**: Áreas designadas como compensación para las reservas afectadas por sustracciones.


# Flujo de Proceso para la Importación y Transformación de Datos

![Proceso Importar](https://github.com/user-attachments/assets/b9438400-cef1-4e54-a2b6-8025527dea6b)

1. **Clonar el repositorio del Modelo**  
   - Se descarga localmente el repositorio que contiene el modelo Modelo_Reservas_Ley_2.

2. **Clonar el repositorio de ETL**  
   - Se clona el repositorio actual.

3. **Crear la base de datos en PostgreSQL**  
   - Con ambos repositorios ya disponibles, se crea una base de datos donde residirán todos los esquemas y tablas (intermedias y finales).

4. **Ejecutar el script de creación de estructura**  
   - Se corre el archivo `1.estructura_intermedia.sql`, que construye las tablas y esquemas necesarios para almacenar los datos de entrada.

6. **Crear el esquema `insumos`**  
   - Se crea un esquema específico para los datos de respaldo, shapes, etc.

7. **Importar `insumos`**  

   **Opcion 1: Importar Backup a `insumos` (Recomendada)**  
   - Se restauran el backup que se encuentra en la carpeta insumos del presente repositorio, en el esquema `insumos`.

   **Opcion 2: Importar Shapes a `insumos`**  
   - Se cargan los shapefiles (u otros formatos) al esquema `insumos, que se encuentran referenciados en la carpeta insumos.

8. **Crear el esquema LADM**  
   - Se crea el esquema LADM extendido para Reservas Ley 2, donde finalmente se alojará la información transformada.

9. **Importar el modelo a LADM con Model Baker**  
    - Se utiliza Model Baker para importar el modelo LADM, sincronizando las tablas y estructuras.

10. **Ejecutar el script de transformación**  
    - El archivo `2.transformar_datos.sql` realiza la conversión de los datos desde `insumos` hacia la estructura intermedia.

11. **Validar la transformación**  
    - Se corre `3.validar_estructura.sql` para verificar la coherencia (llaves foráneas, dominios, tipos, etc.) de lo recién transformado.

12. **Importar al modelo LADM**  
    - Con la validación previa completada, se utiliza `4.importar_al_modelo.sql` para llevar los datos de estrcutura intermedia al esquema LADM.

13. **Validar el modelo en Model Baker**  
    - Se verifica nuevamente en Model Baker para asegurar que todas las reglas del LADM extendido se cumplan.

14. **Exportar XTF**  
    - Finalmente, se exporta la información en el formato Interlis Transfer Format (XTF), validado y listo para intercambio.

15. **Fin**  
    - El proceso concluye cuando se obtiene el archivo XTF final, conforme al modelo LADM para Reservas Ley 2.



## Licencia

Consulte los términos de uso en el sitio de datos abiertos del [SIAC](https://siac-datosabiertos-mads.hub.arcgis.com/).

---

**Nota**: Asegúrate de tener las herramientas necesarias para trabajar con archivos geoespaciales, como software de SIG cono QGIS o scripts en lenguajes compatibles como Python o R, entre muchos otros.

