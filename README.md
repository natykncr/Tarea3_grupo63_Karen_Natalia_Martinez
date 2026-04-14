Descripcion General

Para el desarrollo del análisis se utilizó un conjunto de datos del Banco Mundial correspondiente al Producto Interno Bruto (PIB) de los países a nivel global. Este dataset fue obtenido de la página oficial del Banco Mundial, descargado desde la terminal y posteriormente cargado en el sistema distribuido HDFS para su procesamiento con Apache Spark.
Se realizó un proceso de limpieza y transformación de los datos, seleccionando 10 países y los últimos 10 años de la base como referencia para el análisis. Posteriormente, se realizó un análisis permitiendo identificar los países con mayor y menor PIB durante el 2024.
Fuente de datos: https://data.worldbank.org/indicator/NY.GDP.MKTP.CD
Tecnologías utilizadas: Python, PySpark, Apache Spark, HDFS
Principales acciones:
•	Eliminación de filas iniciales de metadata del dataset
•	Selección de 10 países específicos para el análisis
•	Filtrado de los últimos 10 años (2015–2024)
•	Reemplazo de valores nulos por 0
•	Conversión de datos para facilitar cálculos
Análisis realizado
•	Visualización de los datos seleccionados
•	Cálculo del promedio del PIB por país
•	10 países con mayor PIB en 2024
•	10 países con menor PIB en 2024
Pasos
•	Descargar el dataset del Banco Mundial
•	Subir el archivo a HDFS
•	Ejecutar el script en PySpark:
•	spark-submit datos_pib.py
Autor: Karen Natalia Martinez Romero
