from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Crear la sesión de Spark para trabajar
spark = SparkSession.builder.appName("PIB").getOrCreate()

# Ruta del archivo que subimos a HDFS
file_path = "hdfs://localhost:9000/datos_pib/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_24.csv"

# Leer el archivo como texto porque tiene líneas iniciales que no sirven
rdd = spark.sparkContext.textFile(file_path)

# En este dataset del Banco Mundial hay unas filas que no son datos,
# por eso se eliminan las primeras 4 líneas
rdd_data = rdd.zipWithIndex().filter(lambda x: x[1] >= 4).keys()

# Convertir los datos limpios en un DataFrame
df = spark.read.csv(rdd_data, header=True, inferSchema=True)

## EXPLORACION

# Mostrar información general para entender mejor la base
print("----------- INFO GENERAL -----------")
print("Filas:", df.count())
print("Columnas:", len(df.columns))

print("----------- NOMBRES DE COLUMNAS -----------")
print(df.columns)

# Lista de países a analizar
countries = [
    "Colombia",
    "Mexico",
    "Russian Federation",
    "Chile",
    "Singapore",
    "China",
    "Japan",
    "United States",
    "Montenegro",
    "Venezuela, RB"
]

# Años que se van a usar en el análisis
years = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]



## LIMPIEZA Y TRANSFORMACION

# Filtrar solo los países seleccionados
df_countries = df.filter(F.col("Country Name").isin(countries))

print("----------- PAISES SELECCIONADOS -----------")
df_countries.select("Country Name", "Country Code").show(20, truncate=False)

print("----------- DATOS DE LOS 10 PAISES Y 10 AÑOS -----------")
df_countries.select(
    "Country Name",
    "Country Code",
    *years
).show(truncate=False)

# Se reemplazan los valores nulos por 0 en los años seleccionados para evitar problemas en los cálculos posteriores
df_countries = df_countries.fillna(0, subset=years)



## ANALISIS

# Se calcula el promedio del PIB por pais usando los años seleccionados
print("----------- PROMEDIO POR PAISES SELECCIONADOS ---------------")

sum_expr = None
for year in years:
    col_expr = F.col(year)
    if sum_expr is None:
        sum_expr = col_expr
    else:
        sum_expr = sum_expr + col_expr

# Se calcula el promedio dividiendo entre la cantidad de años
df_avg_country = df_countries.select(
    "Country Name",
    "Country Code",
    (sum_expr / len(years)).alias("Average_GDP")
)

# Se ordenan los datos de mayor a menor y se da formato a los valores
df_avg_country = df_avg_country.orderBy(F.col("Average_GDP").desc())

df_avg_country.select(
    "Country Name",
    "Country Code",
    F.format_number("Average_GDP", 0).alias("Average_GDP")
).show(truncate=False)

# Paises con mayor PIB en 2024 entre todos los paises
print("----------- 10 PAISES CON MAYOR PIB EN 2024 ---------------")

df_top_2024 = df.select(
    "Country Name",
    "Country Code",
    F.col("2024").alias("GDP_2024")
).dropna(subset=["GDP_2024"]).orderBy(F.col("GDP_2024").desc())

df_top_2024.select(
    "Country Name",
    "Country Code",
    F.format_number("GDP_2024", 0).alias("GDP_2024")
).show(10, truncate=False)

# Paises con menor PIB en 2024 entre todos los paises
print("----------- 10 PAISES CON MENOR PIB EN 2024 ---------------")

df_lower_2024 = df.select(
    "Country Name",
    "Country Code",
    F.col("2024").alias("GDP_2024")
).dropna(subset=["GDP_2024"]).orderBy(F.col("GDP_2024").asc())

df_lower_2024.select(
    "Country Name",
    "Country Code",
    F.format_number("GDP_2024", 0).alias("GDP_2024")
).show(10, truncate=False)

spark.stop()