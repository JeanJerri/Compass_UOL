import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import upper, col, count, desc

args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME', 'S3_INPUT_PATH_MOVIES', 'S3_INPUT_PATH_SERIES',
        'S3_TARGET_PATH'
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file_movies = args['S3_INPUT_PATH_MOVIES']
source_file_series = args['S3_INPUT_PATH_SERIES']
target_path = args['S3_TARGET_PATH']

# Ler o arquivo CSV como DataFrame
df_movies = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file_movies]},
    "csv",
    {"withHeader": True, "separator": "|"},
)

df_series = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file_series]},
    "csv",
    {"withHeader": True, "separator": "|"},
)

spark_df_movies = df_movies.toDF()
spark_df_series = df_series.toDF()

spark_df_movies.printSchema()
spark_df_series.printSchema()

spark_df_movies = spark_df_movies.filter(
    (
        (col("genero").contains("Fantasy")) |
        (col("genero").contains("Sci-Fi")) |
        (col("genero").contains("\\N"))
    ) &
    (
        (col("anoLancamento") > 1979) |
        (col("anoLancamento") == "\\N")
    ) &
    (
        (col("notaMedia") > 6.9) |
        (col("notaMedia") == "\\N")
    ) &
    (
        (col("numeroVotos") > 9) |
        (col("numeroVotos") == "\\N")
    )
)

spark_df_series = spark_df_series.filter(
    (
        (col("genero").contains("Fantasy")) |
        (col("genero").contains("Sci-Fi")) |
        (col("genero").contains("\\N"))
    ) &
    (
        (col("anoLancamento") > 1984) |
        (col("anoLancamento") == "\\N")
    ) &
    (
        (col("notaMedia") > 7.1) |
        (col("notaMedia") == "\\N")
    ) &
    (
        (col("numeroVotos") > 9) |
        (col("numeroVotos") == "\\N")
    )
)

spark_df_movies = spark_df_movies.select(
    "id", "tituloPincipal", "tituloOriginal", "anoLancamento", "genero",
    "notaMedia", "numeroVotos"
)
spark_df_series = spark_df_series.select(
    "id", "tituloPincipal", "tituloOriginal", "anoLancamento", "anoTermino",
    "tempoMinutos", "genero", "notaMedia", "numeroVotos"
)

spark_df_movies = spark_df_movies.dropDuplicates()
spark_df_series = spark_df_series.dropDuplicates()

spark_df_movies = spark_df_movies.withColumnRenamed(
    "tituloPincipal", "tituloPrincipal"
)
spark_df_series = spark_df_series.withColumnRenamed(
    "tituloPincipal", "tituloPrincipal"
)

spark_df_movies.printSchema()
spark_df_series.printSchema()

if spark_df_movies.rdd.isEmpty():
    print("Nenhum registro encontrado para movies.")
else:
    dynamic_df_movies = DynamicFrame.fromDF(
        spark_df_movies, glueContext, "dynamic_frame_name")

    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_df_movies,
        connection_type="s3",
        connection_options={"path": target_path + "moviesLocal/"},
        format="parquet"
    )

if spark_df_series.rdd.isEmpty():
    print("Nenhum registro encontrado para series.")
else:
    dynamic_df_series = DynamicFrame.fromDF(
        spark_df_series, glueContext, "dynamic_frame_name")

    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_df_series,
        connection_type="s3",
        connection_options={"path": target_path + "seriesLocal/"},
        format="parquet"
    )

job.commit()
