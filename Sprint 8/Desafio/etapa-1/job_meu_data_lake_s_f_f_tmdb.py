import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, expr, concat_ws, explode, collect_list, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file_movies = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# Listar os arquivos na pasta do S3
s3_client = boto3.client('s3')
bucket_name = source_file_movies.split('/')[2]
prefix = '/'.join(source_file_movies.split('/')[3:])

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

arquivos_id_movies = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'movies' in key and 'ids' in key:
        arquivos_id_movies.append(f"s3://{bucket_name}/{key}")

arquivos_movies = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'movies' in key and 'ids' not in key:
        arquivos_movies.append(f"s3://{bucket_name}/{key}")

arquivos_id_series = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'series' in key and 'ids' in key:
        arquivos_id_series.append(f"s3://{bucket_name}/{key}")

arquivos_detalhes_series = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'series' in key and 'detalhes' in key:
        arquivos_detalhes_series.append(f"s3://{bucket_name}/{key}")

arquivos_series = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'series' in key and 'ids' not in key and 'detalhes' not in key:
        arquivos_series.append(f"s3://{bucket_name}/{key}")

arquivo_genres = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'genres' in key:
        arquivo_genres.append(f"s3://{bucket_name}/{key}")

spark_df_movies = spark.read.option("multiline", "true").json(arquivos_movies)
spark_df_movies.printSchema()

spark_df_id_movies = spark.read.option(
    "multiline", "true").json(arquivos_id_movies)
spark_df_id_movies.printSchema()

spark_df_series = spark.read.option("multiline", "true").json(arquivos_series)
spark_df_series.printSchema()

spark_df_detalhes_series = spark.read.option(
    "multiline", "true").json(arquivos_detalhes_series)
spark_df_detalhes_series.printSchema()

spark_df_id_series = spark.read.option(
    "multiline", "true").json(arquivos_id_series)
spark_df_id_series.printSchema()

spark_df_genres = spark.read.option("multiline", "true").json(arquivo_genres)
spark_df_genres = spark_df_genres.withColumn("genre", explode(
    col("genres"))).select(col("genre.id"), col("genre.name"))
spark_df_genres.printSchema()

if spark_df_movies and spark_df_id_movies:
    spark_df_movies = spark_df_movies.select(
        col("id").alias("movie_id"),
        col("title").alias("tituloPrincipal"),
        col("original_title").alias("tituloOriginal"),
        col("release_date").alias("dataLancamento"),
        col("vote_average").alias("notaMedia"),
        col("vote_count").alias("numeroVotos"),
        col("popularity").alias("popularidade"),
        "genre_ids"
    ).withColumn("anoLancamento", year(col("dataLancamento")))

    spark_df_id_movies = spark_df_id_movies.select(
        col("id").alias("movie_id"), "imdb_id"
    )

    # Explodir o array 'genre_ids' para realizar o join
    df_exploded = spark_df_movies.withColumn(
        "genre_id", explode(col("genre_ids")))
    # Realizar o join com o DataFrame de gêneros
    df_joined_genres = df_exploded.join(
        spark_df_genres, df_exploded.genre_id == spark_df_genres.id, "inner")

    # Agrupar por 'id' original e concatenar os nomes dos gêneros em uma string
    df_final_movies = df_joined_genres.groupBy(
        "movie_id", "tituloPrincipal", "tituloOriginal", "dataLancamento",
        "anoLancamento", "notaMedia", "numeroVotos", "popularidade"
    ).agg(concat_ws(",", collect_list("name")).alias("genero"))

    joined_df = spark_df_id_movies.join(
        df_final_movies, on="movie_id", how="inner")
    joined_df.printSchema()

    if not joined_df.rdd.isEmpty():
        dynamic_df_joined = DynamicFrame.fromDF(
            joined_df, glueContext, "dynamic_frame_joined")

        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_df_joined,
            connection_type="s3",
            connection_options={
                "path": target_path + "moviesTMDB/",
                "partitionKeys": ["anoLancamento"]
            },
            format="parquet"
        )
    else:
        print("Nenhum registro combinado encontrado.")
else:
    print("Um ou ambos os DataFrames de entrada estão vazios.")

if spark_df_series and spark_df_id_series:
    spark_df_series = spark_df_series.select(
        "id", "name", "original_name", "first_air_date", "vote_average",
        "vote_count", "popularity"
    ).withColumn("anoPrimeiraExibicao", year(col("first_air_date")))

    spark_df_detalhes_series = spark_df_detalhes_series.select(
        "id", "episode_run_time", "genres", "last_air_date",
        "number_of_episodes"
    ).withColumn("anoUltimaExibicao", year(col("last_air_date")))

    spark_df_id_series = spark_df_id_series.select("id", "imdb_id")

    # Transformar a coluna 'genres' de array de structs para uma string com
    # os nomes dos gêneros separados por vírgulas
    spark_df_detalhes_series = spark_df_detalhes_series.withColumn(
        "genres", concat_ws(",", expr("transform(genres, x -> x.name)")))

    spark_df_detalhes_series = spark_df_detalhes_series.withColumn(
        "episode_run_time", col("episode_run_time").getItem(0).cast("long"))

    joined_df = spark_df_series \
        .join(spark_df_detalhes_series, on="id", how="inner") \
        .join(spark_df_id_series, on="id", how="inner")

    joined_df = joined_df.select(
        col("id").alias("serie_id"),
        "imdb_id",
        col("name").alias("tituloPrincipal"),
        col("original_name").alias("tituloOriginal"),
        col("first_air_date").alias("dataPrimeiraExibicao"),
        col("anoPrimeiraExibicao"),
        col("last_air_date").alias("dataUltimaExibicao"),
        col("anoUltimaExibicao"),
        col("episode_run_time").alias("minutosPorEpisodio"),
        col("number_of_episodes").alias("numeroEpisodios"),
        col("genres").alias("genero"),
        col("popularity").alias("popularidade"),
        col("vote_average").alias("notaMedia"),
        col("vote_count").alias("numeroVotos")
    )
    joined_df.printSchema()

    if not joined_df.rdd.isEmpty():
        dynamic_df_joined = DynamicFrame.fromDF(
            joined_df, glueContext, "dynamic_frame_joined")

        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_df_joined,
            connection_type="s3",
            connection_options={
                "path": target_path + "seriesTMDB/",
                "partitionKeys": ["anoPrimeiraExibicao"]
            },
            format="parquet"
        )
    else:
        print("Nenhum registro combinado encontrado.")
else:
    print("Um ou ambos os DataFrames de entrada estão vazios.")

job.commit()
