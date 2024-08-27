import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, expr, concat_ws, explode, collect_list, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

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

arquivos_movies = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'movies' in key and 'genres' not in key:
        arquivos_movies.append(f"s3://{bucket_name}/{key}")
        
arquivos_id_series = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'series' in key and 'ids' in key:
        arquivos_id_series.append(f"s3://{bucket_name}/{key}")

arquivos_series = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'series' in key and 'ids' not in key:
        arquivos_series.append(f"s3://{bucket_name}/{key}")
        
arquivo_movie_genres = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'movie' in key and 'genres' in key:
        arquivo_movie_genres.append(f"s3://{bucket_name}/{key}")
        
arquivo_serie_genres = []
for content in response.get('Contents', []):
    key = content['Key']
    if 'serie' in key and 'genres' in key:
        arquivo_serie_genres.append(f"s3://{bucket_name}/{key}")

spark_df_movies = spark.read.option("multiline", "true").json(arquivos_movies)
spark_df_movies.printSchema()

spark_df_id_series = spark.read.option("multiline", "true").json(arquivos_id_series)
spark_df_id_series.printSchema()

spark_df_series = spark.read.option("multiline", "true").json(arquivos_series)
spark_df_series.printSchema()

spark_df_movie_genres = spark.read.option("multiline", "true").json(arquivo_movie_genres)
spark_df_movie_genres = spark_df_movie_genres.withColumn("genre", explode(col("genres"))).select(col("genre.id"), col("genre.name"))
spark_df_movie_genres.printSchema()

spark_df_serie_genres = spark.read.option("multiline", "true").json(arquivo_serie_genres)
spark_df_serie_genres = spark_df_serie_genres.withColumn("genre", explode(col("genres"))).select(col("genre.id"), col("genre.name"))
spark_df_serie_genres.printSchema()

if spark_df_movies:
    spark_df_movies = spark_df_movies.select("id", "imdb_id", "title", "original_title", "release_date", "genres", "budget", "revenue") \
        .withColumn("anoLancamento", year(col("release_date"))) \
        .withColumn("genero",concat_ws(",", expr("transform(genres, x -> x.name)")))
        
    spark_df_movies = spark_df_movies.select(
        col("id").alias("movie_id"), 
        "imdb_id", 
        col("title").alias("tituloPrincipal"), 
        col("original_title").alias("tituloOriginal"), 
        col("release_date").alias("dataLancamento"),
        "anoLancamento",
        "genero",
        col("budget").alias("orcamento"),
        col("revenue").alias("receita")
    )
    spark_df_movies.printSchema()

    if not spark_df_movies.rdd.isEmpty():
        dynamic_df = DynamicFrame.fromDF(spark_df_movies, glueContext, "dynamic_frame")

        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_df,
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
    print("O DataFrame de entrada está vazio.")
    
if spark_df_series and spark_df_id_series:
    spark_df_series = spark_df_series.select("id", "name", "original_name", "origin_country", "languages", "original_language", "genres", \
    "first_air_date", "last_air_date", "episode_run_time", "number_of_episodes", "number_of_seasons", "popularity", "vote_average", "vote_count", "status") \
        .withColumn("anoPrimeiraExibicao", year(col("first_air_date"))) \
        .withColumn("anoUltimaExibicao", year(col("last_air_date"))) \
        .withColumn("genero",concat_ws(",", expr("transform(genres, x -> x.name)"))) \
        .withColumn("paisOriginal", concat_ws(",", col("origin_country"))) \
        .withColumn("idiomas", concat_ws(",", col("languages"))) \
        .withColumn("minutosPorEpisodio", col("episode_run_time").getItem(0).cast("long"))
    spark_df_id_series = spark_df_id_series.select("id", "imdb_id")
    
    joined_df = spark_df_series \
        .join(spark_df_id_series, on="id", how="inner")
        
    joined_df = joined_df.select(
        col("id").alias("serie_id"), 
        "imdb_id", 
        col("name").alias("tituloPrincipal"), 
        col("original_name").alias("tituloOriginal"), 
        "paisOriginal", 
        "idiomas", 
        col("original_language").alias("idiomaOriginal"), 
        "genero", 
        col("first_air_date").alias("dataPrimeiraExibicao"), 
        "anoPrimeiraExibicao", 
        col("last_air_date").alias("dataUltimaExibicao"), 
        "anoUltimaExibicao", 
        "minutosPorEpisodio", 
        col("number_of_episodes").alias("numeroEpisodios"), 
        col("number_of_seasons").alias("numeroTemporadas"), 
        col("popularity").alias("popularidade"), 
        col("vote_average").alias("notaMedia"), 
        col("vote_count").alias("numeroVotos"),
        "status"
    )
    joined_df.printSchema()

    if not joined_df.rdd.isEmpty():
        dynamic_df_joined = DynamicFrame.fromDF(joined_df, glueContext, "dynamic_frame_joined")

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
    
if spark_df_movie_genres:
    spark_df_movie_genres = spark_df_movie_genres.select("id", "name")
    spark_df_movie_genres.printSchema()
        
    if not spark_df_movie_genres.rdd.isEmpty():
        dynamic_df = DynamicFrame.fromDF(spark_df_movie_genres, glueContext, "dynamic_df")

        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_df,
            connection_type="s3",
            connection_options={
                "path": target_path + "generosMovies/"
            },
            format="parquet"
        )
    else:
        print("Nenhum registro combinado encontrado.")
else:
    print("O DataFrame de entrada está vazio.")
    
if spark_df_serie_genres:
    spark_df_serie_genres = spark_df_serie_genres.select("id", "name")
    spark_df_serie_genres.printSchema()
        
    if not spark_df_serie_genres.rdd.isEmpty():
        dynamic_df = DynamicFrame.fromDF(spark_df_serie_genres, glueContext, "dynamic_df")

        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_df,
            connection_type="s3",
            connection_options={
                "path": target_path + "generosSeries/"
            },
            format="parquet"
        )
    else:
        print("Nenhum registro combinado encontrado.")
else:
    print("O DataFrame de entrada está vazio.")
    
job.commit()