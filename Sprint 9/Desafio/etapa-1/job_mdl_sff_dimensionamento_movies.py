import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, split, trim, when, year, month, dayofmonth, date_format
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH_MOVIES_LOCAL', 'S3_INPUT_PATH_MOVIES_TMDB', 'S3_INPUT_PATH_MOVIES_GENEROS', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

files_movies_local = args['S3_INPUT_PATH_MOVIES_LOCAL']
files_movies_tmdb = args['S3_INPUT_PATH_MOVIES_TMDB']
files_movies_generos = args['S3_INPUT_PATH_MOVIES_GENEROS']

target_path = args['S3_TARGET_PATH']

df_movies_local = spark.read.parquet(files_movies_local)
df_movies_tmdb = spark.read.option("basePath", files_movies_tmdb).parquet(files_movies_tmdb)
df_movies_generos = spark.read.parquet(files_movies_generos)

df_movies_local.createOrReplaceTempView("movies_local")
df_movies_tmdb.createOrReplaceTempView("movies_tmdb")
df_movies_generos.createOrReplaceTempView("movies_generos")

df_join_movies = spark.sql("""
SELECT 
    COALESCE(t2.imdb_id, t1.id) AS id_imdb,
    CAST(t2.movie_id AS INT) AS id_movie,
    COALESCE(t2.tituloprincipal, t1.tituloprincipal) AS titulo_principal,
    COALESCE(t2.titulooriginal, t1.titulooriginal) AS titulo_original,
    CAST(t2.datalancamento AS DATE) AS data_lancamento,
    COALESCE(CAST(t2.anolancamento AS SMALLINT), CAST(t1.anolancamento AS SMALLINT)) AS ano_lancamento,
    COALESCE(t2.genero, t1.genero) AS generos,
    t2.orcamento, 
    t2.receita
FROM 
    movies_tmdb t2 
FULL OUTER JOIN 
    movies_local t1
ON
    t2.imdb_id = t1.id
WHERE 
    t2.receita > 0
""")
df_join_movies.printSchema()
df_join_movies.show()

df_separar_generos_movies = df_join_movies \
    .withColumn("genero", explode(split(col("generos"), ","))) \
    .withColumn("genero", trim(col("genero"))) \
    .select(
        "id_imdb",
        "id_movie",
        "titulo_principal",
        "titulo_original",
        "data_lancamento",
        "ano_lancamento",
        "orcamento",
        "receita",
        "genero"
    )

df_separar_generos_movies.createOrReplaceTempView("separar_generos_movies")

df_join_movies = spark.sql("""
WITH join_generos AS (
    SELECT 
        eg.*,
        g.id AS id_genero
    FROM 
        separar_generos_movies eg
    LEFT JOIN 
        (SELECT 
            id, 
            name 
         FROM 
            movies_generos
        ) g
    ON 
        eg.genero = g.name
)
SELECT 
    id_imdb,
    id_movie,
    titulo_principal,
    titulo_original,
    data_lancamento,
    ano_lancamento,
    id_genero,
    genero,
    orcamento,
    receita
FROM 
    join_generos
ORDER BY 
    id_imdb;
""")
df_join_movies.printSchema()
df_join_movies.show()

dim_filmes = df_join_movies.select(
    col("id_movie").alias("id_filme"),
    "id_imdb",
    "titulo_principal",
    "titulo_original"
).dropDuplicates()
dim_filmes.printSchema()
dim_filmes.show()

dim_datas = df_join_movies \
    .withColumn("mes", month(df_join_movies["data_lancamento"])) \
    .withColumn("dia", dayofmonth(df_join_movies["data_lancamento"])) \
    .withColumn("nome_mes", date_format(df_join_movies["data_lancamento"], "MMMM")) \
    .select(
        "data_lancamento",
        col("ano_lancamento").alias("ano"),
        "mes",
        "nome_mes",
        "dia"
    ).dropDuplicates()
dim_datas.printSchema()
dim_datas.show()
    
dim_generos = df_join_movies.select(
    "id_genero",
    "genero"
).dropDuplicates()
dim_generos.printSchema()
dim_generos.show()

fato_lucro_filmes = df_join_movies.select(
    col("id_movie").alias("id_filme"),
    "id_genero",
    "data_lancamento",
    "orcamento",
    "receita",
    (col("receita") - col("orcamento")).alias("lucro")
).dropDuplicates()
fato_lucro_filmes.printSchema()
fato_lucro_filmes.show()


def salvar_dfs(df, path, partition_keys):
    dynamic_df = DynamicFrame.fromDF(df, glueContext, "dynamic_df")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_df,
        connection_type="s3",
        connection_options={
            "path": target_path + "movies/" + path,
            "partitionKeys": partition_keys
        },
        format="parquet"
    )

salvar_dfs(dim_filmes, "dim_filmes/", [])
salvar_dfs(dim_datas, "dim_datas/", ["ano"])
salvar_dfs(dim_generos, "dim_generos_filmes/", ["genero"])
salvar_dfs(fato_lucro_filmes, "fato_lucro_filmes/", ["id_genero"])

job.commit()