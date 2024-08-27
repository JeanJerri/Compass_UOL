import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, split, trim, when, coalesce, year, month, dayofmonth, date_format, dense_rank
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH_SERIES_LOCAL', 'S3_INPUT_PATH_SERIES_TMDB', 'S3_INPUT_PATH_SERIES_GENEROS', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

target_path = args['S3_TARGET_PATH']

files_series_local = args['S3_INPUT_PATH_SERIES_LOCAL']
files_series_tmdb = args['S3_INPUT_PATH_SERIES_TMDB']
files_series_generos = args['S3_INPUT_PATH_SERIES_GENEROS']

df_series_local = spark.read.parquet(files_series_local)
df_series_tmdb = spark.read.option("basePath", files_series_tmdb).parquet(files_series_tmdb)
df_series_generos = spark.read.parquet(files_series_generos)

df_join_series = df_series_tmdb.alias("t2").join(
    df_series_local.alias("t1"),
    col("t2.imdb_id") == col("t1.id"),
    "full_outer"
).select(
    coalesce(col("t2.imdb_id"), col("t1.id")).alias("id_imdb"),
    col("t2.serie_id").cast("int").alias("id_serie"),
    coalesce(col("t2.tituloprincipal"), col("t1.tituloprincipal")).alias("titulo_principal"),
    coalesce(col("t2.titulooriginal"), col("t1.titulooriginal")).alias("titulo_original"),
    col("t2.paisoriginal").alias("pais_original"),
    coalesce(when(trim(col("t2.idiomas")) == "", None).otherwise(trim(col("t2.idiomas"))), col("t2.idiomaoriginal")).alias("idiomas"),
    col("t2.idiomaoriginal").alias("idioma_original"),
    col("t2.dataprimeiraexibicao").cast("date").alias("data_primeira_exibicao"),
    col("t2.dataultimaexibicao").cast("date").alias("data_ultima_exibicao"),
    coalesce(col("t2.anoprimeiraexibicao").cast("smallint"), col("t1.anolancamento").cast("smallint")).alias("ano_lancamento"),
    coalesce(col("t2.anoultimaexibicao").cast("smallint"), col("t1.anotermino").cast("smallint")).alias("ano_termino"),
    coalesce(col("t2.genero"), col("t1.genero")).alias("generos"),
    coalesce(col("t2.minutosporepisodio"), col("t1.tempominutos")).alias("tempo_minutos"),
    col("t2.numeroepisodios").cast("smallint").alias("numero_episodios"),
    col("t2.numerotemporadas").cast("smallint").alias("numero_temporadas"),
    col("t2.popularidade").cast("int").alias("popularidade"),
    coalesce(col("t2.notamedia").cast("double"), col("t1.notamedia").cast("double")).alias("nota_media"),
    coalesce(col("t2.numerovotos").cast("int"), col("t1.numerovotos").cast("int")).alias("numero_votos"),
    col("t2.status")
).filter(
    coalesce(col("t2.genero"), col("t1.genero")).isNotNull() & col("popularidade").isNotNull()
)
df_join_series.printSchema()

df_separar_colunas_series = df_join_series \
    .withColumn("pais_original", explode(split(col("pais_original"), ","))).withColumn("pais_original", trim(col("pais_original"))) \
    .withColumn("idioma", explode(split(col("idiomas"), ","))).withColumn("idioma", trim(col("idioma"))) \
    .withColumn("idioma_original", explode(split(col("idioma_original"), ","))).withColumn("idioma_original", trim(col("idioma_original"))) \
    .withColumn("genero", explode(split(col("generos"), ","))).withColumn("genero", trim(col("genero"))) \
    .withColumn("tempo_minutos", when(col("tempo_minutos") == "\\N", None).otherwise(col("tempo_minutos").cast("smallint"))) \
    .select(
        "id_imdb",
        "id_serie",
        "titulo_principal",
        "titulo_original",
        "data_primeira_exibicao",
        "data_ultima_exibicao",
        "ano_lancamento",
        "ano_termino",
        "numero_episodios",
        "numero_temporadas",
        "popularidade",
        "nota_media",
        "numero_votos",
        "status",
        "pais_original",
        "idioma",
        "idioma_original",
        "genero",
        "tempo_minutos"
    )
df_separar_colunas_series.printSchema()

# Adição dos IDs
df_join_series = df_separar_colunas_series.alias("eg").join(
    df_series_generos.alias("g"),
    col("eg.genero") == col("g.name"),
    "left"
).select(
    col("eg.*"),
    col("g.id").alias("id_genero")
) \
.withColumn("id_idioma", dense_rank().over(Window.orderBy("idioma"))) \
.withColumn("id_duracao", dense_rank().over(Window.orderBy("numero_temporadas", "numero_episodios")))
df_join_series.printSchema()


dim_series = df_join_series.select(
    "id_serie", 
    "id_imdb", 
    "titulo_principal", 
    "titulo_original",
    "pais_original", 
    "idioma_original", 
    "status"
).dropDuplicates()
dim_series.printSchema()

dim_primeira_exibicao = df_join_series \
    .withColumn("ano", year(col("data_primeira_exibicao"))) \
    .withColumn("mes", month(col("data_primeira_exibicao"))) \
    .withColumn("nome_mes", date_format(col("data_primeira_exibicao"), "MMMM")) \
    .withColumn("dia", dayofmonth(col("data_primeira_exibicao"))) \
    .select(
        "data_primeira_exibicao", 
        "ano", 
        "mes", 
        "nome_mes", 
        "dia"
    ).dropDuplicates()
dim_primeira_exibicao.printSchema()

dim_ultima_exibicao = df_join_series \
    .withColumn("ano", year(col("data_ultima_exibicao"))) \
    .withColumn("mes", month(col("data_ultima_exibicao"))) \
    .withColumn("nome_mes", date_format(col("data_ultima_exibicao"), "MMMM")) \
    .withColumn("dia", dayofmonth(col("data_ultima_exibicao"))) \
    .select(
        "data_ultima_exibicao", 
        "ano", 
        "mes", 
        "nome_mes", 
        "dia"
    ).dropDuplicates()
dim_ultima_exibicao.printSchema()

dim_generos = df_join_series.select(
    "id_genero", 
    "genero"
).dropDuplicates()
dim_generos.printSchema()

dim_idiomas = df_join_series.select(
    "id_idioma", 
    "idioma"
).dropDuplicates()
dim_idiomas.printSchema()

dim_duracao = df_join_series.select(
    "id_duracao", 
    "tempo_minutos", 
    "numero_episodios", 
    "numero_temporadas"
).dropDuplicates()
dim_duracao.printSchema()

fato_formato_series = df_join_series.select(
    "id_serie", 
    "data_primeira_exibicao", 
    "ano_lancamento",
    "data_ultima_exibicao", 
    "ano_termino", 
    "id_genero",
    "id_idioma", 
    "id_duracao", 
    "popularidade",
    "nota_media", 
    "numero_votos"
).dropDuplicates()
fato_formato_series.printSchema()

def salvar_dfs(df, path, partition_keys):
    dynamic_df = DynamicFrame.fromDF(df, glueContext, "dynamic_df")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_df,
        connection_type="s3",
        connection_options={
            "path": target_path + "series/" + path,
            "partitionKeys": partition_keys
        },
        format="parquet"
    )

salvar_dfs(dim_primeira_exibicao, "dim_primeira_exibicao/", ["ano"])
salvar_dfs(dim_ultima_exibicao, "dim_ultima_exibicao/", ["ano"])
salvar_dfs(dim_series, "dim_series/", ["status", "pais_original"])
salvar_dfs(dim_generos, "dim_generos_series/", ["genero"])
salvar_dfs(dim_idiomas, "dim_idiomas/", ["idioma"])
salvar_dfs(dim_duracao, "dim_duracao/", ["numero_temporadas", "numero_episodios"])
salvar_dfs(fato_formato_series, "fato_formato_series/", ["id_genero"])

job.commit()
