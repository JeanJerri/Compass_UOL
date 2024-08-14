import json
import requests
import math
import boto3
import os
from datetime import datetime

AUTH_TOKEN = os.environ['AUTH_TOKEN']

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {AUTH_TOKEN}"
}

s3_client = boto3.client('s3')
bucket_name = 'meu-data-lake-sci-fi-fantasia'


def buscar_todas_as_series():
    todos_os_resultados = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        url = f"https://api.themoviedb.org/3/discover/tv\
?first_air_date.gte=1985-01-01&first_air_date.lte=2025-01-01&\
include_adult=false&include_null_first_air_dates=false&\
language=en-US&page={page}&sort_by=name.asc&vote_average.gte=7.2&\
vote_count.gte=10&with_genres=10765"
        resposta = requests.get(url, headers=headers)
        if resposta.status_code != 200:
            print(f"Falha ao buscar dados, página {
                  page}: {resposta.status_code}")
            break

        dados = resposta.json()
        todos_os_resultados.extend(dados.get('results', []))

        total_pages = dados.get('total_pages', 1)
        page += 1

    return todos_os_resultados


def buscar_detalhes_da_serie(series_id):
    url = f"https://api.themoviedb.org/3/tv/{series_id}?language=en-US"
    resposta = requests.get(url, headers=headers)
    if resposta.status_code != 200:
        print(f"Falha ao buscar detalhes para o ID {
              series_id}: {resposta.status_code}")
        return None
    return resposta.json()


def buscar_todos_os_movies():
    todos_os_resultados = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        url = f"https://api.themoviedb.org/3/discover/movie\
?include_adult=false&include_video=false&language=en-US&\
page={page}&primary_release_date.gte=1980-01-01&\
primary_release_date.lte=2029-01-01&sort_by=title.asc&\
vote_average.gte=7&vote_count.gte=10&with_genres=14%7C878"
        resposta = requests.get(url, headers=headers)
        if resposta.status_code != 200:
            print(f"Falha ao buscar dados, página {
                  page}: {resposta.status_code}")
            break

        dados = resposta.json()
        todos_os_resultados.extend(dados.get('results', []))

        total_pages = dados.get('total_pages', 1)
        page += 1

    return todos_os_resultados


def salvar_no_s3(dados, nome_arquivo, folder="Raw/TMDB/JSON"):
    caminho_data = datetime.now().strftime('%Y/%m/%d')
    caminho_s3 = f"{folder}/{caminho_data}/{nome_arquivo}"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=caminho_s3,
        Body=json.dumps(dados, indent=4),
        ContentType='application/json'
    )

    return caminho_s3


def ler_arquivos_json_s3(caminho_s3):
    response = s3_client.get_object(Bucket=bucket_name, Key=caminho_s3)
    conteudo = response['Body'].read().decode('utf-8')
    return json.loads(conteudo)


def extrair_ids(dados):
    return [item['id'] for item in dados]


def buscar_external_ids_tv(series_id):
    url = f"https://api.themoviedb.org/3/tv/{series_id}/external_ids"
    resposta = requests.get(url, headers=headers)
    if resposta.status_code != 200:
        print(f"Falha ao buscar external_ids para a série ID {
              series_id}: {resposta.status_code}")
        return None
    return resposta.json()


def buscar_external_ids_movie(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/external_ids"
    resposta = requests.get(url, headers=headers)
    if resposta.status_code != 200:
        print(f"Falha ao buscar external_ids para o filme ID {
              movie_id}: {resposta.status_code}")
        return None
    return resposta.json()


def processar_external_ids(tipo, caminhos_arquivos):
    for caminho_s3 in caminhos_arquivos:
        dados = ler_arquivos_json_s3(caminho_s3)
        ids = extrair_ids(dados)

        external_ids_detalhes = []
        tamanho_bloco = 100
        num_blocos = math.ceil(len(ids) / tamanho_bloco)

        for i in range(num_blocos):
            bloco_ids = ids[i * tamanho_bloco: (i + 1) * tamanho_bloco]

            for item_id in bloco_ids:
                if tipo == 'movies':
                    detalhes = buscar_external_ids_movie(item_id)
                else:  # tipo == 'series'
                    detalhes = buscar_external_ids_tv(item_id)

                if detalhes:
                    filtered_details = {
                        'id': detalhes.get('id'),
                        'imdb_id': detalhes.get('imdb_id')
                    }
                    external_ids_detalhes.append(filtered_details)

            nome_arquivo_saida = f"{caminho_s3.split(
                '/')[-1].split('.')[0]}_external_ids_{i + 1}.json"
            salvar_no_s3(external_ids_detalhes, nome_arquivo_saida)


def buscar_generos_de_filmes():
    url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
    resposta = requests.get(url, headers=headers)
    if resposta.status_code != 200:
        print(f"Falha ao buscar gêneros de filmes: {resposta.status_code}")
        return None
    return resposta.json()


def salvar_generos_filmes_no_s3():
    generos = buscar_generos_de_filmes()
    if generos:
        nome_arquivo = "movie_genres.json"
        caminho_s3 = salvar_no_s3(generos, nome_arquivo)
        print(f"Gêneros de filmes salvos em {caminho_s3}")


def lambda_handler(evento, contexto):
    todas_as_series = buscar_todas_as_series()
    tamanho_bloco = 100
    num_blocos = math.ceil(len(todas_as_series) / tamanho_bloco)

    caminhos_arquivos_series = []
    for i in range(num_blocos):
        bloco = todas_as_series[i * tamanho_bloco: (i + 1) * tamanho_bloco]
        nome_arquivo = f"series_{i + 1}.json"
        caminho_s3 = salvar_no_s3(bloco, nome_arquivo)
        caminhos_arquivos_series.append(caminho_s3)

    todas_as_series_detalhadas = []
    for serie in todas_as_series:
        id_serie = serie['id']
        detalhes = buscar_detalhes_da_serie(id_serie)
        if detalhes:
            todas_as_series_detalhadas.append(detalhes)

    num_blocos = math.ceil(len(todas_as_series_detalhadas) / tamanho_bloco)

    for i in range(num_blocos):
        bloco = todas_as_series_detalhadas[
            i * tamanho_bloco: (i + 1) * tamanho_bloco
        ]
        nome_arquivo = f"series_detalhes_{i + 1}.json"
        caminho_s3 = salvar_no_s3(bloco, nome_arquivo)

    todos_os_movies = buscar_todos_os_movies()
    num_blocos = math.ceil(len(todos_os_movies) / tamanho_bloco)

    caminhos_arquivos_movies = []
    for i in range(num_blocos):
        bloco = todos_os_movies[i * tamanho_bloco: (i + 1) * tamanho_bloco]
        nome_arquivo = f"movies_{i + 1}.json"
        caminho_s3 = salvar_no_s3(bloco, nome_arquivo)
        caminhos_arquivos_movies.append(caminho_s3)

    processar_external_ids('series', caminhos_arquivos_series)
    processar_external_ids('movies', caminhos_arquivos_movies)
    salvar_generos_filmes_no_s3()

    return {
        'statusCode': 200,
        'body': json.dumps('Dados buscados, salvos e external_ids processados \
com sucesso.')
    }
