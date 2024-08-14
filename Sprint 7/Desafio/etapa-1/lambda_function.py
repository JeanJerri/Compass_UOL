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
            print(
                f"Falha ao buscar dados, página {
                    page
                }: {
                    resposta.status_code
                }"
            )
            break

        dados = resposta.json()
        todos_os_resultados.extend(dados.get('results', []))

        total_pages = dados.get('total_pages', 1)
        page += 1

    return todos_os_resultados


def buscar_detalhes_da_serie(series_id):
    url = f"https://api.themoviedb.org/3/tv/{series_id}?language=en-US"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {AUTH_TOKEN}"
    }
    resposta = requests.get(url, headers=headers)
    if resposta.status_code != 200:
        print(
            f"Falha ao buscar detalhes para o ID {
                series_id
            }: {
                resposta.status_code
            }"
        )
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
            print(
                f"Falha ao buscar dados, página {
                    page
                }: {
                    resposta.status_code
                }"
            )
            break

        dados = resposta.json()
        todos_os_resultados.extend(dados.get('results', []))

        total_pages = dados.get('total_pages', 1)
        page += 1

    return todos_os_resultados


def salvar_no_s3(dados, nome_arquivo):
    caminho_data = datetime.now().strftime('%Y/%m/%d')
    caminho_s3 = f"Raw/TMDB/JSON/{caminho_data}/{nome_arquivo}"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=caminho_s3,
        Body=json.dumps(dados, indent=4),
        ContentType='application/json'
    )


def lambda_handler(evento, contexto):
    todas_as_series = buscar_todas_as_series()

    tamanho_bloco = 100
    num_blocos = math.ceil(len(todas_as_series) / tamanho_bloco)

    for i in range(num_blocos):
        bloco = todas_as_series[i * tamanho_bloco: (i + 1) * tamanho_bloco]
        nome_arquivo = f"series_{i + 1}.json"
        salvar_no_s3(bloco, nome_arquivo)

    todas_as_series_detalhadas = []
    for serie in todas_as_series:
        id_serie = serie['id']
        detalhes = buscar_detalhes_da_serie(id_serie)
        if detalhes:
            todas_as_series_detalhadas.append(detalhes)

    num_blocos = math.ceil(len(todas_as_series_detalhadas) / tamanho_bloco)

    for i in range(num_blocos):
        bloco = todas_as_series_detalhadas[
            i * tamanho_bloco:
            (i + 1) * tamanho_bloco
        ]
        nome_arquivo = f"series_detalhes_{i + 1}.json"
        salvar_no_s3(bloco, nome_arquivo)

    todos_os_movies = buscar_todos_os_movies()

    num_blocos = math.ceil(len(todos_os_movies) / tamanho_bloco)

    for i in range(num_blocos):
        bloco = todos_os_movies[i * tamanho_bloco: (i + 1) * tamanho_bloco]
        nome_arquivo = f"movies_{i + 1}.json"
        salvar_no_s3(bloco, nome_arquivo)

    return {
        'statusCode': 200,
        'body': json.dumps('Dados buscados e salvos com sucesso.')
    }
