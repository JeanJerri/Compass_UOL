import boto3

s3 = boto3.client('s3')

s3.upload_file('movies.csv', 'meu-data-lake-sci-fi-fantasia',
               'Raw/Local/CSV/Movies/2024/07/22/movies.csv')
s3.upload_file('series.csv', 'meu-data-lake-sci-fi-fantasia',
               'Raw/Local/CSV/Series/2024/07/22/series.csv')
