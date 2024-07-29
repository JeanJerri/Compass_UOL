# Desafio

O desafio consiste na construção de um Data Lake para filmes e séries que está dividido em 5 entregas, 1 em cada sprint. Nas entregas seram realizadas a Ingestão, o Armazenamento, o Processamento e o Consumos dos dados.


# Análise dos dados

- Quais são os filmes mais bem avaliados (de acordo com sua nota média por filmes) de cada década (entre a década de 40 e a de 2010)?
    Com essa análise podemos ver os maiores sucessos entre as gerações e as mudanças de gosto em cada gênero ao passar das décadas.

- Qual o top 10 de séries que mais duraram em anos (analisando o ano de lançamento e o de término) e que foram mais bem avaliadas?
    Com essa análise podemos ver quais séries conseguiram manter a qualidade por mais tempo e poder estudar melhor o motivo de seu sucesso continuo através dos anos que ficaram no ar.


# Entrega 1


1. Implementação do códio Python:

Primeiramente criei um bucket no Amazon S3.
![Bucket](/Desafio/etapa-1/evidencias/Screenshot_603.png)

Criei um [código em Python](/Desafio/etapa-1/ingestao.py) e utilizando a biblioteca boto3 criei um cliente S3 para conseguir acessar o Amazon S3.
Nas linhas abaixo o código executará o upload pra o S3 informando quais os arquivos a serem exportados, o bucket que estou utilizando e os diretórios que irei armazena-lo.


2. Criação da imagem no Docker:

Criei o arquivo [dockerfile](/Desafio/etapa-1/dockerfile) para criar uma imagem python em uma versão mais leve e fazer a instalação das dependências necessárias indicadas no arquivo [boto3.txt](/Desafio/etapa-1/boto3.txt) (nesse caso é a instalação da biblioteca boto3) e que faz a cópia dos arquivos atuais no diretório para o diretório da imagem e a execução do código python criado. Fiz a criação da imagem usando o comando ```docker build -t python3-9 .```.


3. Execução do container:

Iniciei um container para executar o arquivo python criado anteriormente usando o comando ```docker run -e AWS_ACCESS_KEY_ID="" -e AWS_SECRET_ACCESS_KEY="" -e AWS_SESSION_TOKEN="" python3-9``` preenchendo-o entre as aspas minhas credencias atualizadas da AWS.

- Arquivo [movies.csv](/Desafio/etapa-1/movies.csv) armazenado no bucket após a execução do container:
    ![Arquivo movies.csv](/Desafio/etapa-1/evidencias/Screenshot_605.png)
- Arquivo [series.csv](/Desafio/etapa-1/series.csv) armazenado no bucket após a execução do container:
    ![Arquivo series.csv](/Desafio/etapa-1/evidencias/Screenshot_606.png)
