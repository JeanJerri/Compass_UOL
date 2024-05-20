# Passo a Passo do Desenvolvimento

-Usei o WSL no windowns pra rodar o Linux e conforme o passo a passo indicado no desafio:

1. passo: Baixei o arquivo de dados sobre as vendas, dados_de_vendas.csv.
2. passo: Criei um diretório chamado ecommerce e aloquei o arquivo dados_de_vendas.csv.
3. passo: Criei um arquivo executável chamado processamento_de_vendas.sh para gerar o relatório de vendas, nesse arquivo foi inserido os seguintes comandos:
   1. Criar um diretório chamado vendas e copiar o arquivo dados_de_vendas.csv dentro dele.
   2. Criar um subdiretório chamado backup e copiar o arquivo dados_de_vendas.csv dentro dele renomeando-o como dados-data.csv. "data" será a data do dia em que ele for executado no formato YYYY/MM/DD.
   3. Renomear o arquivo dados-data.csv para backup-dados-data.csv.
   4. No diretório backup, criar um arquivo chamado relatorio.txt tendo como seu conteúdo a data de criação desse relatório, a data do primeiro e do último registro de venda contido no arquivo backup-dados-data.csv, a quantidade total de itens diferentes vendidos e as 10 primeiras linhas do arquivo backup-dados-data.csv.
   5. Comprimir o arquivo backup-dados-data.csv para backup-dados-data.zip.
   6. Apagar o arquivo backup-dados-data.csv do diretório backup e dados_de_vendas.csv do diretório vendas.
4. passo: Agendei a execução do arquivo processamento_de_vendas.sh de segunda a quinta as 15:27 através do crontab.
   ![Comando no crontab](\Sprint%201\evidencias\crontab.png)
5. passo: Alterei os dados de vendas para serem substituídos antes de cada execução do arquivo processamento_de_vendas.sh.
6. passo: Criei um arquivo executável chamado consolidador_de_processamento_de_vendas.sh para unir todos os relatórios gerados e gerar outro arquivo chamado relatorio_final.txt.
