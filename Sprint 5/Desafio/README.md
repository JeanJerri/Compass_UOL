# Etapa I


- Selecionei um arquivo .csv do portal de dados indicado no desafio (https://dados.gov.br/home) onde é registrado as vendas de derivados de petróleo e biocombustíveis feito pela Agência Nacional do Petróleo. Nele, são apresentados os dados referentes à quantidade vendida entre a UF de origem e a de destino informada pelos produtores e o mês em que foram realizadas essas vendas.

    [biodiesel_dadosabertos_csv_vendas.csv](../Desafio/etapa-1/biodiesel_dadosabertos_csv_vendas.csv)

- Fiz a analise do conjunto de dados para elaborar uma consulta com os critérios solicitados no desafio.

    ![Critérios](../Desafio/critérios%20(1).png)
    ![Critérios](../Desafio/critérios%20(2).png)

- Subi o arquivo para um bucket que eu criei para executar a consulta.

    ![Bucket](../evidencias/bucket%20criado.png)

- Na consulta obtive a data em que foi realizado o processamento através da função de data UTCNOW() e fiz sua conversão com o TO_STRING() pra melhor formatação dos dados de data. 

- Com a função de string UPPER() deixei as informações de data em maiúsculo.

- Usei as funções agregadas SUM(), AVG() e COUNT() para fazer a soma do total de produtos vendidos, a média por vendas e a quantidade de vendas, - respectivamente. 

- E também usei o SUM() para calcular a quantidade de vendas nordeste/sudeste junto com uma função condicional em que se somava 1 cada vez que a condição fosse verdadeira.

- Junto as funções agregadas usei a função de conversão CAST() para definir o tipo de dados que estava sendo tratado.

- E fiz um filtro usando operadores lógicos no WHERE para mostrar apenas os registros que tem como origem o Sul ou como destino o nordeste e as que tem como origem o nordeste e destino sudeste, logo, selecionando dados com pelo uma das 3 condições.

- Tudo isso somente em uma consulta: [Consulta_SQL.sql](../Desafio/etapa-1/consulta_sql.sql)

    ![Execução da consulta SQL](../evidencias/execução%20da%20consulta%20SQL.png)
