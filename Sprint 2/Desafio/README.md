# Etapas


__Banco de Dados Concessionaria.__

![Banco de Dados Concessionaria](../evidencias/concessionaria.sqlite.png)


__Arquivo .SQL com comandos para resolução do Desafio__

[Arquivo .SQL](../Desafio/etapa-1/desafio.sql)



## Etapa 1
- Primeiro fiz a normalização do banco de dados concessionaria para o modelo relacional. Para isso agrupei as colunas de acordo com o seu conteúdo, gerando novas tabelas apartir dos agrupamentos com foco no cliente, vendedor, carro, desgaste do carro, combustível e na locação.

![Etapa I](../Desafio/etapa-1/modelo_relacional.png)


## Etapa 2
- Apartir do modelo relacional, fiz o modelo dimensional, unindo algumas tabelas do modelo anterior(como as tabelas carro, desgaste_carro e combustivel), dividindo algumas(tabela locacao) e alterando a nomeclatura das tabelas de acordo com o padrão desse modelo. Na tabela fato mantive apenas as colunas com informações numéricas.

![Etapa II](../Desafio/etapa-2/modelo_dimensional.png)
