mkdir vendas
cp dados_de_vendas.csv vendas/
mkdir vendas/backup
data=$(date +%Y%m%d)
cp dados_de_vendas.csv vendas/backup/dados-$data.csv
cp vendas/backup/dados-$data.csv vendas/backup/backup-dados-$data.csv
rm vendas/backup/dados-$data.csv

echo "Relatório gerado em: $(date +'%Y/%m/%d %H:%M')" > vendas/backup/relatorio-$data.txt
echo " " >> vendas/backup/relatorio-$data.txt

primeira_data=$(sed -n '2p' vendas/backup/backup-dados-$data.csv | cut -d ',' -f 5)
ultima_data=$(tail -n 1 vendas/backup/backup-dados-$data.csv | cut -d ',' -f 5)
quantidade_itens=$(cut -d ',' -f 2 vendas/backup/backup-dados-$data.csv | sort | uniq | wc -l)

echo "Data do primeiro registro de venda: $primeira_data" >> vendas/backup/relatorio-$data.txt
echo " " >> vendas/backup/relatorio-$data.txt
echo "Data do último registro de venda: $ultima_data" >> vendas/backup/relatorio-$data.txt
echo " " >> vendas/backup/relatorio-$data.txt
echo "Quantidade total de itens diferentes vendidos: $quantidade_itens" >> vendas/backup/relatorio-$data.txt
echo " " >> vendas/backup/relatorio-$data.txt
echo "Primeiras 10 linhas do arquivo backup-dados-$data.csv:" >> vendas/backup/relatorio-$data.txt
head -n 10 vendas/backup/backup-dados-$data.csv >> vendas/backup/relatorio-$data.txt

zip vendas/backup/backup-dados-$data.zip vendas/backup/backup-dados-$data.csv
rm vendas/backup/backup-dados-$data.csv vendas/dados_de_vendas.csv

echo " " >> vendas/backup/relatorio-$data.txt
echo " " >> vendas/backup/relatorio-$data.txt
