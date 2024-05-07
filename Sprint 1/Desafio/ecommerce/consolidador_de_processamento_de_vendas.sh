> "relatorio_final.txt"

for relatorio in "vendas/backup"/*.txt; do
    cat "$relatorio" >> "relatorio_final.txt"
done
