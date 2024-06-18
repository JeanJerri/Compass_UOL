filmes = []

with open('actors.csv', 'r', encoding='utf-8') as arquivo:
    linhas = arquivo.readlines()

    for linha in linhas[1:]:
        linha = linha.strip()
        colunas = linha.split(',')

        if len(colunas) > 6:
            ator_nome = ''.join(colunas[:-5])
            ator_nome = ator_nome[1:-1]
            outras_colunas = colunas[-5:]
            colunas = [ator_nome] + outras_colunas

        filmes.append((colunas[0].strip(), float(colunas[1].strip()),
                       int(colunas[2].strip()), float(colunas[3].strip()),
                       colunas[4].strip(), colunas[5].strip()))

# Contar quantas vezes o filme aparece
contar_filme = {}
for filme in filmes:
    nome_filme = filme[4]
    if nome_filme in contar_filme:
        contar_filme[nome_filme] += 1
    else:
        contar_filme[nome_filme] = 1

# Adicionar a contagem de repetições em uma nova coluna
filme_contagens = []
for filme in filmes:
    nome_filme = filme[4]
    contagem = contar_filme[nome_filme]
    filme_contagens.append(filme + (contagem,))

# Ordenar a lista de atores pela contagem de aparições em
# ordem decrescente e pelo nome do filme
filmes_sorted = sorted(filme_contagens, key=lambda x: (-x[6], x[4]))

# Usar um dicionário para remover filmes duplicados
filmes_unicos = {}
for linha in filmes_sorted:
    filme = linha[4]
    if filme not in filmes_unicos:
        filmes_unicos[filme] = linha

filmes_unicos_lista = list(filmes_unicos.values())

resultado_lista = ""
for indice, lista in enumerate(filmes_unicos_lista, start=1):
    resultado_lista += f"{indice} - O filme {lista[4]} \
aparece {lista[6]} vez(es) no dataset\n"
    print(f"{lista[4]} aparece {lista[6]} vez(es)")

with open('etapa-4.txt', 'w', encoding='utf-8') as arquivotxt:
    arquivotxt.write(resultado_lista)

print(f"A informação foi salva em {'etapa-4.txt'}")
