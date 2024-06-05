# Inicializar uma lista para armazenar os dados dos filmes
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

filmes_ordenados = sorted(filmes, key=lambda x: x[4])

filmes_unicos = {}
for linha in filmes_ordenados:
    filme = linha[4]
    if filme not in filmes_unicos:
        filmes_unicos[filme] = linha
        print(f"Filme: {linha[4]}, Receita: {linha[5]}")

filmes_unicos_lista = list(filmes_unicos.values())

media = 0.0
for receita in filmes_unicos_lista:
    media += float(receita[5])

media = media/len(filmes_unicos_lista)

resultado = f"Média da receita de bilheteria bruta dos principais filmes dos\
 atores listados (em milhões de doláres): {media:.3f}"

with open('etapa-2.txt', 'w', encoding='utf-8') as arquivotxt:
    arquivotxt.write(resultado)

print(f"A informação foi salva em {'etapa-2.txt'}")
