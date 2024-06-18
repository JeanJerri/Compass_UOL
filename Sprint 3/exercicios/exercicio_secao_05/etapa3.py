atores = []

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

        atores.append((colunas[0].strip(), float(colunas[1].strip()),
                       int(colunas[2].strip()), float(colunas[3].strip()),
                       colunas[4].strip(), colunas[5].strip()))

atores_ordenados = sorted(atores, key=lambda x: x[3], reverse=True)

for ator in atores_ordenados:
    print(f"Ator/Atriz: {ator[0]}, Média por Filme: {ator[3]}")

maior_media_ator = atores_ordenados[0][0]
maior_media = atores_ordenados[0][3]
resultado = f"Ator/Atriz com maior média de receita de bilheteria bruta \
por filme: {maior_media_ator}\n"
resultado += f"Receita média por filme(em milhões de doláres): {maior_media}"

with open('etapa-3.txt', 'w', encoding='utf-8') as arquivotxt:
    arquivotxt.write(resultado)

print(f"A informação foi salva em {'etapa-3.txt'}")
