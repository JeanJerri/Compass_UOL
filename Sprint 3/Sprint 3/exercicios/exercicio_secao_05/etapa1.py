atores = []

with open('actors.csv', 'r', encoding='utf-8') as arquivo:
    linhas = arquivo.readlines()

    # 1: para ignorar a primeira linha (cabeçalho)
    for linha in linhas[1:]:
        # strip() remove espaços em branco e quebras de linha
        linha = linha.strip()
        colunas = linha.split(',')

        # Verificar e corrigir se o nome do ator tiver vírgulas
        # adicionais (Robert Downey Jr)
        if len(colunas) > 6:
            ator_nome = ''.join(colunas[:-5])
            ator_nome = ator_nome[1:-1]
            outras_colunas = colunas[-5:]
            colunas = [ator_nome] + outras_colunas
            print(f"Coluna com erro corrigida: {colunas}\n")

        atores.append((colunas[0].strip(), float(colunas[1].strip()),
                       int(colunas[2].strip()), float(colunas[3].strip()),
                       colunas[4].strip(), colunas[5].strip()))

# Ordenar a lista de atores pela coluna número de filmes (Number of Movies)
# em ordem decrescente
atores_ordenados = sorted(atores, key=lambda x: x[2], reverse=True)

for ator in atores_ordenados:
    print(f"Ator/Atriz: {ator[0]}, Qtd. de Filmes: {ator[2]}")

maior_qtd_filmes_ator = atores_ordenados[0][0]
maior_qtd_filmes = atores_ordenados[0][2]
resultado = f"Ator/Atriz com maior número de filmes: {maior_qtd_filmes_ator}\n"
resultado += f"Quantidade de filmes: {maior_qtd_filmes}"

with open('etapa-1.txt', 'w', encoding='utf-8') as arquivotxt:
    arquivotxt.write(resultado)

print(f"A informação foi salva em {'etapa-1.txt'}")
