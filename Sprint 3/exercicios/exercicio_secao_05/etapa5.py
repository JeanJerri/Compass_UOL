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

atores_ordenados = sorted(atores, key=lambda x: x[1], reverse=True)

resultado_lista = ""
for ator in atores_ordenados:
    resultado_lista += f"{ator[0]} - {ator[1]}\n"
    print(f"Ator/Atriz: {ator[0]}, \
Receita Total: {ator[1]}")

with open('etapa-5.txt', 'w', encoding='utf-8') as arquivotxt:
    arquivotxt.write(resultado_lista)

print(f"A informação foi salva em {'etapa-5.txt'}")
