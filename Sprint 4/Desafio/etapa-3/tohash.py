import hashlib


def gerar_hash(input_str):
    sha1 = hashlib.sha1()
    sha1.update(input_str.encode('utf-8'))
    return sha1.hexdigest()


def main():
    while True:
        input_str = input(
            "Digite uma string para gerar um hash ou 'exit' para sair: "
        )

        if input_str.lower() == 'exit':
            print("Encerrando...")
            break

        hash = gerar_hash(input_str)
        print(f"O hash SHA-1 da string '{input_str}' é: {hash}")


if __name__ == "__main__":
    main()
