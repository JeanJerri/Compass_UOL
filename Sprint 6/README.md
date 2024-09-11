# Exercícios


1. Lab AWS S3

- Etapa 1: Criar um bucket
    ![Bucket criado](./evidencias/01%20-%20S3/Screenshot_525.png)
- Etapa 2: Habilitar hospedagem de site estático
    ![Hospedagem estática habilitada](./evidencias/01%20-%20S3/Screenshot_527.png)
- Etapa 3: Editar as configurações do Bloqueio de acesso público
    ![Configurações do Bloqueio de acesso público](./evidencias/01%20-%20S3/Screenshot_683.png)
- Etapa 4: Adicionar política de bucket que torna o conteúdo do bucket publicamente disponível
    ![Política de bucket que torna o conteúdo do bucket publicamente disponível](./evidencias/01%20-%20S3/Screenshot_529.png)
- Etapa 5: Configurar um documento de índice
    ![Upload arquivo index.html](./evidencias/01%20-%20S3/Screenshot_531.png)
    [arquivo index.html](./exercicios/Arquivos%20no%20bucket/index.html)
    ![Upload arquivo nomes.csv](./evidencias/01%20-%20S3/Screenshot_534.png)
    [arquivo nomes.csv](./exercicios/Arquivos%20no%20bucket/dados/nomes.csv)
- Etapa 6: Configurar documento de erros
    ![Upload arquivo 404.html](./evidencias/01%20-%20S3/Screenshot_536.png)
    [arquivo 404.html](./exercicios/Arquivos%20no%20bucket/404.html)
- Etapa 7: Testar o endpoint do site
    ![Site hospedado no Amazon S3](./evidencias/01%20-%20S3/Screenshot_540.png)


2. Lab AWS Athena

- Etapa 1: Configurar Athena
    ![Pasta criada](./evidencias/02%20-%20Athena/Screenshot_542.png)
    ![Configuração Athena](./evidencias/02%20-%20Athena/Screenshot_683.png)
- Etapa 2: Criar um banco de dados
    ![Query para criar um banco de dados](./evidencias/02%20-%20Athena/Screenshot_544.png)
- Etapa 3: Criar uma tabela
    ![Query para criar uma tabela](./evidencias/02%20-%20Athena/Screenshot_548.png)
    - Testar dados
        ![Testar dados](./evidencias/02%20-%20Athena/Screenshot_550.png)
        ![Resultado](./evidencias/02%20-%20Athena//Screenshot_552.png)
    - Consulta que lista os 3 nomes mais usados em cada década desde o 1950 até hoje
        ![Consulta que lista os 3 nomes mais usados em cada década desde o 1950 até hoje](./evidencias/02%20-%20Athena/Screenshot_558.png)
        ![Resultado](./evidencias/02%20-%20Athena/Screenshot_556.png)


3. Lab AWS Lambda

- Etapa 1: Criar a função do Lambda
    ![Função do Lambda criada](./evidencias/03%20-%20Lambda/Screenshot_560.png)
- Etapa 2: Construir o código
    ![Código atualizado](./evidencias/03%20-%20Lambda/Screenshot_561.png)
    ![Test](./evidencias/03%20-%20Lambda/Screenshot_563.png)
- Etapa 3: Criar uma Layer
    - Arquivo Dockerfile e instalação da imagem
        ![Arquivo Dockerfile e instalação da imagem](./evidencias/03%20-%20Lambda/Screenshot_584.png)
    - Execução do container e criação de pastas
        ![Executar container e criar pastas](./evidencias/03%20-%20Lambda/Screenshot_585.png)
    - Instalando a biblioteca Pandas
        ![Instalando a biblioteca Pandas](./evidencias/03%20-%20Lambda/Screenshot_586.png)
    - Arquivo comprimido
        ![Arquivo comprimido](./evidencias/03%20-%20Lambda/Screenshot_587.png)
    - Copiando arquivo para máquina local
        ![Copiando arquivo para máquina local](./evidencias/03%20-%20Lambda/Screenshot_589.png)
    - Upload do arquivo zipado
        ![Upload do arquivo zipado](./evidencias/03%20-%20Lambda/Screenshot_599.png)
    [Arquivo zipado](./exercicios/Arquivos%20no%20bucket/libs/minha-camada-pandas.zip)
    - Criação da camada
        ![Criação da camada](./evidencias/03%20-%20Lambda/Screenshot_578.png)
        ![Camada criada](./evidencias/03%20-%20Lambda/Screenshot_579.png)
- Etapa 4: Utilizando a Layer
    ![Camada adicionada](./evidencias/03%20-%20Lambda/Screenshot_591.png)
    ![Código executado com o Test](./evidencias/03%20-%20Lambda/Screenshot_597.png)


4. Lab AWS - Limpeza de recursos

![Exclusão de arquivos do bucket](./evidencias/04%20-%20Lab%20AWS%20-%20Limpeza%20de%20recursos/Screenshot_683.png)
![Exclusão do bucket](.//evidencias/04%20-%20Lab%20AWS%20-%20Limpeza%20de%20recursos/Screenshot_684.png)


# Desafio


- [Entrega do Desafio](../Sprint%206/Desafio/README.md)


# Certificados


- Certificado do Curso Noções básicas de Analytics na AWS – Parte 1 (Português)  Fundamentals of Analytics on AWS – Part 1 (Portuguese)
[Noções básicas de Analytics na AWS – Parte 1 (Português)  Fundamentals of Analytics on AWS – Part 1 (Portuguese)](../Sprint%206/certificados/Noções%20básicas%20de%20Analytics%20na%20AWS%20–%20Parte%201%20(Português)%20%20Fundamentals%20of%20Analytics%20on%20AWS%20–%20Part%201%20(Portuguese).pdf)

- Certificado do Curso Fundamentos de analytics na AWS – Parte 2 (Português)  Fundamentals of Analytics on AWS – Part 2 (Portuguese)
[Fundamentos de analytics na AWS – Parte 2 (Português)  Fundamentals of Analytics on AWS – Part 2 (Portuguese)](../Sprint%206/certificados/Fundamentos%20de%20analytics%20na%20AWS%20–%20Parte%202%20(Português)%20%20Fundamentals%20of%20Analytics%20on%20AWS%20–%20Part%202%20(Portuguese).pdf)

- Certificado do Curso Serverless Analytics (Portuguese)
[Serverless Analytics (Portuguese)](../Sprint%206/certificados/Serverless%20Analytics%20(Portuguese).pdf)

- Certificado do Curso Introduction to Amazon Athena (Portuguese)
[Introduction to Amazon Athena (Portuguese)](../Sprint%206/certificados/Introduction%20to%20Amazon%20Athena%20(Portuguese).pdf)

- Certificado do Curso AWS Glue Getting Started
[AWS Glue Getting Started](../Sprint%206/certificados/AWS%20Glue%20Getting%20Started.pdf)

- Certificado do Curso Amazon EMR Getting Started
[Amazon EMR Getting Started](../Sprint%206/certificados//Amazon%20EMR%20Getting%20Started.pdf)

- Certificado do Curso Amazon Redshift Getting Started
[Amazon Redshift Getting Started](../Sprint%206/certificados/Amazon%20Redshift%20Getting%20Started.pdf)

- Certificado do Curso Best Practices for Data Warehousing with Amazon Redshift (Portuguese)
[Best Practices for Data Warehousing with Amazon Redshift (Portuguese)](../Sprint%206/certificados/Best%20Practices%20for%20Data%20Warehousing%20with%20Amazon%20Redshift%20(Portuguese).pdf)

- Certificado do Curso Amazon QuickSight - Getting Started
[Amazon QuickSight - Getting Started](../Sprint%206/certificados/Amazon%20QuickSight%20-%20Getting%20Started.pdf)

- Certificado do Data & Analytics 6/10
[Data & Analytics 6/10](./certificados/Data%20&%20Analytics%206.pdf)
