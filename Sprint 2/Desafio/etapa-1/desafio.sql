-- TABLE
CREATE TABLE carro (  
idCarro INT PRIMARY KEY,  
classiCarro VARCHAR(50),  
marcaCarro VARCHAR(50),  
modeloCarro VARCHAR(50),  
anoCarro INT,  
idcombustivel INT,  
FOREIGN KEY (idcombustivel) REFERENCES combustivel(idcombustivel)  
);
CREATE TABLE cliente (  
idCliente INT PRIMARY KEY,  
nomeCliente VARCHAR(100),  
cidadeCliente VARCHAR(50),  
estadoCliente VARCHAR(50),  
paisCliente VARCHAR(50)  
);
CREATE TABLE combustivel (  
idcombustivel INT PRIMARY KEY,  
tipoCombustivel VARCHAR(50)  
);
CREATE TABLE demo (ID integer primary key, Name varchar(20), Hint text );
CREATE TABLE desgaste_carro (  
idCarro INT,  
dataEntrega DATE,  
kmCarro INT,  
PRIMARY KEY (idCarro, dataEntrega),  
FOREIGN KEY (idCarro) REFERENCES carro(idCarro)  
);
CREATE TABLE locacao (  
idLocacao INT PRIMARY KEY,  
idCliente INT,  
idCarro INT,  
idVendedor INT,  
dataLocacao DATE,  
horaLocacao TIME,  
qtdDiaria INT,  
vlrDiaria DECIMAL(10, 2),  
dataEntrega DATE,  
horaEntrega TIME,  
FOREIGN KEY (idCliente) REFERENCES cliente(idCliente),  
FOREIGN KEY (idCarro) REFERENCES carro(idCarro),  
FOREIGN KEY (idVendedor) REFERENCES vendedor(idVendedor)  
);
CREATE TABLE tb_locacao (
  idLocacao       int primary key,
  idCliente       int,
  nomeCliente     varchar(100),
  cidadeCliente   varchar(40),
  estadoCliente   varchar(40),
  paisCliente     varchar(40),
  idCarro         int,
  kmCarro         int,
  classiCarro     varchar(50),
  marcaCarro      varchar(80),
  modeloCarro     varchar(80),
  anoCarro        int,
  idcombustivel   int,
  tipoCombustivel varchar(20),
  dataLocacao     datetime,
  horaLocacao     time,
  qtdDiaria       int,
  vlrDiaria       decimal(18,2),
  dataEntrega     date,
  horaEntrega     time,
  idVendedor      int,
  nomeVendedor    varchar(15),
  sexoVendedor    smallint,
  estadoVendedor  varchar(40)
);
CREATE TABLE vendedor (  
idVendedor INT PRIMARY KEY,  
nomeVendedor VARCHAR(100),  
sexoVendedor CHAR(1),  
estadoVendedor VARCHAR(50)  
);
 
-- INDEX
 
-- TRIGGER
 
-- VIEW
CREATE VIEW dim_carro AS  
SELECT carro.idCarro, carro.classiCarro, carro.marcaCarro, carro.modeloCarro, carro.anoCarro, carro.idcombustivel, desgaste_carro.kmCarro  
FROM carro JOIN desgaste_carro ON carro.idCarro = desgaste_carro.idCarro  
JOIN combustivel ON carro.idCombustivel = combustivel .idCombustivel;
CREATE VIEW dim_cliente AS  
SELECT idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente  
FROM cliente;
CREATE VIEW dim_tempo_entrega AS  
SELECT DISTINCT idLocacao, dataEntrega, horaEntrega, strftime('%Y', dataEntrega) AS diaEntrega, strftime('%Y', dataEntrega) AS mesEntrega, strftime('%Y', dataEntrega) AS anoEntrega  
FROM locacao;
CREATE VIEW dim_tempo_locacao AS  
SELECT DISTINCT idLocacao, dataLocacao, horaLocacao, strftime('%Y', dataLocacao) AS diaLocacao, strftime('%Y', dataLocacao) AS mesLocacao, strftime('%Y', dataLocacao) AS anoLocacao  
FROM locacao;
CREATE VIEW dim_vendedor AS  
SELECT idVendedor, nomeVendedor, sexoVendedor, estadoVendedor  
FROM vendedor;
CREATE VIEW fato_locacao AS  
SELECT locacao.idLocacao, locacao.idCliente, locacao.idCarro, locacao.idVendedor,  
locacao.qtdDiaria, locacao.vlrDiaria  
FROM locacao;
 
