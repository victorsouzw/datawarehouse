import pandas as pd
import sqlalchemy as sa
import uuid as uuid
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.elements import conv

##conexão com o bd
engine_str = (
    "mysql+pymysql://{user}:{password}@{server}/{database}".format(
        user="user",
        password="pass",
        server="localhost:3306",
        database="test"))
engine = sa.create_engine(engine_str)
conn = engine.connect()

##Define nome das colunas
column_names = [
    "ano",
    "mes",
    "pais",
    "bloco_economico",
    "uf",
    "codigo_sh4",
    "descricao_sh4",
    "codigo_sh2",
    "descricao_sh2",
    "codigo_secao",
    "descricao_secao",
    "valor_fob_dolar",
    "quilograma_liquido"
]
csv_path = ""
print("O arquivo deve estar no mesmo diretório que o arquivo main.py.")
input_path = input("Digite o nome do arquivo CSV: ")
if not input_path:
    csv_path = "EXP_2000_2023_20231027.csv"
else:
    csv_path = input_path
print(csv_path)

##extract
df = pd.read_csv(csv_path, delimiter=";", names=column_names, low_memory=False, header=None,
                 skiprows=1)

dim_sh4 = df.copy(deep=True)[['codigo_sh4', 'descricao_sh4']].drop_duplicates()
dim_sh2 = df.copy(deep=True)[['codigo_sh2', 'descricao_sh2']].drop_duplicates()
dim_secao = df.copy(deep=True)[['codigo_secao', 'descricao_secao']].drop_duplicates()

dim_local = df.copy(deep=True).drop_duplicates()

## o que fazer com o pais? tem q criar um id e depois passar pra fato
#fact = df[["codigo_secao","codigo_sh2", "codigo_sh4", "data", "ID DO PAIS",
 #          "NOME DA ORIGEM","valor_fob_dolar","quilograma_liquido"]]

##carrega os dados no mysql
##posteriormente criar um processo para cada dimensao e dividir em chunks e criar threads

#
# if ("EXP" in csv_path):
#     # CSV de exportacoes
#     fact.to_sql(name='fact_exportacoes', schema='test', con=conn,
#                 if_exists='append', index=False)
#
#
# else:
#     # CSV de importacoes
#     fact.to_sql(name='fact_importacoes', schema='test', con=conn,
#                 if_exists='append', index=False)
def my_to_sql(dim, table_name):
    for i in range(len(df)):
        try:
            dim[i:i+1].to_sql(name=table_name, schema='test', con=conn, if_exists='append', index=False)
        except IntegrityError:
            ##necessario para lidar com o caso de inserir um registro com primary key ja existente
            pass

#Persiste os dados em suas respectivas tabelas
my_to_sql(dim_sh4, "dim_sh4")
my_to_sql(dim_sh2, "dim_sh2")
my_to_sql(dim_secao, "dim_secao")

# check whether connection is Successful or not
if (conn):
    print("MySQL Connection is Successful ... ... ...")
else:
    print("MySQL Connection is not Successful ... ... ...")