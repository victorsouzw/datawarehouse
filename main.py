import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.elements import conv

# Conexão com o BD
engine_str = (
    "mysql+pymysql://{user}:{password}@{server}/{database}".format(
        user="user",
        password="pass",
        server="localhost:3306",
        database="test"))
engine = sa.create_engine(engine_str)
conn = engine.connect()

# Define nome das colunas
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
    csv_path = "exp_data.csv"
else:
    csv_path = input_path

# Extract
df = pd.read_csv(csv_path, delimiter=";", names=column_names, low_memory=False, header=None,
                 skiprows=1)

dim_sh4 = df.copy(deep=True)[['codigo_sh4', 'descricao_sh4']].drop_duplicates()
dim_sh2 = df.copy(deep=True)[['codigo_sh2', 'descricao_sh2']].drop_duplicates()
dim_secao = df.copy(deep=True)[['codigo_secao', 'descricao_secao']].drop_duplicates()
dim_pais = df.copy(deep=True)[['pais']].drop_duplicates()

fact = df[['codigo_secao','codigo_sh2', 'codigo_sh4', 'ano', 'mes','valor_fob_dolar','quilograma_liquido', 'pais']]

# Adiciona ID ao dataframe dim_pais
dim_pais['id_pais'] = [x for x in range(len(dim_pais))]

# Adiciona ref de pais ao dataframe fact
fact['id_pais'] = 0

def my_to_sql(dim, table_name):
    for i in range(len(df)):
        try:
            dim[i:i+1].to_sql(name=table_name, schema='test', con=conn, if_exists='append', index=False)
        except IntegrityError:
            pass

#Persiste os dados em suas respectivas tabelas

my_to_sql(dim_sh4, "dim_sh4")
my_to_sql(dim_sh2, "dim_sh2")
my_to_sql(dim_secao, "dim_secao")
my_to_sql(dim_pais, "dim_pais")

if ("EXP" in csv_path.upper()):
    # Exportacoes
    my_to_sql(fact, "fact_exportacoes")
else:
     # Importacoes
     my_to_sql(fact, "fact_importacoes")

print('Dados persistidos')

# check whether connection is Successful or not
if (conn):
    print("MySQL Connection is Successful ... ... ...")
else:
    print("MySQL Connection is not Successful ... ... ...")