import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from sqlalchemy.sql.elements import conv

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
#
#
# ##Define o tipo de dado das colunas
# columns_types = {
#     'Ano': sa.types.INTEGER,
#     'Mes': sa.types.INTEGER,
#     'CodigoSecao': sa.types.TEXT,
#     'DescricaoSecao': sa.types.TEXT,
#     'CodigoSH2': sa.types.INTEGER,
#     'DescricaoSH2': sa.types.TEXT,
#     'CodigoSH4': sa.types.INTEGER,
#     'DescricaoSH4': sa.types.TEXT,
#     'UFdoProduto': sa.types.TEXT,
#     'ValorFOBemDolar': sa.types.BIGINT,
# }
##extract
df = pd.read_csv("EXP_2000_2023_20231027.csv", delimiter=";", names=column_names, low_memory=False, header=None,
                 skiprows=1)
##Nós já alteramos os dados no método read_csv
## `Primeiramente, mudamos o delimitador para utilizar ponto e vírgula, ao invés de virgulas
## e depois fazemos algumas alterações referente ao nome das colunas, onde passamos a variável "names", no caso
## removemos caractéres especiais e espaços em branco

##conexão com o bd
engine_str = (
    "mysql+pymysql://{user}:{password}@{server}/{database}".format(
        user="user",
        password="pass",
        server="localhost:3306",
        database="test"))
engine = sa.create_engine(engine_str)
conn = engine.connect()


dim_sh4 = df.copy(deep=True)[['codigo_sh4', 'descricao_sh4']].drop_duplicates()
dim_sh2 = df.copy(deep=True)[['codigo_sh2', 'descricao_sh2']]
dim_local = df.copy(deep=True)
dim_secao = df.copy(deep=True)[['codigo_secao', 'descricao_secao']]
dim_calendario = df.copy(deep=True)[['ano', 'mes']]

#se fizer sentido nao ter duplicatas, retirar os drop_duplicates
##load our dato to mysql
def my_to_sql(dim, table_name):
    for i in range(len(df)):
        try:
            dim[i:i+1].drop_duplicates().to_sql(name=table_name, schema='test', con=conn, if_exists='append', index=False)
        except IntegrityError:
            pass


#my_to_sql(dim_sh4, "dim_sh4")
#my_to_sql(dim_sh2, "dim_sh2")
#my_to_sql(dim_secao, "dim_secao")

##entender a dimensao calendario: no CSV so temos ano e mês, na tabela aparecem diversas outras
##
my_to_sql(dim_calendario, "dim_calendario")
#dim_sh2[['codigo_sh2', 'descricao_sh2']].drop_duplicates().to_sql(name='dim_sh2', schema='test', con=conn, if_exists='append', index=False)
# dim_local[['UFdoProduto']].to_sql(name='dim_local', schema='backroom', con=conn, if_exists='append', index=False, dtype=columns_types)
# dim_secao[['codigo_secao', 'descricao_secao']].drop_duplicates().to_sql(name='dim_secao', schema='test', con=conn, if_exists='append', index=False)
# dim_calendario[['Ano', 'Mes']].to_sql(name='dim_calendario', schema='backroom', con=conn, if_exists='append', index=False, dtype=columns_types)
#
# fact_exportacoes = df[['Ano', 'Mes', 'CodigoSecao', 'DescricaoSecao', 'CodigoSH2', 'DescricaoSH2', 'CodigoSH4', 'DescricaoSH4', 'UFdoProduto', 'ValorFOBemDolar']]
# fact_exportacoes.to_sql(name='fact_exportacoes', schema='backroom', con=conn, if_exists='append', index=False, dtype=columns_types)

##define o datatype das colunas
#type_cols = ['INTEGER', 'INTEGER', 'TEXT', 'TEXT', 'INTEGER', 'TEXT', 'INTEGER', 'TEXT', 'TEXT','BIGINT'] #Put here the columns types


##retira espaco s e caracteres especiais do nome das colunas
#name_cols = df.columns.tolist()

# first_clean_name = [sub.replace(' ', '') for sub in name_cols]
# cleaned_name_cols = [sub.replace('(US$)', 'emDolar') for sub in first_clean_name]

##cria tabela
#table_config = ', '.join([' '.join(map(str, i)) for i in zip(name_cols, type_cols)])
#conn.execute(text('CREATE TABLE IF NOT EXISTS test_table5 ({})'.format(table_config)))

# check whether connection is Successful or not
if (conn):
    print("MySQL Connection is Successful ... ... ...")
else:
    print("MySQL Connection is not Successful ... ... ...")

# for row in df.tail(-1).iterrows():
#     print(row)
#     sql = "INSERT INTO employee.employee_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#     conn.execute(text(sql), row[1])
#     print("Record inserted") # the connection is not auto committed by default, so we must commit to save our changes conn.commit() except Error as e: print("Error while connecting to MySQL", e)
#     conn.commit()