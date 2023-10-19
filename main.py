import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

names = ['Ano', 'Mes', 'CodigoSecao', 'DescricaoSecao', 'CodigoSH2', 'DescricaoSH2', 'CodigoSH4', 'DescricaoSH4', 'UFdoProduto', 'ValorFOBemDolar']
columns_types = {
    'Ano': sa.types.INTEGER,
    'Mes': sa.types.INTEGER,
    'CodigoSecao': sa.types.TEXT,
    'DescricaoSecao': sa.types.TEXT,
    'CodigoSH2': sa.types.INTEGER,
    'DescricaoSH2': sa.types.TEXT,
    'CodigoSH4': sa.types.INTEGER,
    'DescricaoSH4': sa.types.TEXT,
    'UFdoProduto': sa.types.TEXT,
    'ValorFOBemDolar': sa.types.BIGINT,
}
##extract
df = pd.read_csv("comex_data.csv", delimiter=";", names=names, low_memory=False, header=None, skiprows=1)
##inside the "read_csv" method we already do some changes at data
## first we change the delimiter to use semicolons, because our data do not use commas
## second we do some changes at column names, removing blank spaces and special characters

##conect to db
engine_str = (
    "mysql+pymysql://{user}:{password}@{server}/{database}".format(
        user="user",
        password="pass",
        server="localhost:3306",
        database="backroom"))
engine = sa.create_engine(engine_str)
conn = engine.connect()

##load our dato to mysql
df.to_sql(name='table_name2', schema='backroom', con=conn, if_exists='append', index=False, dtype=columns_types)

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