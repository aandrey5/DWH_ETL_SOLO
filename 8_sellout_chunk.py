import pandas as pd
import numpy as np
from sqlalchemy import create_engine, String, Integer, Float
# select a source for iterate

# chunk_start = 20
# chunk_fin = 63
# for el in range(chunk_start, chunk_fin):
#     source_name = 'chunk'+str(el+1)+'.csv'
#     print(source_name)
# Divide to many parts as a count of rows in chunk_size
# chunk_size=1080000
# batch_no=1
# for chunk in pd.read_csv('Solopharm.csv',delimiter=";", chunksize=chunk_size, low_memory=False):
#     chunk.to_csv('chunk'+str(batch_no)+'.csv',index=False, sep=";")
#     batch_no+=1
source_name = 'chunk1.csv'
df = pd.read_csv(source_name, delimiter=";", nrows=10)
db_string_airflow = 'postgresql://airflow_xcom:1q2w3e4r5T@10.128.100.98/dwh'

engine = create_engine(db_string_airflow, paramstyle="format")
dtype = {
'date_main' : String(length=50),
'name_contragent_client_card' : String(length=255),
'provider_as' : String(length=255),
'as_point_contagent' : String(length=255),
'region_point_contragent' : String(length=150),
'city_point_contragent' : String(length=150),
'inn_point_contragent' : String(length=50),
'address_point_contragent' : String(length=550),
'address_source_point_contragent' : String(length=550),
'brand_on_promotion' : String(length=100),
'trade_mark' : String(length=100),
'solo_code' : String(length=80),
'sku_name_etalon' : String(length=100),
'sku_name_source' : String(length=150),
'price' : Float,
'purchase_warehouse_q' : Float,
'purchase_sum' : Float,
'purchase_grotex_q' : Float,
'purchase_grotex_sum' : Float,
'sale_to_point_q' : Float,
'sale_to_point_sum' : Float,
'sale_q' : Float,
'sale_sum' : Float,
'stock_q' : Float,
'stock_sum' : Float,
'source' : String(length=150)}
db_col = ['date_main',
'name_contragent_client_card',
'provider_as',
'as_point_contagent',
'region_point_contragent',
'city_point_contragent',
'inn_point_contragent',
'address_point_contragent',
'address_source_point_contragent',
'brand_on_promotion',
'trade_mark',
'solo_code',
'sku_name_etalon',
'sku_name_source',
'price',
'purchase_warehouse_q',
'purchase_sum',
'purchase_grotex_q',
'purchase_grotex_sum',
'sale_to_point_q',
'sale_to_point_sum',
'sale_q',
'sale_sum',
'stock_q',
'stock_sum',

]
df.columns = db_col
# db_float_col = ['price',
# 'purchase_warehouse_q',
# 'purchase_sum',
# 'purchase_grotex_q',
# 'purchase_grotex_sum',
# 'sale_to_point_q',
# 'sale_to_point_sum',
# 'sale_q',
# 'sale_sum',
# 'stock_q',
# 'stock_sum'
# ]

db_float_col = ['price']
df[db_float_col] = df[db_float_col].apply(lambda s: s.str.replace(',', '.'))
# add new column in dataframe with sourcename for next validation
df.loc[:, 'source'] = source_name
# df.to_sql('sellout',index=False,  schema='public', con=engine, if_exists='append', dtype=dtype)
df
