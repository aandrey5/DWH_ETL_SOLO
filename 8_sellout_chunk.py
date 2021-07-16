from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import json
import os
import datetime as dt
import pandas as pd
import numpy as np
from airflow.models import Variable
from sqlalchemy import create_engine, String, Integer, Float
from time import sleep
import logging


@dag(default_args={'owner': 'MorozovAN'}, schedule_interval=None, start_date=days_ago(2))

def Task9_Sellout_to_Dwh_chunk3():

    conf = {'chunk_start': 4, 'chunk_fin': 30}

    for el in range(conf['chunk_start'] - 1, conf['chunk_fin']):
        source_name = 'chunk' + str(el + 1) + '.csv'


        @task()
        def push_to_postgresql(sourcename):

            logging.info(f'////////// {sourcename} [[[[[[[[[[[[[[[[[[[[[[[[[')


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
                'source' : String(length=150)
            }

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
                      'stock_sum'
                      ]

            db_float_col = ['price']

            # create sql engine for sqlalchemy
            db_string_airflow = 'postgresql://airflow_xcom:1q2w3e4r5T@10.128.100.98/dwh'
            engine = create_engine(db_string_airflow, paramstyle="format")
            # read dataframe
            df = pd.read_csv(f'/home/solopharm/airflow/sales_data/{sourcename}',
                             low_memory=False, delimiter=";")

            # Transform DF
            df.columns = db_col
            df[db_float_col] = df[db_float_col].apply(lambda s: s.str.replace(',', '.'))

            # Add new DQ column for future control
            df.loc[:, 'source'] = sourcename

            # push data to postgresql
            df.to_sql('sellout', index=False, schema='public', con=engine, if_exists='append', dtype=dtype)


        push_to_postgresql(source_name)

        sleep(1)

Taskflow = Task9_Sellout_to_Dwh_chunk3()
