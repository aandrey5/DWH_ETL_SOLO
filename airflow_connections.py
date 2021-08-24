from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import json
import os
import datetime as dt
import pandas as pd
import numpy as np
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, String, Integer, Float
from time import sleep
import logging
import psycopg2

connection_input = BaseHook.get_connection("airflow_connection")


@dag(default_args={'owner': 'MorozovAN'}, 
  schedule_interval=None,
  tags=['ds'], 
  start_date=days_ago(0))

def Task10_test():

  @task()
  def test_connection():

    logging.info(f'postgresql://{connection_input.login}:{connection_input.password}@{connection_input.host}/{connection_input.schema}')
    return 1


  test_connection()

        #sleep(1)

Taskflow = Task10_test()
