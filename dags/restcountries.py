from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import requests
import logging

# Airflow에 등록된 Connection 정보를 통해 Redshift 연결
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


# Extract + Transform
@task
def get_counrty_list(url):
    context = requests.get(url).json()

    country_list = []
    for country in context:
        country_list.append([country['name']['official'], country['population'], country['area']])
    
    return country_list

# Load
@task
def load(schema, table, country_list):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    country text,
    population bigint,
    area bigint,
);""")

        for c in country_list:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{c[0]}', {c[1]}, {c[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'restCountries',
    start_date = datetime(2023,6,7),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6' # 토요일 오전 6시 30분마다
) as dag:

    url = "https://restcountries.com/v3/all"
    country_list = get_counrty_list(url) # restcounty의 url
    load("sunhokim_public", "country_info", country_list)