import pandas as pd
from datetime import timedelta, datetime
from dateutil import rrule
from airflow import DAG

import logging

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 5, 1),
    'end_date': datetime(2022, 5, 5),
    'owner': 'Alexey',
    'poke_interval': 600
}

with DAG("taxi",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['taxi','green']) as dag:


    # def fucking_date(*args):
    #    date_object = datetime.strptime(args[0], "%Y-%m-%d")
    #    return f'Mother {date_object}'

    # fucking_date = PythonOperator(
    #     task_id='fucking_date',
    #     python_callable=fucking_date,
    #     op_args=['{{ds}}'],
    #     dag=dag
    # )


    def load_taxi(*args):
        date = datetime.strptime(args[0], "%Y-%m-%d")
        first = pd.to_datetime(date).date()

        #Read dataset 
        data = pd.read_parquet(f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{date.strftime("%Y")}-{date.strftime("%m")}.parquet', engine='pyarrow')

        # Create date column wo time
        data['trip_date'] = pd.to_datetime(data['lpep_pickup_datetime']).dt.date

        # Filter data
        date_filter = data.loc[:,'trip_date']  == first
        data = data.loc[date_filter]
        
        #to_csv
        data.to_csv(f'/tmp/green_taxi_{date.strftime("%Y")}-{date.strftime("%m")}-{date.strftime("%d")}.csv',sep=',',encoding='utf-8',index=False,header=False)

    load_taxi = PythonOperator(
        task_id='load_taxi',
        python_callable=load_taxi,
        op_args=['{{ds}}'],
        dag=dag
    )


    def load_csv_to_gp_func(*args):
        date = datetime.strptime(args[0], "%Y-%m-%d")
        pg_hook = PostgresHook('Dataset')
        # TODO: Мёрдж или очищение предыдущего батча
        pres = "','"
        sql = f'COPY green_taxi FROM STDIN DELIMITER {pres}'
        file = f'/tmp/green_taxi_{date.strftime("%Y")}-{date.strftime("%m")}-{date.strftime("%d")}.csv'
        pg_hook.copy_expert(sql, file)


    load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    op_args=['{{ds}}'],
    dag=dag
        )


    load_taxi >> load_csv_to_gp