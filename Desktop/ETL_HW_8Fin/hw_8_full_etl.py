from datetime import datetime, timedelta
from airflow.decorators import dag, task
import json
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import pandas as pd
import os
from airflow.models import Variable

# Получаем базовый путь Airflow или используем ваш собственный путь
AIRFLOW_HOME = Variable.get("AIRFLOW_HOME", "/Users/mariannasarkisian/airflow")

# 2. Создайте новый dag;
@dag(
    dag_id="hw_8_etl",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 9, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
def hw_8_etl():
    # 1. Скачайте файлы booking.csv, client.csv и hotel.csv;
    # 3. Создайте три оператора для получения данных и загрузите файлы. Передайте дата фреймы в оператор трансформации;
    @task
    def hw_8_fetch_bookings():
        bookings = pd.read_csv(os.path.join(AIRFLOW_HOME, "dags", "files", "booking.csv"))
        return bookings.to_json()

    @task
    def hw_8_fetch_clients():
        clients = pd.read_csv(os.path.join(AIRFLOW_HOME, "dags", "files", "client.csv"))
        return clients.to_json()

    @task
    def hw_8_fetch_hotels():
        hotels = pd.read_csv(os.path.join(AIRFLOW_HOME, "dags", "files", "hotel.csv"))
        return hotels.to_json()

    # 4. Создайте оператор который будет трансформировать данные:
    # — Объедините все таблицы в одну;
    # — Приведите даты к одному виду;
    # — Удалите невалидные колонки;
    # — Приведите все валюты к одной;
    @task
    def hw_8_transform(**kwargs):
        ti = kwargs['ti']
        xcom_bookings = ti.xcom_pull(task_ids="hw_8_fetch_bookings")
        xcom_hotels = ti.xcom_pull(task_ids="hw_8_fetch_hotels")
        xcom_clients = ti.xcom_pull(task_ids="hw_8_fetch_clients")

        # Parse JSON to DataFrames
        booking = pd.read_json(xcom_bookings)
        hotel = pd.read_json(xcom_hotels)
        client = pd.read_json(xcom_clients)

        client['age'].fillna(client['age'].mean(), inplace=True)
        client['age'] = client['age'].astype(int)

        # Merge booking with client
        data = pd.merge(booking, client, on='client_id')
        data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)

        # Merge booking, client & hotel
        data = pd.merge(data, hotel, on='hotel_id')
        data.rename(columns={'name': 'hotel_name'}, inplace=True)

        # Format dates
        data['booking_date'] = pd.to_datetime(data['booking_date']).dt.strftime('%Y-%m-%d')

        # Convert all costs to GBP (assuming exchange rate from EUR to GBP is 0.8)
        data.loc[data.currency == 'EUR', 'booking_cost'] *= 0.8
        data['currency'].replace("EUR", "GBP", inplace=True)

        # Drop unnecessary columns
        data = data.drop('address', axis=1)

        # Сохранение трансформированных данных в CSV файл
        file_path = os.path.join(AIRFLOW_HOME, "dags", "files", "processed_data.csv")
        data.to_csv(file_path, index=False)

    # 5. Создайте оператор загрузки в базу данных;
    @task
    def hw_8_load_data():
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        c = conn.cursor()
        c.execute('''
                    CREATE TABLE IF NOT EXISTS hw_8_booking_record (
                        client_id INTEGER NOT NULL,
                        booking_date TEXT NOT NULL,
                        room_type TEXT NOT NULL,
                        hotel_id INTEGER NOT NULL,
                        booking_cost NUMERIC,
                        currency TEXT,
                        age INTEGER,
                        client_name TEXT,
                        client_type TEXT,
                        hotel_name TEXT
                    );
                 ''')
        # Загрузка данных в базу
        file_path = os.path.join(AIRFLOW_HOME, "dags", "files", "processed_data.csv")
        with open(file_path, "r") as file:
            c.copy_expert(
                "COPY hw_8_booking_record FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    # Зависимости между задачами
    [hw_8_fetch_bookings(), hw_8_fetch_hotels(), hw_8_fetch_clients()] >> hw_8_transform() >> hw_8_load_data()

# 6. Запуск dag.
dag = hw_8_etl()


#/Users/mariannasarkisian/airflow/logs/
#airflow  webserver
#airflow scheduler
#airflow scheduler --daemon
#airflow webserver --daemon