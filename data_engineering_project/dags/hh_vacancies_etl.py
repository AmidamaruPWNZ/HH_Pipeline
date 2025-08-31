from datetime import datetime, timedelta
import requests
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import logging
import json
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag = DAG(
    'hh_vacancies_etl',
    default_args=default_args,
    description='ETL pipeline for HH.ru Data Engineer vacancies',
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=['etl', 'hh.ru', 'data_engineering'],
)


def extract_hh_data(**kwargs):
    """Extract data from HH.ru API"""
    execution_date = kwargs['execution_date']
    logging.info(f"Extracting data for date: {execution_date}")

    url = "https://api.hh.ru/vacancies"
    all_vacancies = []

    for page in range(0, 2):  # Берем 2 страницы для начала
        params = {
            'text': 'Data Engineer',
            'search_field': 'name',
            'per_page': 50,
            'page': page,
        }
        headers = {'User-Agent': 'HH-User-Agent'}

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            page_data = response.json()
            all_vacancies.extend(page_data['items'])

            if page >= page_data['pages'] - 1:
                break

        except requests.RequestException as e:
            logging.error(f"Error fetching page {page}: {e}")
            continue

    # Сохраняем сырые данные
    raw_data_dir = Path("/opt/airflow/data/raw")
    raw_data_dir.mkdir(exist_ok=True)

    raw_data_path = raw_data_dir / f"hh_raw_{execution_date.strftime('%Y%m%d')}.json"
    with open(raw_data_path, 'w', encoding='utf-8') as f:
        json.dump(all_vacancies, f, ensure_ascii=False, indent=2)

    logging.info(f"Extracted {len(all_vacancies)} vacancies, saved to {raw_data_path}")
    return all_vacancies


def transform_data(**kwargs):
    """Transform and clean data"""
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']

    all_vacancies = ti.xcom_pull(task_ids='extract_data')

    if not all_vacancies:
        logging.warning("No vacancies data received")
        return None

    vacancies_list = []

    for item in all_vacancies:
        try:
            salary = item.get('salary', {})

            vacancy = {
                'hh_id': item['id'],
                'name': item['name'],
                'company_name': item['employer']['name'],
                'published_at': item['published_at'],
                'salary_from': salary.get('from'),
                'salary_to': salary.get('to'),
                'salary_currency': salary.get('currency'),
                'experience': item['experience']['name'],
                'employment': item['employment']['name'],
                'url': item['alternate_url'],
                'load_date': execution_date.strftime('%Y-%m-%d'),
            }
            vacancies_list.append(vacancy)

        except KeyError as e:
            logging.warning(f"Missing key {e} in vacancy {item.get('id')}")
            continue

    df = pd.DataFrame(vacancies_list)

    if not df.empty:
        df['published_at'] = pd.to_datetime(df['published_at'])

    # Сохраняем преобразованные данные
    processed_dir = Path("/opt/airflow/data/processed")
    processed_dir.mkdir(exist_ok=True)

    transformed_path = processed_dir / f"hh_processed_{execution_date.strftime('%Y%m%d')}.parquet"
    df.to_parquet(transformed_path, index=False)

    logging.info(f"Transformed {len(df)} records, saved to {transformed_path}")
    return transformed_path


def load_to_sqlite(**kwargs):
    """Load data to SQLite database"""
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']

    transformed_path = ti.xcom_pull(task_ids='transform_data')

    if not transformed_path:
        logging.error("No transformed data found")
        return

    df = pd.read_parquet(transformed_path)

    if df.empty:
        logging.warning("No data to load")
        return

    # Создаем базу данных
    db_dir = Path("/opt/airflow/data/database")
    db_dir.mkdir(exist_ok=True)

    db_path = db_dir / "hh_vacancies.db"
    engine = create_engine(f'sqlite:///{db_path}')

    # Создаем таблицу
    create_table_query = """
    CREATE TABLE IF NOT EXISTS vacancies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hh_id TEXT UNIQUE,
        name TEXT,
        company_name TEXT,
        published_at DATETIME,
        salary_from INTEGER,
        salary_to INTEGER,
        salary_currency TEXT,
        experience TEXT,
        employment TEXT,
        url TEXT,
        load_date DATE
    );
    """

    with engine.connect() as conn:
        conn.execute(create_table_query)

    # Загружаем данные
    try:
        df.to_sql('vacancies', engine, if_exists='append', index=False)
        logging.info(f"Successfully loaded {len(df)} records to database")

    except Exception as e:
        logging.error(f"Error loading data to database: {e}")


# Define tasks
start_task = EmptyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_hh_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_to_sqlite,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(task_id='end', dag=dag)

# Set task dependencies
start_task >> extract_task >> transform_task >> load_task >> end_task