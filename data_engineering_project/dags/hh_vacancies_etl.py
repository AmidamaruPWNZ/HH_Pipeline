from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text
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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': days_ago(1),
}


def extract_hh_data(**kwargs):
    """Extract data from HH.ru API"""
    ti = kwargs['ti']
    execution_date = kwargs['logical_date']
    logging.info(f"Extracting data for date: {execution_date}")

    try:
        url = "https://api.hh.ru/vacancies"
        all_vacancies = []

        # Берем только первую страницу для теста
        params = {
            'text': 'Data Engineer',
            'search_field': 'name',
            'per_page': 20,  # Уменьшаем для теста
            'page': 0,
        }
        headers = {'User-Agent': 'HH-User-Agent'}

        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        page_data = response.json()
        all_vacancies.extend(page_data['items'])

        # Сохраняем сырые данные
        raw_data_dir = Path("/opt/airflow/data/raw")
        raw_data_dir.mkdir(parents=True, exist_ok=True)

        raw_data_path = raw_data_dir / f"hh_raw_{execution_date.strftime('%Y%m%d')}.json"
        with open(raw_data_path, 'w', encoding='utf-8') as f:
            json.dump(all_vacancies, f, ensure_ascii=False, indent=2)

        logging.info(f"Successfully extracted {len(all_vacancies)} vacancies")
        return all_vacancies

    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        raise


def transform_data(**kwargs):
    """Transform and clean data"""
    ti = kwargs['ti']
    execution_date = kwargs['logical_date']

    try:
        all_vacancies = ti.xcom_pull(task_ids='extract_data')

        if not all_vacancies:
            logging.warning("No vacancies data received")
            return None

        vacancies_list = []

        for item in all_vacancies:
            try:
                salary = item.get('salary', {})

                vacancy = {
                    'hh_id': item.get('id'),
                    'name': item.get('name'),
                    'company_name': item.get('employer', {}).get('name'),
                    'published_at': item.get('published_at'),
                    'salary_from': salary.get('from'),
                    'salary_to': salary.get('to'),
                    'salary_currency': salary.get('currency'),
                    'experience': item.get('experience', {}).get('name'),
                    'employment': item.get('employment', {}).get('name'),
                    'url': item.get('alternate_url'),
                    'load_date': execution_date.strftime('%Y-%m-%d'),
                }
                vacancies_list.append(vacancy)

            except Exception as e:
                logging.warning(f"Error processing vacancy: {e}")
                continue

        df = pd.DataFrame(vacancies_list)

        if not df.empty and 'published_at' in df.columns:
            df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')

        # Сохраняем преобразованные данные
        processed_dir = Path("/opt/airflow/data/processed")
        processed_dir.mkdir(parents=True, exist_ok=True)

        transformed_path = processed_dir / f"hh_processed_{execution_date.strftime('%Y%m%d')}.parquet"
        df.to_parquet(transformed_path, index=False)

        logging.info(f"Successfully transformed {len(df)} records")
        return str(transformed_path)

    except Exception as e:
        logging.error(f"Transformation failed: {str(e)}")
        raise


def load_to_sqlite(**kwargs):
    """Load data to SQLite database"""
    ti = kwargs['ti']

    try:
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
        db_dir.mkdir(parents=True, exist_ok=True)

        db_path = db_dir / "hh_vacancies.db"
        engine = create_engine(f'sqlite:///{db_path}')

        # Создаем таблицу
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hh_id TEXT,
                    name TEXT,
                    company_name TEXT,
                    published_at DATETIME,
                    salary_from INTEGER,
                    salary_to INTEGER,
                    salary_currency TEXT,
                    experience TEXT,
                    employment TEXT,
                    url TEXT,
                    load_date DATE,
                    UNIQUE(hh_id, load_date)
                )
            """))
            conn.commit()

        # Загружаем данные
        df.to_sql('vacancies', engine, if_exists='append', index=False, method='multi')
        logging.info(f"Successfully loaded {len(df)} records to database")

    except Exception as e:
        logging.error(f"Loading failed: {str(e)}")
        raise


with DAG(
        'hh_vacancies_etl',
        default_args=default_args,
        description='ETL pipeline for HH.ru Data Engineer vacancies',
        schedule_interval='0 10 * * *',
        catchup=False,
        tags=['etl', 'hh.ru', 'data_engineering'],
) as dag:
    start_task = EmptyOperator(task_id='start')

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_hh_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_sqlite,
        provide_context=True,
    )

    end_task = EmptyOperator(task_id='end')

    start_task >> extract_task >> transform_task >> load_task >> end_task