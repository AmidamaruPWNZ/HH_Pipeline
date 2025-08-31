import pandas as pd
import requests
import sqlite3
from datetime import datetime



def extract_hh_data():
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": "Data Engineer",
        "search_field": "name",
        "page": 0,
        "per_page": 50,
    }
    headers = {"User-Agent": "HH-User-Agent"}

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

raw_data = extract_hh_data()
print(f"Найдено вакансий: {raw_data['found']}")


def transform_data(raw_data):
    vacancies = []
    for item in raw_data["items"]:
        salary = item.get("salary")
        if salary:
            salary_from = salary.get("from")
            salary_to = salary.get("to")
            currency = salary.get("currency")
        else:
            salary_from = None
            salary_to = None
            currency = None

        vacancy = {
            'hh_id': item["id"],
            'name': item["name"],
            'company_name': item["employer"]["name"],
            'published_at': item["published_at"],
            'salary_from': salary_from,
            'salary_to': salary_to,
            'salary_currency': currency,
            'experience': item["experience"]["name"],
            'employment': item["employment"]["name"],
            'url': item["alternate_url"],
            'api_url': item["url"],
            'created_at': datetime.now().isoformat()
        }
        vacancies.append(vacancy)

    df = pd.DataFrame(vacancies)

    df['published_at'] = pd.to_datetime(df['published_at'])

    return df

cleaned_df = transform_data(raw_data)
print(cleaned_df.head(3))


def create_database():
    conn = sqlite3.connect('hh_vacancies.db')
    cursor = conn.cursor()

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
        api_url TEXT,
        created_at DATETIME
    );
    """

    cursor.execute(create_table_query)
    conn.commit()
    conn.close()


def load_data_to_db(df):
    conn = sqlite3.connect('hh_vacancies.db')

    df.to_sql('vacancies', conn, if_exists='append', index=False)

    conn.close()

create_database()

load_data_to_db(cleaned_df)
print('Данные успешно загружены в базу данных.')


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    try:
        logger.info("Запуск ETL-пайплайна...")

        raw_data = extract_hh_data()
        logger.info(f"Данные получены. Найдено вакансий: {raw_data['found']}")

        df = transform_data(raw_data)
        logger.info(f"Данные преобработаны. Получено {len(df)} записей.")

        create_database()
        load_data_to_db(df)
        logger.info("Пайплайн успешно завершен.")

    except requests.RequestException as e:
        logger.error(f"Произошла ошибка при запросе к API: {e}")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
