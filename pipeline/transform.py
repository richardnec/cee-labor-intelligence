import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()


def get_client():
    return bigquery.Client()


def run_query(client, query):
    job = client.query(query)
    job.result()
    print("✅ Hotovo")


def transform_unemployment(client):
    print("🔄 Transformujem nezamestnanosť...")
    project = client.project
    query = f"""
    CREATE OR REPLACE TABLE `{project}.labor_market.unemployment` AS
    SELECT
        country,
        DATE(date) as date,
        AVG(value) AS unemployment_rate
    FROM `{project}.labor_market.raw_unemployment`
    WHERE
        unit = 'PC_ACT'
        AND sex = 'T'
        AND age = 'TOTAL'
        AND value IS NOT NULL
        AND DATE(date) >= '2000-01-01'
    GROUP BY country, DATE(date)
    ORDER BY country, date
    """
    run_query(client, query)


def transform_youth_unemployment(client):
    print("🔄 Transformujem youth unemployment...")
    project = client.project
    query = f"""
    CREATE OR REPLACE TABLE `{project}.labor_market.youth_unemployment` AS
    SELECT
        CAST(country AS STRING) as country,
        DATE(date) as date,
        AVG(youth_unemployment_rate) as youth_unemployment_rate
    FROM `{project}.labor_market.raw_youth_unemployment`
    WHERE
        youth_unemployment_rate IS NOT NULL
    GROUP BY country, DATE(date)
    ORDER BY country, date
    """
    run_query(client, query)


def transform_job_vacancies(client):
    print("🔄 Transformujem job vacancies...")
    project = client.project
    query = f"""
    CREATE OR REPLACE TABLE `{project}.labor_market.job_vacancies` AS
    SELECT
        country,
        DATE(date) as date,
        AVG(job_vacancy_rate) as job_vacancy_rate
    FROM `{project}.labor_market.raw_job_vacancies`
    WHERE
        job_vacancy_rate IS NOT NULL
    GROUP BY country, DATE(date)
    ORDER BY country, date
    """
    run_query(client, query)


def transform_wages(client):
    print("🔄 Transformujem mzdy...")
    project = client.project
    query = f"""
    CREATE OR REPLACE TABLE `{project}.labor_market.wages` AS
    SELECT
        country,
        DATE(date) as date,
        AVG(labor_cost_index) as labor_cost_index
    FROM `{project}.labor_market.raw_wages`
    WHERE
        labor_cost_index IS NOT NULL
    GROUP BY country, DATE(date)
    ORDER BY country, date
    """
    run_query(client, query)


def create_master_table(client):
    print("🔄 Vytváram master tabuľku...")
    project = client.project
    query = f"""
    CREATE OR REPLACE TABLE `{project}.labor_market.master` AS
    SELECT
        u.country,
        u.date,
        u.unemployment_rate,
        AVG(y.youth_unemployment_rate) as youth_unemployment_rate,
        j.job_vacancy_rate,
        AVG(w.labor_cost_index) as labor_cost_index
    FROM `{project}.labor_market.unemployment` u
    LEFT JOIN `{project}.labor_market.youth_unemployment` y
        ON u.country = y.country AND u.date = y.date
    LEFT JOIN `{project}.labor_market.job_vacancies` j
        ON u.country = j.country
        AND j.date = DATE_TRUNC(u.date, QUARTER)
    LEFT JOIN `{project}.labor_market.wages` w
        ON u.country = w.country AND u.date = w.date
    GROUP BY u.country, u.date, u.unemployment_rate, j.job_vacancy_rate
    ORDER BY u.country, u.date
    """
    run_query(client, query)


if __name__ == "__main__":
    client = get_client()
    transform_unemployment(client)
    transform_youth_unemployment(client)
    transform_job_vacancies(client)
    transform_wages(client)
    create_master_table(client)
    print("\n✅ Transform layer hotový!")