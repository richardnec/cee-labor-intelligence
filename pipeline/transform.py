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
    WHERE unit = 'PC_ACT' AND sex = 'T' AND age = 'TOTAL'
        AND value IS NOT NULL AND DATE(date) >= '2000-01-01'
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
    WHERE youth_unemployment_rate IS NOT NULL
    GROUP BY country, DATE(date)
    ORDER BY country, date
    """
    run_query(client, query)


def transform_inflation(client):
    print("🔄 Transformujem infláciu...")
    project = client.project
    query = f"""
    CREATE OR REPLACE TABLE `{project}.labor_market.inflation` AS
    SELECT
        country,
        DATE(date) as date,
        AVG(inflation_rate) as inflation_rate
    FROM `{project}.labor_market.raw_inflation`
    WHERE inflation_rate IS NOT NULL
    GROUP BY country, DATE(date)
    ORDER BY country, date
    """
    run_query(client, query)


def transform_gdp_growth(client):
    print("🔄 Transformujem GDP growth...")
    project = client.project
    query = f"""
    CREATE OR REPLACE TABLE `{project}.labor_market.gdp_growth` AS
    SELECT
        country,
        DATE(date) as date,
        AVG(gdp_growth) as gdp_growth
    FROM `{project}.labor_market.raw_gdp_growth`
    WHERE gdp_growth IS NOT NULL
    GROUP BY country, DATE(date)
    ORDER BY country, date
    """
    run_query(client, query)


def transform_net_wages_pps(client):
    print("🔄 Transformujem čisté mzdy (PPS)...")
    project = client.project
    query = f"""
    CREATE OR REPLACE TABLE `{project}.labor_market.net_wages_pps` AS
    SELECT
        country,
        DATE(date) as date,
        AVG(net_wage_pps) as net_wage_pps
    FROM `{project}.labor_market.raw_net_wages_pps`
    WHERE net_wage_pps IS NOT NULL
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
        i.inflation_rate,
        g.gdp_growth,
        w.net_wage_pps
    FROM `{project}.labor_market.unemployment` u
    LEFT JOIN `{project}.labor_market.youth_unemployment` y
        ON u.country = y.country AND u.date = y.date
    LEFT JOIN `{project}.labor_market.inflation` i
        ON u.country = i.country AND u.date = i.date
    LEFT JOIN `{project}.labor_market.gdp_growth` g
        ON u.country = g.country AND g.date = DATE_TRUNC(u.date, QUARTER)
    LEFT JOIN `{project}.labor_market.net_wages_pps` w
        ON u.country = w.country AND w.date = DATE_TRUNC(u.date, YEAR)
    GROUP BY u.country, u.date, u.unemployment_rate,
             i.inflation_rate, g.gdp_growth, w.net_wage_pps
    ORDER BY u.country, u.date
    """
    run_query(client, query)


if __name__ == "__main__":
    client = get_client()
    transform_unemployment(client)
    transform_youth_unemployment(client)
    transform_inflation(client)
    transform_gdp_growth(client)
    transform_net_wages_pps(client)
    create_master_table(client)
    print("\n✅ Transform layer hotový!")