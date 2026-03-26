import os
import eurostat
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

CEE_COUNTRIES = ['SK', 'CZ', 'PL', 'HU', 'DE', 'AT', 'EE', 'EL']

def get_bigquery_client():
    return bigquery.Client()

def create_dataset_if_not_exists(client):
    project_id = client.project
    dataset_ref = bigquery.Dataset(f"{project_id}.labor_market")
    dataset_ref.location = "EU"
    try:
        client.create_dataset(dataset_ref)
        print("Dataset labor_market vytvorený")
    except Exception:
        print("Dataset labor_market už existuje")

def load_to_bigquery(df, table_name, client):
    project_id = client.project
    table_id = f"{project_id}.labor_market.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Nahraté do {table_id} — {len(df)} riadkov")

def fetch_unemployment():
    print("📥 Sťahujem nezamestnanosť...")
    df = eurostat.get_data_df('une_rt_m')
    df = df[df['geo\\TIME_PERIOD'].isin(CEE_COUNTRIES)]
    id_cols = [c for c in df.columns if not str(c).startswith('19') and not str(c).startswith('20')]
    df_long = df.melt(id_vars=id_cols, var_name='date', value_name='value')
    df_long = df_long.rename(columns={'geo\\TIME_PERIOD': 'country'})
    df_long['date'] = pd.to_datetime(df_long['date'])
    df_long['metric'] = 'unemployment_rate'
    df_long = df_long.dropna(subset=['value'])
    return df_long[['country', 'date', 'metric', 'value', 'sex', 'age', 'unit']]

def fetch_youth_unemployment():
    print("📥 Sťahujem youth unemployment...")
    df = eurostat.get_data_df('une_rt_m')
    df = df[df['geo\\TIME_PERIOD'].isin(CEE_COUNTRIES)]
    if 'age' in df.columns:
        df = df[df['age'] == 'Y_LT25']
    if 'sex' in df.columns:
        df = df[df['sex'] == 'T']
    if 'unit' in df.columns:
        df = df[df['unit'] == 'PC_ACT']
    id_cols = [c for c in df.columns if not str(c).startswith('19') and not str(c).startswith('20')]
    df_long = df.melt(id_vars=id_cols, var_name='date', value_name='value')
    df_long = df_long.rename(columns={'geo\\TIME_PERIOD': 'country'})
    df_long['date'] = pd.to_datetime(df_long['date'])
    df_long = df_long.dropna(subset=['value'])
    return df_long[['country', 'date', 'value']].rename(columns={'value': 'youth_unemployment_rate'})

def fetch_inflation():
    print("📥 Sťahujem infláciu (HICP)...")
    df = eurostat.get_data_df('prc_hicp_mmor')
    geo_col = [c for c in df.columns if 'geo' in c.lower()][0]
    df = df[df[geo_col].isin(CEE_COUNTRIES)]
    df = df[df['coicop'] == 'CP00']
    if 'unit' in df.columns:
        df = df[df['unit'] == 'RCH_M']
    id_cols = [c for c in df.columns if not str(c).startswith('19') and not str(c).startswith('20')]
    df_long = df.melt(id_vars=id_cols, var_name='date', value_name='value')
    df_long = df_long.rename(columns={geo_col: 'country'})
    df_long['date'] = pd.to_datetime(df_long['date'])
    df_long = df_long.dropna(subset=['value'])
    return df_long[['country', 'date', 'value']].rename(columns={'value': 'inflation_rate'})

def fetch_gdp_growth():
    print("📥 Sťahujem GDP growth...")
    df = eurostat.get_data_df('namq_10_gdp')
    geo_col = [c for c in df.columns if 'geo' in c.lower()][0]
    df = df[df[geo_col].isin(CEE_COUNTRIES)]
    df = df[df['na_item'] == 'B1GQ']
    df = df[df['unit'] == 'CLV_PCH_PRE']
    if 's_adj' in df.columns:
        df = df[df['s_adj'] == 'SCA']
    id_cols = [c for c in df.columns if not str(c).startswith('19') and not str(c).startswith('20')]
    df_long = df.melt(id_vars=id_cols, var_name='date', value_name='value')
    df_long = df_long.rename(columns={geo_col: 'country'})
    df_long['date'] = pd.PeriodIndex(df_long['date'], freq='Q').to_timestamp()
    df_long = df_long.dropna(subset=['value'])
    return df_long[['country', 'date', 'value']].rename(columns={'value': 'gdp_growth'})

def fetch_net_wages_pps():
    print("📥 Sťahujem čisté mzdy (PPS)...")
    df = eurostat.get_data_df('earn_nt_net')
    geo_col = [c for c in df.columns if 'geo' in c.lower()][0]
    df = df[df[geo_col].isin(CEE_COUNTRIES)]
    df = df[(df['currency'] == 'PPS') & (df['estruct'] == 'NET') & (df['ecase'] == 'P1_NCH_AW100')]
    id_cols = [c for c in df.columns if not str(c).startswith('19') and not str(c).startswith('20')]
    df_long = df.melt(id_vars=id_cols, var_name='date', value_name='value')
    df_long = df_long.rename(columns={geo_col: 'country'})
    df_long['date'] = pd.to_datetime(df_long['date'], format='%Y') + pd.offsets.YearBegin(0)
    df_long = df_long.dropna(subset=['value'])
    return df_long[['country', 'date', 'value']].rename(columns={'value': 'net_wage_pps'})

if __name__ == "__main__":
    client = get_bigquery_client()
    create_dataset_if_not_exists(client)

    df_unemployment = fetch_unemployment()
    load_to_bigquery(df_unemployment, 'raw_unemployment', client)

    df_youth = fetch_youth_unemployment()
    load_to_bigquery(df_youth, 'raw_youth_unemployment', client)

    df_inflation = fetch_inflation()
    load_to_bigquery(df_inflation, 'raw_inflation', client)

    df_gdp = fetch_gdp_growth()
    load_to_bigquery(df_gdp, 'raw_gdp_growth', client)

    df_wages_pps = fetch_net_wages_pps()
    load_to_bigquery(df_wages_pps, 'raw_net_wages_pps', client)

    print("\n✅ Všetky dáta nahraté do BigQuery!")