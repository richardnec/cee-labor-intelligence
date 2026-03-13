import pandas as pd
from prophet import Prophet
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

CEE_COUNTRIES = ['SK', 'CZ', 'PL', 'HU', 'DE', 'AT', 'EE', 'EL']

def get_client():
    return bigquery.Client()

def load_unemployment(client):
    print("📥 Načítavam dáta z BigQuery...")
    query = f"""
    SELECT country, date, unemployment_rate
    FROM `{client.project}.labor_market.unemployment`
    ORDER BY country, date
    """
    return client.query(query).to_dataframe()

def forecast_country(df_country, country, periods=12):
    print(f"  🔮 Forecasting {country}...")
    
    # Prophet potrebuje stlpce 'ds' a 'y'
    df_prophet = df_country[['date', 'unemployment_rate']].copy()
    df_prophet.columns = ['ds', 'y']
    df_prophet['ds'] = pd.to_datetime(df_prophet['ds'])
    df_prophet = df_prophet.dropna()
    
    if len(df_prophet) < 24:
        print(f"  ⚠️ {country} má málo dát, preskakujem")
        return None
    
    # Model
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        interval_width=0.95
    )
    model.fit(df_prophet)
    
    # Forecast
    future = model.make_future_dataframe(periods=periods, freq='MS')
    forecast = model.predict(future)
    
    # Pridaj country
    forecast['country'] = country
    
    return forecast[['ds', 'country', 'yhat', 'yhat_lower', 'yhat_upper']]

def save_forecasts(client, df_forecast):
    print("📤 Nahrávam forecasty do BigQuery...")
    table_id = f"{client.project}.labor_market.forecasts"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(df_forecast, table_id, job_config=job_config)
    job.result()
    print(f"✅ Nahraté {len(df_forecast)} riadkov do {table_id}")

if __name__ == "__main__":
    client = get_client()
    df = load_unemployment(client)
    
    all_forecasts = []
    for country in CEE_COUNTRIES:
        df_country = df[df['country'] == country]
        forecast = forecast_country(df_country, country, periods=12)
        if forecast is not None:
            all_forecasts.append(forecast)
    
    df_all = pd.concat(all_forecasts, ignore_index=True)
    df_all = df_all.rename(columns={'ds': 'date'})
    
    save_forecasts(client, df_all)
    print("\n✅ Forecasting hotový!")