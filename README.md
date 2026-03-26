# 🌍 CEE Labor Market & Economic Intelligence

A real-time economic analytics dashboard for **Central & Eastern Europe**, built with Python, BigQuery, and Streamlit.

🔗 **[Live Dashboard](https://cee-labor-intelligence-a4bvxaf2agxgvbkamujvcj.streamlit.app/)**

---

## 📊 What It Shows

| Module | Description | Frequency |
|--------|-------------|-----------|
| 📈 Unemployment Rate | Monthly unemployment rate across 8 CEE countries | Monthly |
| 👥 Youth Unemployment | Rate for population under 25 | Monthly |
| 📉 Inflation (HICP) | Harmonized Consumer Price Index, % change | Monthly |
| 📊 GDP Growth | Real GDP growth vs previous quarter, seasonally adjusted | Quarterly |
| 💰 Net Wages (PPS) | Annual net wages in Purchasing Power Standards | Annual |
| 🔮 Unemployment Forecast | Prophet model predictions with 95% confidence intervals | — |
| 🗺️ CEE Map | Choropleth map of current unemployment rates | — |
| 📋 Economic Snapshot | Latest data table across all indicators | — |

**Countries covered:** Slovakia 🇸🇰 · Czechia 🇨🇿 · Poland 🇵🇱 · Hungary 🇭🇺 · Germany 🇩🇪 · Austria 🇦🇹 · Estonia 🇪🇪 · Greece 🇬🇷

---

## 🏗️ Architecture
```
Eurostat API
     │
     ▼
pipeline/ingest.py          # Fetches raw data from Eurostat
     │
     ▼
BigQuery (raw layer)        # raw_unemployment, raw_youth_unemployment,
                            # raw_inflation, raw_gdp_growth, raw_net_wages_pps
     │
     ▼
pipeline/transform.py       # Cleans, aggregates, joins into master table
     │
     ▼
BigQuery (transformed layer) # unemployment, youth_unemployment, inflation,
                             # gdp_growth, net_wages_pps, master
     │
     ▼
pipeline/forecast.py        # Prophet forecasting per country (12 months ahead)
     │
     ▼
BigQuery (forecasts)        # forecasts table
     │
     ▼
app/main.py                 # Streamlit dashboard
```

---

## 🔄 Automated Pipeline

Data is refreshed **daily at 06:00 UTC** via GitHub Actions:

1. Pulls latest data from Eurostat API
2. Loads raw data into BigQuery
3. Runs transformations and rebuilds master table
4. Recalculates Prophet forecasts

**Eurostat update frequency:**
- Unemployment, Youth Unemployment & Inflation → monthly (4–6 week lag)
- GDP Growth → quarterly (2–3 month lag)
- Net Wages (PPS) → annually

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | [Eurostat API](https://ec.europa.eu/eurostat) |
| Data Warehouse | Google BigQuery |
| Pipeline | Python · pandas · eurostat |
| Forecasting | Prophet |
| Dashboard | Streamlit · Plotly |
| Automation | GitHub Actions |
| Hosting | Streamlit Cloud |

---

## 🚀 Run Locally
```bash
# Clone the repo
git clone https://github.com/richardnec/cee-labor-intelligence.git
cd cee-labor-intelligence

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
# Create .env file with:
# GOOGLE_APPLICATION_CREDENTIALS=path/to/your/gcp-key.json

# Run the dashboard
streamlit run app/main.py
```

---

## 📁 Project Structure
```
cee-labor-intelligence/
├── app/
│   └── main.py                    # Streamlit dashboard
├── pipeline/
│   ├── ingest.py                  # Eurostat → BigQuery raw
│   ├── transform.py               # Raw → transformed + master
│   └── forecast.py                # Prophet forecasting
├── .github/
│   └── workflows/
│       └── daily_pipeline.yml     # GitHub Actions automation
├── requirements.txt
└── .gitignore
```

---

## 📄 Data Sources

All data sourced from **[Eurostat](https://ec.europa.eu/eurostat)**.

| Dataset | Eurostat Code | Description |
|---------|--------------|-------------|
| Unemployment Rate | `une_rt_m` | Monthly unemployment rate |
| Youth Unemployment | `une_rt_m` (age: Y_LT25) | Under 25 unemployment |
| Inflation (HICP) | `prc_hicp_mmor` | Harmonized consumer prices |
| GDP Growth | `namq_10_gdp` | Quarterly real GDP growth |
| Net Wages (PPS) | `earn_nt_net` | Annual net wages in PPS |

---

*Built with ❤️ using Python, BigQuery & Streamlit*