# 🌍 CEE Labor Market Intelligence

A real-time labor market analytics dashboard for **Central & Eastern Europe**, built with Python, BigQuery, and Streamlit.

🔗 **[Live Dashboard](https://cee-labor-intelligence-a4bvxaf2agxgvbkamujvcj.streamlit.app/)**

---

## 📊 What It Shows

| Module | Description |
|--------|-------------|
| 📈 Unemployment Rate | Monthly unemployment rate across 8 CEE countries |
| 👥 Youth Unemployment | Rate for population under 25 |
| 💼 Job Vacancy Rate | Share of unfilled positions in the total labor market |
| 💰 Labor Cost Index | Quarterly index tracking changes in hourly labor costs |
| 🔮 12-Month Forecast | Prophet model predictions with 95% confidence intervals |
| 🗺️ CEE Map | Choropleth map of current unemployment rates |
| 📊 Scorecard | Year-over-year comparison heatmap |

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
BigQuery (raw layer)        # raw_unemployment, raw_job_vacancies, raw_wages, raw_youth_unemployment
     │
     ▼
pipeline/transform.py       # Cleans, aggregates, joins into master table
     │
     ▼
BigQuery (transformed layer) # unemployment, job_vacancies, wages, youth_unemployment, master
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

New data typically appears on the dashboard within **24 hours** of Eurostat publishing it.

**Eurostat update frequency:**
- Unemployment & Youth Unemployment → monthly (4–6 week lag)
- Job Vacancy Rate → quarterly (2–3 month lag)
- Labor Cost Index → quarterly (3–4 month lag)

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | [Eurostat API](https://ec.europa.eu/eurostat) |
| Data Warehouse | Google BigQuery |
| Pipeline | Python · pandas · eurostat |
| Forecasting | Facebook Prophet |
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
│   └── main.py              # Streamlit dashboard
├── pipeline/
│   ├── ingest.py            # Eurostat → BigQuery raw
│   ├── transform.py         # Raw → transformed + master
│   └── forecast.py          # Prophet forecasting
├── .github/
│   └── workflows/
│       └── daily_pipeline.yml  # GitHub Actions automation
├── requirements.txt
└── .gitignore
```

---

## 📄 Data Source

All data sourced from **[Eurostat](https://ec.europa.eu/eurostat)** — the statistical office of the European Union.

| Dataset | Eurostat Code |
|---------|--------------|
| Unemployment Rate | `une_rt_m` |
| Youth Unemployment | `une_rt_m` (age: Y_LT25) |
| Job Vacancy Rate | `jvs_q_nace2` |
| Labor Cost Index | `lc_lci_r2_q` |

---

*Built with ❤️ using Python, BigQuery & Streamlit*
