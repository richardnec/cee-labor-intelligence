import streamlit as st
from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

load_dotenv()

CEE_COUNTRIES = ['SK', 'CZ', 'PL', 'HU', 'DE', 'AT', 'EE', 'EL']
COUNTRY_NAMES = {
    'SK': 'Slovakia', 'CZ': 'Czechia', 'PL': 'Poland',
    'HU': 'Hungary', 'DE': 'Germany', 'AT': 'Austria',
    'EE': 'Estonia', 'EL': 'Greece'
}
ISO_MAP = {
    'SK': 'SVK', 'CZ': 'CZE', 'PL': 'POL',
    'HU': 'HUN', 'DE': 'DEU', 'AT': 'AUT',
    'EE': 'EST', 'EL': 'GRC'
}

st.set_page_config(
    page_title="CEE Labor Market Intelligence",
    page_icon="📊",
    layout="wide"
)

@st.cache_data(ttl=3600)
def load_unemployment():
    client = bigquery.Client()
    query = f"""
    SELECT country, date, unemployment_rate
    FROM `{client.project}.labor_market.unemployment`
    ORDER BY country, date
    """
    df = client.query(query).to_dataframe()
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    df['unemployment_rate'] = df['unemployment_rate'].round(2)
    return df.sort_values(['country', 'date'])

@st.cache_data(ttl=3600)
def load_forecasts():
    client = bigquery.Client()
    query = f"""
    SELECT country, date, yhat, yhat_lower, yhat_upper
    FROM `{client.project}.labor_market.forecasts`
    ORDER BY country, date
    """
    df = client.query(query).to_dataframe()
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    return df

@st.cache_data(ttl=3600)
def load_master():
    client = bigquery.Client()
    query = f"""
    SELECT country, date, unemployment_rate, youth_unemployment_rate,
           job_vacancy_rate, labor_cost_index
    FROM `{client.project}.labor_market.master`
    ORDER BY country, date
    """
    df = client.query(query).to_dataframe()
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    df['unemployment_rate'] = df['unemployment_rate'].round(2)
    df['youth_unemployment_rate'] = df['youth_unemployment_rate'].round(2)
    df['job_vacancy_rate'] = df['job_vacancy_rate'].round(2)
    df['labor_cost_index'] = df['labor_cost_index'].round(2)
    return df.sort_values(['country', 'date'])

@st.cache_data(ttl=3600)
def load_scorecard():
    client = bigquery.Client()
    query = f"""
    WITH latest AS (
        SELECT country, unemployment_rate,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY date DESC) as rn
        FROM `{client.project}.labor_market.unemployment`
    ),
    prev_year AS (
        SELECT country, unemployment_rate as rate_prev_year,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY date DESC) as rn
        FROM `{client.project}.labor_market.unemployment`
        WHERE date <= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    )
    SELECT
        l.country,
        l.unemployment_rate as current_rate,
        p.rate_prev_year,
        ROUND(l.unemployment_rate - p.rate_prev_year, 2) as yoy_change
    FROM latest l
    LEFT JOIN prev_year p ON l.country = p.country AND p.rn = 1
    WHERE l.rn = 1
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_last_updated():
    client = bigquery.Client()
    query = f"""
    SELECT 'unemployment' as source, MAX(date) as last_date FROM `{client.project}.labor_market.unemployment`
    UNION ALL
    SELECT 'youth_unemployment', MAX(date) FROM `{client.project}.labor_market.youth_unemployment`
    UNION ALL
    SELECT 'job_vacancies', MAX(date) FROM `{client.project}.labor_market.job_vacancies`
    UNION ALL
    SELECT 'wages', MAX(date) FROM `{client.project}.labor_market.wages`
    """
    df = client.query(query).to_dataframe()
    return dict(zip(df['source'], pd.to_datetime(df['last_date'])))

def last_updated_str(last_dates, key):
    d = last_dates.get(key)
    return f"*Last updated: {d.strftime('%b %Y')}  |  Source: Eurostat*" if d else ""

# ── HEADER ──────────────────────────────────────────────
st.title("🌍 CEE Labor Market Intelligence")

st.markdown("""
Real-time labor market analytics for **Central & Eastern Europe** — tracking unemployment, 
youth employment, job vacancies, and labor costs across 8 countries.

---

**What this dashboard shows:**
- 📈 **Unemployment Rate** — monthly data for SK, CZ, PL, HU, DE, AT, EE, GR
- 👥 **Youth Unemployment** — rate for population under 25
- 💼 **Job Vacancy Rate** — share of unfilled positions in the total labor market
- 💰 **Labor Cost Index** — quarterly index tracking changes in hourly labor costs
- 🔮 **12-month Forecast** — Prophet model predictions with 95% confidence intervals

**Data source:** [Eurostat](https://ec.europa.eu/eurostat) — the official statistical office of the EU.

**Update frequency:**
- Unemployment & Youth Unemployment → updated **monthly**, typically with a **4–6 week lag**
- Job Vacancy Rate → updated **quarterly**, typically with a **2–3 month lag**  
- Labor Cost Index → updated **quarterly**, typically with a **3–4 month lag**

This dashboard automatically pulls the latest available data from Eurostat on a **daily basis**.
""")

# ── LOAD DATA ───────────────────────────────────────────
df = load_unemployment()
df_forecast = load_forecasts()
df_master = load_master()
df_scorecard = load_scorecard()
last_dates = load_last_updated()
df_scorecard['country_name'] = df_scorecard['country'].map(COUNTRY_NAMES)

# ── SIDEBAR ─────────────────────────────────────────────
st.sidebar.header("Filters")
countries = st.sidebar.multiselect(
    "Select Countries",
    options=sorted(df['country'].unique()),
    default=sorted(df['country'].unique())
)

df_filtered = df[df['country'].isin(countries)].copy()
df_master_filtered = df_master[df_master['country'].isin(countries)].copy()

# ── KPI METRIKY ─────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    latest_avg = df_filtered.groupby('country')['unemployment_rate'].last().mean()
    st.metric("Avg Unemployment Rate", f"{latest_avg:.1f}%")
with col2:
    youth_avg = df_master_filtered.groupby('country')['youth_unemployment_rate'].last().mean()
    st.metric("Avg Youth Unemployment", f"{youth_avg:.1f}%" if pd.notna(youth_avg) else "N/A")
with col3:
    jvr_avg = df_master_filtered.dropna(subset=['job_vacancy_rate']).groupby('country')['job_vacancy_rate'].last().mean()
    st.metric("Avg Job Vacancy Rate", f"{jvr_avg:.1f}%" if pd.notna(jvr_avg) else "N/A")
with col4:
    st.metric("Countries", len(countries))

st.divider()

# ── HLAVNY GRAF ─────────────────────────────────────────
st.subheader("📈 Unemployment Rate Over Time")
st.caption(last_updated_str(last_dates, 'unemployment'))

col_btn1, col_btn2, col_btn3, col_btn4, _ = st.columns([1,1,1,1,8])
with col_btn1:
    if st.button("1Y", key="main_1y"): st.session_state['main_years'] = 1
with col_btn2:
    if st.button("3Y", key="main_3y"): st.session_state['main_years'] = 3
with col_btn3:
    if st.button("5Y", key="main_5y"): st.session_state['main_years'] = 5
with col_btn4:
    if st.button("All", key="main_all"): st.session_state['main_years'] = 999

main_years = st.session_state.get('main_years', 3)
max_date = df_filtered['date'].max()
df_view = df_filtered if main_years == 999 else df_filtered[df_filtered['date'] >= max_date - pd.DateOffset(years=main_years)].copy()

df_smooth = df_view.copy()
df_smooth['unemployment_rate'] = df_smooth.groupby('country')['unemployment_rate'].transform(
    lambda x: x.rolling(window=3, min_periods=1).mean()
)
y_min = df_smooth['unemployment_rate'].min() * 0.9
y_max = df_smooth['unemployment_rate'].max() * 1.1

fig_main = go.Figure()
for country in sorted(df_smooth['country'].unique()):
    df_c = df_smooth[df_smooth['country'] == country]
    fig_main.add_trace(go.Scatter(
        x=df_c['date'], y=df_c['unemployment_rate'],
        name=COUNTRY_NAMES.get(country, country), mode='lines',
        line=dict(width=2),
        hovertemplate=f'<b>{COUNTRY_NAMES.get(country, country)}</b><br>%{{x|%b %Y}}<br>%{{y:.1f}}%<extra></extra>'
    ))
fig_main.update_layout(
    height=500, hovermode='x unified', plot_bgcolor='white', paper_bgcolor='white',
    legend=dict(orientation='v', x=1.02, y=1), margin=dict(r=120),
    xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', range=[y_min, y_max], ticksuffix='%'),
)
st.plotly_chart(fig_main, use_container_width=True)

# ── BAR CHART ───────────────────────────────────────────
st.subheader("🏳️ Latest Unemployment Rate by Country")
df_latest = df_filtered.groupby('country')['unemployment_rate'].last().reset_index()
df_latest = df_latest.sort_values('unemployment_rate')
df_latest['country_name'] = df_latest['country'].map(COUNTRY_NAMES)

fig2 = px.bar(
    df_latest, x='country_name', y='unemployment_rate',
    color='unemployment_rate', color_continuous_scale='RdYlGn_r',
    labels={'unemployment_rate': 'Unemployment Rate (%)', 'country_name': 'Country'},
    text='unemployment_rate'
)
fig2.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig2.update_layout(height=400, plot_bgcolor='white', paper_bgcolor='white',
                   showlegend=False, yaxis=dict(ticksuffix='%'))
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── YOUTH UNEMPLOYMENT ──────────────────────────────────
st.subheader("👥 Youth Unemployment Rate (Under 25)")
st.caption(last_updated_str(last_dates, 'youth_unemployment'))

col_y1, col_y2, col_y3, col_y4, _ = st.columns([1,1,1,1,8])
with col_y1:
    if st.button("1Y", key="youth_1y"): st.session_state['youth_years'] = 1
with col_y2:
    if st.button("3Y", key="youth_3y"): st.session_state['youth_years'] = 3
with col_y3:
    if st.button("5Y", key="youth_5y"): st.session_state['youth_years'] = 5
with col_y4:
    if st.button("All", key="youth_all"): st.session_state['youth_years'] = 999

youth_years = st.session_state.get('youth_years', 3)
df_youth = df_master_filtered[df_master_filtered['youth_unemployment_rate'].notna()].copy()
max_date_y = df_youth['date'].max() if not df_youth.empty else pd.Timestamp.now()
df_youth_view = df_youth if youth_years == 999 else df_youth[df_youth['date'] >= max_date_y - pd.DateOffset(years=youth_years)].copy()

fig_youth = go.Figure()
for country in sorted(df_youth_view['country'].unique()):
    df_c = df_youth_view[df_youth_view['country'] == country]
    fig_youth.add_trace(go.Scatter(
        x=df_c['date'], y=df_c['youth_unemployment_rate'],
        name=COUNTRY_NAMES.get(country, country), mode='lines', line=dict(width=2),
        hovertemplate=f'<b>{COUNTRY_NAMES.get(country, country)}</b><br>%{{x|%b %Y}}<br>%{{y:.1f}}%<extra></extra>'
    ))
fig_youth.update_layout(
    height=450, hovermode='x unified', plot_bgcolor='white', paper_bgcolor='white',
    legend=dict(orientation='v', x=1.02, y=1), margin=dict(r=120),
    xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', ticksuffix='%'),
)
st.plotly_chart(fig_youth, use_container_width=True)

df_youth_latest = df_master_filtered.dropna(subset=['youth_unemployment_rate'])
df_youth_latest = df_youth_latest.groupby('country').last().reset_index()
df_youth_latest['country_name'] = df_youth_latest['country'].map(COUNTRY_NAMES)
df_youth_latest = df_youth_latest.sort_values('youth_unemployment_rate')

fig_youth_bar = go.Figure()
fig_youth_bar.add_trace(go.Bar(
    x=df_youth_latest['country_name'], y=df_youth_latest['unemployment_rate'],
    name='Total Unemployment', marker_color='#42A5F5',
    text=df_youth_latest['unemployment_rate'], texttemplate='%{text:.1f}%', textposition='outside'
))
fig_youth_bar.add_trace(go.Bar(
    x=df_youth_latest['country_name'], y=df_youth_latest['youth_unemployment_rate'],
    name='Youth Unemployment', marker_color='#EF5350',
    text=df_youth_latest['youth_unemployment_rate'], texttemplate='%{text:.1f}%', textposition='outside'
))
fig_youth_bar.update_layout(
    height=400, barmode='group', plot_bgcolor='white', paper_bgcolor='white',
    yaxis=dict(ticksuffix='%'), legend=dict(orientation='h', y=1.1)
)
st.plotly_chart(fig_youth_bar, use_container_width=True)

st.divider()

# ── JOB VACANCY RATE ────────────────────────────────────
st.subheader("💼 Job Vacancy Rate")
st.markdown("**Job Vacancy Rate** = percentage of total jobs (filled + unfilled) that are vacant. Higher = more open positions, tighter labor market.")
st.caption(last_updated_str(last_dates, 'job_vacancies'))

col_j1, col_j2, col_j3, col_j4, _ = st.columns([1,1,1,1,8])
with col_j1:
    if st.button("1Y", key="jvr_1y"): st.session_state['jvr_years'] = 1
with col_j2:
    if st.button("3Y", key="jvr_3y"): st.session_state['jvr_years'] = 3
with col_j3:
    if st.button("5Y", key="jvr_5y"): st.session_state['jvr_years'] = 5
with col_j4:
    if st.button("All", key="jvr_all"): st.session_state['jvr_years'] = 999

jvr_years = st.session_state.get('jvr_years', 3)
df_jvr = df_master_filtered[df_master_filtered['job_vacancy_rate'].notna()].copy()
max_date_j = df_jvr['date'].max() if not df_jvr.empty else pd.Timestamp.now()
df_jvr_view = df_jvr if jvr_years == 999 else df_jvr[df_jvr['date'] >= max_date_j - pd.DateOffset(years=jvr_years)].copy()

fig_jvr = go.Figure()
for country in sorted(df_jvr_view['country'].unique()):
    df_c = df_jvr_view[df_jvr_view['country'] == country]
    fig_jvr.add_trace(go.Scatter(
        x=df_c['date'], y=df_c['job_vacancy_rate'],
        name=COUNTRY_NAMES.get(country, country), mode='lines+markers',
        marker=dict(size=4), line=dict(width=2),
        hovertemplate=f'<b>{COUNTRY_NAMES.get(country, country)}</b><br>%{{x|%b %Y}}<br>%{{y:.1f}}%<extra></extra>'
    ))
fig_jvr.update_layout(
    height=450, hovermode='x unified', plot_bgcolor='white', paper_bgcolor='white',
    legend=dict(orientation='v', x=1.02, y=1), margin=dict(r=120),
    xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', ticksuffix='%'),
)
st.plotly_chart(fig_jvr, use_container_width=True)

st.divider()

# ── LABOR COST INDEX ────────────────────────────────────
st.subheader("💰 Labor Cost Index")
st.markdown("**Labor Cost Index (LCI)** = measures changes in hourly labor costs (wages + non-wage costs). Base year = 100. Values above 100 indicate costs have risen since the base period.")
st.caption(last_updated_str(last_dates, 'wages'))

col_w1, col_w2, col_w3, col_w4, _ = st.columns([1,1,1,1,8])
with col_w1:
    if st.button("1Y", key="wage_1y"): st.session_state['wage_years'] = 1
with col_w2:
    if st.button("3Y", key="wage_3y"): st.session_state['wage_years'] = 3
with col_w3:
    if st.button("5Y", key="wage_5y"): st.session_state['wage_years'] = 5
with col_w4:
    if st.button("All", key="wage_all"): st.session_state['wage_years'] = 999

wage_years = st.session_state.get('wage_years', 3)
df_wage = df_master_filtered[df_master_filtered['labor_cost_index'].notna()].copy()
max_date_w = df_wage['date'].max() if not df_wage.empty else pd.Timestamp.now()
df_wage_view = df_wage if wage_years == 999 else df_wage[df_wage['date'] >= max_date_w - pd.DateOffset(years=wage_years)].copy()

fig_wage = go.Figure()
for country in sorted(df_wage_view['country'].unique()):
    df_c = df_wage_view[df_wage_view['country'] == country]
    fig_wage.add_trace(go.Scatter(
        x=df_c['date'], y=df_c['labor_cost_index'],
        name=COUNTRY_NAMES.get(country, country), mode='lines+markers',
        marker=dict(size=4), line=dict(width=2),
        hovertemplate=f'<b>{COUNTRY_NAMES.get(country, country)}</b><br>%{{x|%b %Y}}<br>Index: %{{y:.1f}}<extra></extra>'
    ))
fig_wage.update_layout(
    height=450, hovermode='x unified', plot_bgcolor='white', paper_bgcolor='white',
    legend=dict(orientation='v', x=1.02, y=1), margin=dict(r=120),
    xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='Index (base=100)'),
)
st.plotly_chart(fig_wage, use_container_width=True)

st.divider()

# ── FORECAST ────────────────────────────────────────────
st.subheader("🔮 Unemployment Rate Forecast — Next 12 Months")
st.caption(last_updated_str(last_dates, 'unemployment'))

selected_country = st.selectbox(
    "Select Country for Forecast",
    options=sorted(df['country'].unique()),
    format_func=lambda x: COUNTRY_NAMES.get(x, x)
)

col_fc1, col_fc2, col_fc3, col_fc4, _ = st.columns([1,1,1,1,8])
with col_fc1:
    if st.button("1Y", key="fc_1y"): st.session_state['fc_years'] = 1
with col_fc2:
    if st.button("3Y", key="fc_3y"): st.session_state['fc_years'] = 3
with col_fc3:
    if st.button("5Y", key="fc_5y"): st.session_state['fc_years'] = 5
with col_fc4:
    if st.button("All", key="fc_all"): st.session_state['fc_years'] = 999

fc_years = st.session_state.get('fc_years', 3)
df_hist = df[df['country'] == selected_country].copy()
df_fc = df_forecast[df_forecast['country'] == selected_country].copy()
last_date = df_hist['date'].max()
df_hist_view = df_hist if fc_years == 999 else df_hist[df_hist['date'] >= last_date - pd.DateOffset(years=fc_years)].copy()

y_min_fc = min(df_hist_view['unemployment_rate'].min(), df_fc['yhat_lower'].min()) * 0.85
y_max_fc = max(df_hist_view['unemployment_rate'].max(), df_fc['yhat_upper'].max()) * 1.15

fig3 = go.Figure()
fig3.add_trace(go.Scatter(
    x=df_hist_view['date'], y=df_hist_view['unemployment_rate'],
    name='Historical', line=dict(color='#2196F3', width=2),
    hovertemplate='<b>Historical</b><br>%{x|%b %Y}<br>%{y:.1f}%<extra></extra>'
))
fig3.add_trace(go.Scatter(
    x=df_fc['date'], y=df_fc['yhat'],
    name='Forecast', line=dict(color='#FF9800', width=2.5, dash='dash'),
    hovertemplate='<b>Forecast</b><br>%{x|%b %Y}<br>%{y:.1f}%<extra></extra>'
))
fig3.add_trace(go.Scatter(
    x=pd.concat([df_fc['date'], df_fc['date'][::-1]]),
    y=pd.concat([df_fc['yhat_upper'], df_fc['yhat_lower'][::-1]]),
    fill='toself', fillcolor='rgba(255,152,0,0.12)',
    line=dict(color='rgba(255,255,255,0)'),
    name='95% Confidence', hoverinfo='skip'
))
fig3.add_vline(
    x=last_date.timestamp() * 1000, line_dash="dot",
    line_color="#aaa", line_width=1.5,
    annotation_text="Forecast →", annotation_position="top right",
    annotation_font_color="#aaa"
)
fig3.update_layout(
    height=500,
    title=f'Unemployment Forecast — {COUNTRY_NAMES.get(selected_country, selected_country)}',
    hovermode='x unified', plot_bgcolor='white', paper_bgcolor='white',
    xaxis=dict(showgrid=True, gridcolor='#f0f0f0',
               range=[df_hist_view['date'].min(), df_fc['date'].max()]),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', range=[y_min_fc, y_max_fc], ticksuffix='%'),
)
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── MAPA ────────────────────────────────────────────────
st.subheader("🗺️ CEE Region Map — Current Unemployment")

df_map = df_latest.copy()
df_map['iso_alpha'] = df_map['country'].map(ISO_MAP)

fig_map = px.choropleth(
    df_map, locations='iso_alpha', color='unemployment_rate',
    hover_name='country_name',
    hover_data={'unemployment_rate': ':.1f', 'iso_alpha': False},
    color_continuous_scale='RdYlGn_r', range_color=[2, 10],
    labels={'unemployment_rate': 'Unemployment Rate (%)'}
)
fig_map.update_layout(
    height=600,
    geo=dict(
        scope='europe', projection_type='natural earth',
        showland=True, landcolor='#f5f5f5',
        showocean=True, oceancolor='#e8f4f8',
        showcountries=True, countrycolor='#cccccc',
        center=dict(lat=52, lon=18), projection_scale=3
    )
)
st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# ── SCORECARD ───────────────────────────────────────────
st.subheader("📊 CEE Comparative Scorecard")
st.markdown("Overview of all countries and key metrics at a glance")

scorecard_pivot = df_scorecard.set_index('country_name')[
    ['current_rate', 'rate_prev_year', 'yoy_change']
].T
scorecard_pivot.index = ['Current Rate (%)', 'Rate 1Y Ago (%)', 'YoY Change (pp)']

fig_heat = px.imshow(
    scorecard_pivot, color_continuous_scale='RdYlGn_r',
    aspect='auto', title='CEE Labor Market Scorecard', text_auto='.1f'
)
fig_heat.update_layout(height=300)
st.plotly_chart(fig_heat, use_container_width=True)

st.dataframe(
    df_scorecard[['country_name', 'current_rate', 'rate_prev_year', 'yoy_change']]
    .rename(columns={
        'country_name': 'Country',
        'current_rate': 'Current Rate (%)',
        'rate_prev_year': '1Y Ago (%)',
        'yoy_change': 'YoY Change (pp)'
    })
    .sort_values('Current Rate (%)'),
    use_container_width=True, hide_index=True
)

st.divider()
st.caption("Data source: Eurostat | Built with Streamlit & BigQuery")