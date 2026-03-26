import streamlit as st
from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.oauth2 import service_account

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

def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            return bigquery.Client(
                credentials=credentials,
                project=st.secrets["gcp_service_account"]["project_id"]
            )
    except Exception:
        pass
    load_dotenv()
    return bigquery.Client()

@st.cache_data(ttl=3600)
def load_unemployment():
    client = get_bq_client()
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
    client = get_bq_client()
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
    client = get_bq_client()
    query = f"""
    SELECT country, date, unemployment_rate, youth_unemployment_rate,
           inflation_rate, gdp_growth, net_wage_pps
    FROM `{client.project}.labor_market.master`
    ORDER BY country, date
    """
    df = client.query(query).to_dataframe()
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    df['unemployment_rate'] = df['unemployment_rate'].round(2)
    df['youth_unemployment_rate'] = df['youth_unemployment_rate'].round(2)
    df['inflation_rate'] = df['inflation_rate'].round(2)
    df['gdp_growth'] = df['gdp_growth'].round(2)
    df['net_wage_pps'] = df['net_wage_pps'].round(0)
    return df.sort_values(['country', 'date'])

@st.cache_data(ttl=3600)
def load_last_updated():
    client = get_bq_client()
    query = f"""
    SELECT 'unemployment' as source, MAX(date) as last_date FROM `{client.project}.labor_market.unemployment`
    UNION ALL
    SELECT 'youth_unemployment', MAX(date) FROM `{client.project}.labor_market.youth_unemployment`
    UNION ALL
    SELECT 'inflation', MAX(date) FROM `{client.project}.labor_market.inflation`
    UNION ALL
    SELECT 'gdp_growth', MAX(date) FROM `{client.project}.labor_market.gdp_growth`
    UNION ALL
    SELECT 'net_wages_pps', MAX(date) FROM `{client.project}.labor_market.net_wages_pps`
    """
    df = client.query(query).to_dataframe()
    return dict(zip(df['source'], pd.to_datetime(df['last_date'])))

def last_updated_str(last_dates, key):
    d = last_dates.get(key)
    return f"*Last updated: {d.strftime('%b %Y')}  |  Source: Eurostat*" if d else ""

def time_buttons(key):
    col1, col2, col3, col4, _ = st.columns([1,1,1,1,8])
    with col1:
        if st.button("1Y", key=f"{key}_1y"): st.session_state[key] = 1
    with col2:
        if st.button("3Y", key=f"{key}_3y"): st.session_state[key] = 3
    with col3:
        if st.button("5Y", key=f"{key}_5y"): st.session_state[key] = 5
    with col4:
        if st.button("All", key=f"{key}_all"): st.session_state[key] = 999
    return st.session_state.get(key, 3)

# ── HEADER ──────────────────────────────────────────────
st.title("🌍 CEE Labor Market Intelligence")
st.markdown("""
Real-time economic & labor market analytics for **Central & Eastern Europe** — tracking
unemployment, wages, inflation, and GDP growth across 8 countries.

---

**What this dashboard shows:**
- 📈 **Unemployment Rate** — monthly data for SK, CZ, PL, HU, DE, AT, EE, GR
- 👥 **Youth Unemployment** — rate for population under 25
- 📉 **Inflation (HICP)** — monthly harmonized consumer price index (% change)
- 📊 **GDP Growth** — quarterly real GDP growth vs previous quarter
- 💰 **Net Wages (PPS)** — annual net wages in Purchasing Power Standards, comparable across countries
- 🔮 **12-month Forecast** — Prophet model predictions with 95% confidence intervals

**Data source:** [Eurostat](https://ec.europa.eu/eurostat) — the official statistical office of the EU.

**Update frequency:**
- Unemployment & Youth Unemployment & Inflation → updated **monthly**, typically with a **4–6 week lag**
- GDP Growth → updated **quarterly**, typically with a **2–3 month lag**
- Net Wages (PPS) → updated **annually**

This dashboard automatically pulls the latest available data from Eurostat on a **daily basis**.
""")

# ── LOAD DATA ───────────────────────────────────────────
df = load_unemployment()
df_forecast = load_forecasts()
df_master = load_master()
last_dates = load_last_updated()

# ── SIDEBAR ─────────────────────────────────────────────
st.sidebar.header("Filters")
countries = st.sidebar.multiselect(
    "Select Countries",
    options=sorted(df['country'].unique()),
    default=sorted(df['country'].unique()),
    format_func=lambda x: COUNTRY_NAMES.get(x, x)
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
    infl_avg = df_master_filtered.groupby('country')['inflation_rate'].last().mean()
    st.metric("Avg Inflation (HICP)", f"{infl_avg:.1f}%" if pd.notna(infl_avg) else "N/A")
with col4:
    gdp_avg = df_master_filtered.dropna(subset=['gdp_growth']).groupby('country')['gdp_growth'].last().mean()
    st.metric("Avg GDP Growth", f"{gdp_avg:.1f}%" if pd.notna(gdp_avg) else "N/A")

st.divider()

# ── HLAVNY GRAF ─────────────────────────────────────────
st.subheader("📈 Unemployment Rate Over Time")
st.caption(last_updated_str(last_dates, 'unemployment'))
main_years = time_buttons('main_years')

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
        name=COUNTRY_NAMES.get(country, country), mode='lines', line=dict(width=2),
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
youth_years = time_buttons('youth_years')

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

# ── INFLACIA ────────────────────────────────────────────
st.subheader("📉 Inflation Rate (HICP)")
st.markdown("**HICP** = Harmonized Index of Consumer Prices. Monthly % change vs same month previous year. Comparable across all EU countries.")
st.caption(last_updated_str(last_dates, 'inflation'))
infl_years = time_buttons('infl_years')

df_infl = df_master_filtered[df_master_filtered['inflation_rate'].notna()].copy()
max_date_i = df_infl['date'].max() if not df_infl.empty else pd.Timestamp.now()
df_infl_view = df_infl if infl_years == 999 else df_infl[df_infl['date'] >= max_date_i - pd.DateOffset(years=infl_years)].copy()

fig_infl = go.Figure()
for country in sorted(df_infl_view['country'].unique()):
    df_c = df_infl_view[df_infl_view['country'] == country]
    fig_infl.add_trace(go.Scatter(
        x=df_c['date'], y=df_c['inflation_rate'],
        name=COUNTRY_NAMES.get(country, country), mode='lines', line=dict(width=2),
        hovertemplate=f'<b>{COUNTRY_NAMES.get(country, country)}</b><br>%{{x|%b %Y}}<br>%{{y:.1f}}%<extra></extra>'
    ))
fig_infl.add_hline(y=2, line_dash="dot", line_color="#aaa", line_width=1,
                   annotation_text="ECB target 2%", annotation_position="bottom right")
fig_infl.update_layout(
    height=450, hovermode='x unified', plot_bgcolor='white', paper_bgcolor='white',
    legend=dict(orientation='v', x=1.02, y=1), margin=dict(r=120),
    xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', ticksuffix='%'),
)
st.plotly_chart(fig_infl, use_container_width=True)

st.divider()

# ── GDP GROWTH ──────────────────────────────────────────
st.subheader("📊 GDP Growth (Quarterly)")
st.markdown("**GDP Growth** = % change in real GDP vs previous quarter. Seasonally adjusted. Positive = economy expanding, negative = contracting.")
st.caption(last_updated_str(last_dates, 'gdp_growth'))
gdp_years = time_buttons('gdp_years')

df_gdp = df_master_filtered[df_master_filtered['gdp_growth'].notna()].copy()
max_date_g = df_gdp['date'].max() if not df_gdp.empty else pd.Timestamp.now()
df_gdp_view = df_gdp if gdp_years == 999 else df_gdp[df_gdp['date'] >= max_date_g - pd.DateOffset(years=gdp_years)].copy()
df_gdp_view = df_gdp_view.drop_duplicates(subset=['country', 'date'])

fig_gdp = go.Figure()
for country in sorted(df_gdp_view['country'].unique()):
    df_c = df_gdp_view[df_gdp_view['country'] == country]
    fig_gdp.add_trace(go.Scatter(
        x=df_c['date'], y=df_c['gdp_growth'],
        name=COUNTRY_NAMES.get(country, country), mode='lines+markers',
        marker=dict(size=4), line=dict(width=2),
        hovertemplate=f'<b>{COUNTRY_NAMES.get(country, country)}</b><br>%{{x|%b %Y}}<br>%{{y:.1f}}%<extra></extra>'
    ))
fig_gdp.add_hline(y=0, line_color="#ccc", line_width=1)
fig_gdp.update_layout(
    height=450, hovermode='x unified', plot_bgcolor='white', paper_bgcolor='white',
    legend=dict(orientation='v', x=1.02, y=1), margin=dict(r=120),
    xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', ticksuffix='%'),
)
st.plotly_chart(fig_gdp, use_container_width=True)

st.divider()

# ── NET WAGES PPS ───────────────────────────────────────
st.subheader("💰 Net Wages in PPS (Purchasing Power Standards)")
st.markdown("**Net Wages in PPS** = annual take-home pay of a single worker at average wage, expressed in Purchasing Power Standards. PPS removes price level differences between countries, making wages truly comparable. Higher PPS = higher real purchasing power.")
st.caption(last_updated_str(last_dates, 'net_wages_pps'))

df_wages = df_master_filtered[df_master_filtered['net_wage_pps'].notna()].copy()
df_wages_latest = df_wages.groupby('country').last().reset_index()
df_wages_latest['country_name'] = df_wages_latest['country'].map(COUNTRY_NAMES)
df_wages_latest = df_wages_latest.sort_values('net_wage_pps')

fig_wages_bar = px.bar(
    df_wages_latest, x='country_name', y='net_wage_pps',
    color='net_wage_pps', color_continuous_scale='Blues',
    labels={'net_wage_pps': 'Net Wage (PPS)', 'country_name': 'Country'},
    text='net_wage_pps'
)
fig_wages_bar.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
fig_wages_bar.update_layout(
    height=400, plot_bgcolor='white', paper_bgcolor='white',
    showlegend=False, yaxis=dict(title='Annual Net Wage (PPS)')
)
st.plotly_chart(fig_wages_bar, use_container_width=True)

# Wage trend over time
wage_years = time_buttons('wage_years')
df_wages_view = df_wages if wage_years == 999 else df_wages[df_wages['date'] >= df_wages['date'].max() - pd.DateOffset(years=wage_years)].copy()

fig_wages = go.Figure()
for country in sorted(df_wages_view['country'].unique()):
    df_c = df_wages_view[df_wages_view['country'] == country]
    fig_wages.add_trace(go.Scatter(
        x=df_c['date'], y=df_c['net_wage_pps'],
        name=COUNTRY_NAMES.get(country, country), mode='lines+markers',
        marker=dict(size=6), line=dict(width=2),
        hovertemplate=f'<b>{COUNTRY_NAMES.get(country, country)}</b><br>%{{x|%Y}}<br>%{{y:,.0f}} PPS<extra></extra>'
    ))
fig_wages.update_layout(
    height=450, hovermode='x unified', plot_bgcolor='white', paper_bgcolor='white',
    legend=dict(orientation='v', x=1.02, y=1), margin=dict(r=120),
    xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='Annual Net Wage (PPS)'),
)
st.plotly_chart(fig_wages, use_container_width=True)

st.divider()

# ── FORECAST ────────────────────────────────────────────
st.subheader("🔮 Unemployment Rate Forecast — Next 12 Months")
st.markdown("*Forecast generated using the Prophet time series model, trained on historical unemployment data.*")
st.caption(last_updated_str(last_dates, 'unemployment'))

selected_country = st.selectbox(
    "Select Country for Forecast",
    options=sorted(df['country'].unique()),
    format_func=lambda x: COUNTRY_NAMES.get(x, x)
)
fc_years = time_buttons('fc_years')

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

# ── SCORECARD TABLE ─────────────────────────────────────
st.subheader("📊 CEE Economic Snapshot")
st.markdown("Latest available data for all countries across key indicators.")

df_snap = df_master.copy()
df_snap_latest = df_snap.groupby('country').last().reset_index()
df_snap_latest['country_name'] = df_snap_latest['country'].map(COUNTRY_NAMES)
df_snap_latest = df_snap_latest[df_snap_latest['country'].isin(countries)]
df_snap_latest = df_snap_latest.sort_values('unemployment_rate')

st.dataframe(
    df_snap_latest[['country_name', 'unemployment_rate', 'youth_unemployment_rate',
                    'inflation_rate', 'gdp_growth', 'net_wage_pps']]
    .rename(columns={
        'country_name': 'Country',
        'unemployment_rate': 'Unemployment (%)',
        'youth_unemployment_rate': 'Youth Unempl. (%)',
        'inflation_rate': 'Inflation (%)',
        'gdp_growth': 'GDP Growth (%)',
        'net_wage_pps': 'Net Wage (PPS)'
    }),
    use_container_width=True,
    hide_index=True
)

st.divider()
st.caption("Data source: Eurostat | Built with Streamlit & BigQuery")