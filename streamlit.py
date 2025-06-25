import streamlit as st
import plotly.express as px
import pandas as pd
import psycopg2


# PostgreSQL connection parameters from environment
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'dbname': os.getenv('DB_NAME')
}

@st.cache_data
def get_table_names():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """)
    tables = cursor.fetchall()
    conn.close()
    return tables

@st.cache_data
def get_table_data(schema, table):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query(f'SELECT * FROM "{schema}"."{table}" LIMIT 500', conn)
    conn.close()
    return df

@st.cache_data
def load_weather_data():
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT
            f.temperature_id,
            f.temp,
            d.full_date,
            ci.city_name,
            co.country_name,
            cond.condition_name
        FROM fact.temperature f
        JOIN dim.date d ON f.date_id = d.date_id
        JOIN dim.city ci ON f.city_id = ci.city_id
        JOIN dim.country co ON ci.country_id = co.country_id
        JOIN dim.conditions cond ON f.condition_id = cond.condition_id;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def categorize_temperature(temp):
    if pd.isnull(temp):
        return "Unknown"
    elif temp < 10:
        return "<10°C"
    elif 10 <= temp < 20:
        return "10–20°C"
    elif 20 <= temp < 30:
        return "20–30°C"
    else:
        return "≥30°C"

# App setup
st.set_page_config(page_title="Weather Dashboard", layout="wide")
st.title("🌡️ Weather Data Dashboard")

tabs = st.tabs(["📋 Tables", "📊 Charts", "⚙️ Filters", "👤 Rodrigo Ribeiro Gonçalves"])

# === 📋 TAB 1: ALL TABLES ===
with tabs[0]:
    st.subheader("📋 All Tables in 'warehouse' Database")
    tables = get_table_names()
    for schema, table in tables:
        with st.expander(f"{schema}.{table}"):
            try:
                df = get_table_data(schema, table)
                st.dataframe(df)
            except Exception as e:
                st.error(f"Error loading {schema}.{table}: {e}")

# === 📊 TAB 2: CHARTS ===
with tabs[1]:
    st.subheader("📊 Visual Analysis")
    df = load_weather_data()
    df["temp_range"] = df["temp"].apply(categorize_temperature)

    # Bar chart: average temperature by city
    temp_by_city = (
        df.groupby("city_name")["temp"]
        .mean()
        .reset_index()
        .sort_values(by="temp", ascending=False)
    )
    fig_city = px.bar(temp_by_city, x="city_name", y="temp",
                      title="Average Temperature by City",
                      labels={"temp": "Average Temp (°C)", "city_name": "City"})
    st.plotly_chart(fig_city, use_container_width=True)

    # Line chart: temperature over time
    temp_over_time = (
        df.groupby("full_date")["temp"]
        .mean()
        .reset_index()
    )
    fig_time = px.line(temp_over_time, x="full_date", y="temp",
                       title="Average Temperature Over Time",
                       labels={"full_date": "Date", "temp": "Average Temp (°C)"})
    st.plotly_chart(fig_time, use_container_width=True)

# === ⚙️ TAB 3: FILTERS + METRICS ===
with tabs[2]:
    st.subheader("⚙️ Interactive Filters and Metrics")
    df = load_weather_data()
    df["temp_range"] = df["temp"].apply(categorize_temperature)

    # Filters
    st.sidebar.header("🔎 Filters")
    selected_condition = st.sidebar.multiselect("Condition", df["condition_name"].unique(), default=df["condition_name"].unique())
    selected_country = st.sidebar.multiselect("Country", df["country_name"].unique(), default=df["country_name"].unique())
    selected_city = st.sidebar.multiselect("City", df["city_name"].unique(), default=df["city_name"].unique())
    selected_ranges = st.sidebar.multiselect("Temperature Range", df["temp_range"].unique(), default=df["temp_range"].unique())

    filtered_df = df[
        df["condition_name"].isin(selected_condition) &
        df["country_name"].isin(selected_country) &
        df["city_name"].isin(selected_city) &
        df["temp_range"].isin(selected_ranges)
    ]

    # Summary metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("🏙️ Number of Cities", filtered_df["city_name"].nunique())
    col2.metric("📈 Temperature Records", len(filtered_df))
    col3.metric("🌡️ Average Temp (°C)", f"{filtered_df['temp'].mean():.2f}" if not filtered_df.empty else "–")

    with st.expander("📋 Filtered Data Table"):
        st.dataframe(filtered_df.reset_index(drop=True))

    st.download_button(
        "⬇️ Download Filtered CSV",
        filtered_df.to_csv(index=False).encode('utf-8'),
        "filtered_weather_data.csv",
        "text/csv"
    )

# === 👤 TAB 4: PERSONAL PROFILE ===
with tabs[3]:
    st.subheader("👤 Rodrigo Ribeiro Gonçalves")
    st.write("Senior Data Engineer | BI | Data Warehouse | Lakehouse | Python | SQL")

    st.markdown(
        """
        - 🌐 [LinkedIn](https://www.linkedin.com/in/rodrigo-ribeiro-pro/)
        - 💻 [GitHub](https://github.com/dbconsultoria)
        - 💻 [Weather Project](https://github.com/dbconsultoria/weatherproject)
        """,
        unsafe_allow_html=True
    )
