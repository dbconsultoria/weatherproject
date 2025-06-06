import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

API_KEY = '2SWEQ4J9GH75XDPXDY56V3FED'  # Replace with your actual Visual Crossing API key

# PostgreSQL connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'admin',
    'password': 'secret123',
    'dbname': 'warehouse'
}

# Brazilian state capitals
capitals = [
    "Rio Branco", "Maceio", "Macapa", "Manaus", "Salvador", "Fortaleza", "Brasilia",
    "Vitoria", "Goiania", "Sao Luis", "Cuiaba", "Campo Grande", "Belo Horizonte",
    "Belem", "Joao Pessoa", "Curitiba", "Recife", "Teresina", "Rio de Janeiro",
    "Natal", "Porto Alegre", "Porto Velho", "Boa Vista", "Florianopolis", "Sao Paulo",
    "Aracaju", "Palmas"
]


def get_weather_data():
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=4)
    all_data = []

    for city in capitals:
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{start_date}/{end_date}"
        params = {
            "unitGroup": "metric",
            "include": "days",
            "key": API_KEY,
            "contentType": "json"
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            for day in data.get("days", []):
                all_data.append({
                    "city": city,
                    "country": "Brazil",
                    "date": day['datetime'],
                    "temp": day.get('temp'),
                    "conditions": day.get('conditions'),
                    "description": day.get('description', day.get('conditions'))
                })
            print(f" Collected data for {city}")
        else:
            print(f" Failed for {city}: {response.status_code} - {response.text}")

    return pd.DataFrame(all_data)

def insert_to_postgres(df):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            TRUNCATE stage.weathervc;
        """)

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO stage.weathervc (city, country, date, temp, conditions, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['city'], row['country'], row['date'], row['temp'], row['conditions'], row['description']))

        conn.commit()
        print(" Data inserted into 'weathervc' table.")
        cur.close()
        conn.close()
    except Exception as e:
        print(" Error inserting data into PostgreSQL:", e)

def reset_and_reload_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print(" Truncating all dimensions and fact table...")

        # Truncate in dependency order (fact first, then dimensions)
        cursor.execute("TRUNCATE fact.temperature RESTART IDENTITY CASCADE;")
        cursor.execute("TRUNCATE dim.conditions RESTART IDENTITY CASCADE;")
        cursor.execute("TRUNCATE dim.city RESTART IDENTITY CASCADE;")
        cursor.execute("TRUNCATE dim.country RESTART IDENTITY CASCADE;")
        cursor.execute("TRUNCATE dim.date RESTART IDENTITY CASCADE;")

        print(" All tables truncated.")

        print(" Populating dimensions...")

        # Run stored procedures in dependency order
        cursor.execute("CALL load_dim_date();")
        cursor.execute("CALL load_dim_country();")
        cursor.execute("CALL load_dim_city();")
        cursor.execute("CALL load_dim_conditions();")

        print(" Populating fact table...")
        cursor.execute("CALL merge_fact_temperature();")

        print(" ETL process complete: all tables reloaded.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(" Error during reset and reload:", e)


print(" Script started")

if __name__ == "__main__":
    df_weather = get_weather_data()
    if not df_weather.empty:
        insert_to_postgres(df_weather)
    else:
        print("⚠ No data collected.")
    reset_and_reload_data()
