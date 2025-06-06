# Data Engineering Portfolio: Weather Data ETL Pipeline

## Overview

This project demonstrates the implementation of a complete data engineering pipeline that collects weather data, processes it through an ETL process, and stores it in a PostgreSQL database. The project leverages **Python**, **FastAPI**, **PostgreSQL**, and **Docker** to handle and expose weather data for cities in Brazil. The pipeline fetches weather data, transforms it, and loads it into a data warehouse designed to store and analyze temperature data over time.

## Project Architecture

The architecture follows the **ETL (Extract, Transform, Load)** pattern, utilizing the following components:

* **Data Extraction**: Weather data is fetched via the Visual Crossing Weather API for multiple Brazilian cities.
* **Data Transformation**: The data is cleaned and formatted in Python using Pandas before being inserted into PostgreSQL.
* **Data Loading**: Data is loaded into a PostgreSQL database using SQL, where it is organized into multiple tables with a star schema design.
* **API**: FastAPI is used to expose a RESTful API for querying the weather data.
* **Docker**: The entire environment, including the FastAPI app and PostgreSQL database, is containerized using Docker.

## Data Model

The PostgreSQL database schema is designed to store weather data in a **star schema** format with the following key tables:

### Dimension Tables

* **dim.city**: Stores information about cities.
* **dim.conditions**: Stores weather conditions (e.g., "sunny", "cloudy").
* **dim.country**: Stores country information (only Brazil in this case).
* **dim.date**: Stores date-related information (e.g., day, month, weekday, is\_weekend).

### Fact Table

* **fact.temperature**: Stores the actual temperature measurements for each city on a given date and condition.

### Staging Table

* **stage.weathervc**: This table holds the raw weather data fetched from the Visual Crossing API before it is transformed and loaded into the main tables.

### Stored Procedures

* Several stored procedures are used to load data into dimension tables (`dim.city`, `dim.conditions`, `dim.country`, `dim.date`) and merge the temperature data into the fact table (`fact.temperature`).

## ETL Process

The ETL process is orchestrated in Python, where:

1. **Data Extraction**: Weather data is fetched using the Visual Crossing API for a predefined list of Brazilian capitals.
2. **Data Transformation**: The raw data is cleaned, and necessary transformations are applied (e.g., ensuring data integrity, formatting the data properly).
3. **Data Loading**: The data is inserted into the `stage.weathervc` table in PostgreSQL, and then dimension and fact tables are populated using stored procedures that handle the data transformations and merging.

### Main Components of the ETL Pipeline:

* **`main.py`**: This file is responsible for:

  * Fetching weather data using the API.
  * Inserting the data into the staging table (`stage.weathervc`).
  * Calling stored procedures to load data into dimension tables and merge into the fact table.
* **Stored Procedures**: These are PL/pgSQL scripts in the `database_metadata.sql` file that manage the data transformation and insertion logic for the various tables.

## Docker Setup

The project is containerized using Docker to ensure easy deployment and environment consistency. The `Dockerfile` sets up the necessary environment for running both the FastAPI application and the PostgreSQL database.

### Dockerfile Overview

* It uses the official Python image to install required Python dependencies.
* The PostgreSQL instance runs locally within the Docker container, ensuring that the application can interact with the database seamlessly.
* The FastAPI app is exposed on port `8000`.

### Running the Project with Docker

To run the project locally using Docker, follow these steps:

1. **Build the Docker image**:

   ```bash
   docker build -t weather-etl .
   ```

2. **Run the Docker container**:

    You can create a .env file to execute the docker locally in a proper way like this.

   ```bash
    VC_API_KEY=your visual crossing key
    DB_HOST=your pg database
    DB_PORT=5432
    DB_USER=your db user
    DB_PASSWORD=your db password
    DB_NAME=database name
    ```

   ```bash
   docker run -it weather-etl bash
   ```
   To input the information from a env file
   ```bash
   docker run --env-file .env weather-etl
   ```

3. **Run Streamlit Locally**:
   
    ```bash
    streamlit run .\streamlit.py
   ```
   
## Deployment on Render

This project is deployed on **Render.com** for cloud hosting. The deployment process is automated using Docker, making it easy to deploy the application and database on Render's cloud infrastructure. The FastAPI app is accessible via a public URL, and the PostgreSQL database is managed within the Render environment.

---

## API Endpoints

The FastAPI app exposes the following endpoints:

* **GET `/weather/`**: Fetches weather data for all cities.
* **GET `/weather/{city}`**: Fetches weather data for a specific city.

For detailed API documentation, visit the `/docs` endpoint after running the app locally or on Render.

## Dimensional Modeling

The dimensional model in this project follows a **star schema**, which is ideal for analytical workloads and reporting. It separates the data into **dimension tables** (describing the context) and a **fact table** (storing measurable events). This approach improves query performance and supports easy filtering, aggregation, and slicing of data.

### Table Relationships

- Each **temperature reading** in the `fact.temperature` table is linked to:
  - a **specific city** in `dim.city`, which is related to a `dim.country`
  - a **specific date** in `dim.date`, which includes calendar attributes
  - a **weather condition** in `dim.conditions`, which includes descriptive information

### Data Integrity Rules

- The `country_name`, `city_name`, `condition_name`, and `full_date` fields are **unique** in their respective tables to avoid duplication.
- The combination of `date_id` and `city_id` is **unique in the fact table**, ensuring that each city has only one temperature record per date.

This model enables the generation of time-series analyses, condition-based aggregations, and city-wise comparisons with high flexibility and performance.

A unified query (also used to power visualizations) joins all tables to produce complete weather facts enriched by date, location, and conditions.


## Requirements

* Docker
* Python 3.9+
* PostgreSQL

---

## Conclusion

This project demonstrates how to create a complete ETL pipeline, manage data with a star schema in PostgreSQL, and expose an API to access the processed data. It serves as an excellent example of building scalable data engineering solutions with Python, FastAPI, PostgreSQL, and Docker.

---

This `README.md` is structured to provide a clear explanation of the project components, data model, and how to run the project both locally and in the cloud. Let me know if you need further adjustments!
