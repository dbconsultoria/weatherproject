-- =======================================
-- CREATE TABLE dim.city
-- =======================================
CREATE TABLE dim.city (
    city_id integer DEFAULT nextval('dim.city_city_id_seq'::regclass) NOT NULL,
    city_name character varying NOT NULL,
    country_id integer,
    PRIMARY KEY (city_id),
    FOREIGN KEY (country_id) REFERENCES dim.country(country_id)
);

-- =======================================
-- CREATE TABLE dim.conditions
-- =======================================
CREATE TABLE dim.conditions (
    condition_id integer DEFAULT nextval('dim.conditions_condition_id_seq'::regclass) NOT NULL,
    condition_name character varying NOT NULL,
    description text,
    PRIMARY KEY (condition_id)
);

-- =======================================
-- CREATE TABLE dim.country
-- =======================================
CREATE TABLE dim.country (
    country_id integer DEFAULT nextval('dim.country_country_id_seq'::regclass) NOT NULL,
    country_name character varying NOT NULL,
    PRIMARY KEY (country_id)
);

-- =======================================
-- CREATE TABLE dim.date
-- =======================================
CREATE TABLE dim.date (
    date_id integer DEFAULT nextval('dim.date_date_id_seq'::regclass) NOT NULL,
    full_date date NOT NULL,
    year integer,
    month integer,
    day integer,
    weekday integer,
    is_weekend boolean,
    PRIMARY KEY (date_id)
);

-- =======================================
-- CREATE TABLE fact.temperature
-- =======================================
CREATE TABLE fact.temperature (
    temperature_id integer DEFAULT nextval('fact.temperature_temperature_id_seq'::regclass) NOT NULL,
    date_id integer,
    city_id integer,
    condition_id integer,
    temp real,
    insert_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (temperature_id),
    FOREIGN KEY (date_id) REFERENCES dim.date(date_id),
    FOREIGN KEY (city_id) REFERENCES dim.city(city_id),
    FOREIGN KEY (condition_id) REFERENCES dim.conditions(condition_id)
);

-- =======================================
-- CREATE TABLE stage.weathervc
-- =======================================
CREATE TABLE stage.weathervc (
    id integer DEFAULT nextval('stage.weathervc_id_seq'::regclass) NOT NULL,
    city character varying NOT NULL,
    country character varying DEFAULT 'Brazil'::character varying,
    date date NOT NULL,
    temp real,
    conditions character varying,
    description character varying,
    insert_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- =======================================
-- STORED PROCEDURES AND FUNCTIONS
-- =======================================

-- public.a (PROCEDURE)
-- Language: plpgsql, Returns: None
CREATE OR REPLACE PROCEDURE public.a()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    d DATE := '2000-01-01';
    end_date DATE := '2099-01-01';
BEGIN
    WHILE d <= end_date LOOP
        INSERT INTO dim.date (full_date, year, month, day, weekday, is_weekend)
        VALUES (
            d,
            EXTRACT(YEAR FROM d),
            EXTRACT(MONTH FROM d),
            EXTRACT(DAY FROM d),
            EXTRACT(DOW FROM d),
            EXTRACT(DOW FROM d) IN (0, 6)  -- Sunday = 0, Saturday = 6
        )
        ON CONFLICT (full_date) DO NOTHING;

        d := d + INTERVAL '1 day';
    END LOOP;

    RAISE NOTICE '✅ dim.date populated from 2000-01-01 to 2099';
END;
$procedure$

-- public.load_dim_city (PROCEDURE)
-- Language: plpgsql, Returns: None
CREATE OR REPLACE PROCEDURE public.load_dim_city()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO dim.city (city_name, country_id)
    SELECT DISTINCT
        s.city,
        c.country_id
    FROM stage.weathervc s
    JOIN dim.country c ON c.country_name = s.country
    WHERE s.city IS NOT NULL
    ON CONFLICT (city_name) DO NOTHING;

    RAISE NOTICE '✅ dim.city populated successfully.';
END;
$procedure$

-- public.load_dim_conditions (PROCEDURE)
-- Language: plpgsql, Returns: None
CREATE OR REPLACE PROCEDURE public.load_dim_conditions()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO dim.conditions (condition_name, description)
    SELECT DISTINCT
        conditions,
        description
    FROM stage.weathervc
    WHERE conditions IS NOT NULL
    ON CONFLICT (condition_name) DO NOTHING;

    RAISE NOTICE '✅ dim.conditions populated successfully.';
END;
$procedure$

-- public.load_dim_country (PROCEDURE)
-- Language: plpgsql, Returns: None
CREATE OR REPLACE PROCEDURE public.load_dim_country()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO dim.country (country_name)
    SELECT DISTINCT country
    FROM stage.weathervc
    WHERE country IS NOT NULL
    ON CONFLICT (country_name) DO NOTHING;
    
    RAISE NOTICE '✅ dim.country populated successfully.';
END;
$procedure$

-- public.load_dim_date (PROCEDURE)
-- Language: plpgsql, Returns: None
CREATE OR REPLACE PROCEDURE public.load_dim_date()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    d DATE := '2000-01-01';
    end_date DATE := '2099-01-01';
BEGIN
    WHILE d <= end_date LOOP
        INSERT INTO dim.date (full_date, year, month, day, weekday, is_weekend)
        VALUES (
            d,
            EXTRACT(YEAR FROM d),
            EXTRACT(MONTH FROM d),
            EXTRACT(DAY FROM d),
            EXTRACT(DOW FROM d),
            EXTRACT(DOW FROM d) IN (0, 6)  -- Sunday = 0, Saturday = 6
        )
        ON CONFLICT (full_date) DO NOTHING;

        d := d + INTERVAL '1 day';
    END LOOP;

    RAISE NOTICE '✅ dim.date populated from 2000-01-01 to %', end_date;
END;
$procedure$

-- public.merge_fact_temperature (PROCEDURE)
-- Language: plpgsql, Returns: None
CREATE OR REPLACE PROCEDURE public.merge_fact_temperature()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO fact.temperature (date_id, city_id, condition_id, temp)
    SELECT
        d.date_id,
        ci.city_id,
        co.condition_id,
        s.temp
    FROM stage.weathervc s
    JOIN dim.date d ON d.full_date = s.date
    JOIN dim.city ci ON ci.city_name = s.city
    JOIN dim.conditions co ON co.condition_name = s.conditions
    WHERE s.temp IS NOT NULL
    ON CONFLICT (date_id, city_id) DO UPDATE
    SET
        condition_id = EXCLUDED.condition_id,
        temp = EXCLUDED.temp,
        insert_time = CURRENT_TIMESTAMP;

    RAISE NOTICE '✅ fact.temperature merged successfully.';
END;
$procedure$

