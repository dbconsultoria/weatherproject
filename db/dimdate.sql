DO $$
DECLARE
    d DATE := '2000-01-01';
    end_date DATE := CURRENT_DATE;
BEGIN
    WHILE d <= end_date LOOP
        INSERT INTO dim.date (full_date, year, month, day, weekday, is_weekend)
        VALUES (
            d,
            EXTRACT(YEAR FROM d),
            EXTRACT(MONTH FROM d),
            EXTRACT(DAY FROM d),
            EXTRACT(DOW FROM d),
            EXTRACT(DOW FROM d) IN (0, 6)  -- Sunday=0, Saturday=6
        )
        ON CONFLICT (full_date) DO NOTHING;  -- prevent duplicates

        d := d + INTERVAL '1 day';
    END LOOP;
END $$;
