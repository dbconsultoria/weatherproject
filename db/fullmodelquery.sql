SELECT DISTINCT
    dt.full_date,
    dt.year,
    dt.month,
    dt.day,
    dt.weekday,
    dt.is_weekend,

    c.city_name,
    co.country_name,

    cond.condition_name,
    cond.description,

    f.temp,
    f.insert_time

FROM fact.temperature f
JOIN dim.date dt ON f.date_id = dt.date_id
JOIN dim.city c ON f.city_id = c.city_id
JOIN dim.country co ON c.country_id = co.country_id
JOIN dim.conditions cond ON f.condition_id = cond.condition_id;
