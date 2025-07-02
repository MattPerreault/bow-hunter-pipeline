INSERT INTO elk_harvest
SELECT
    unit,
    bulls,
    cows,
    calves,
    total_harvest,
    total_hunters,
    percent_success,
    total_rec_days,
    year,
    season
FROM read_parquet('./data/processed/elk/harvest/*/*/*.parquet')
WHERE unit IS NOT NULL
ON CONFLICT (unit, year, season) DO UPDATE SET
    bulls = EXCLUDED.bulls,
    cows = EXCLUDED.cows,
    calves = EXCLUDED.calves,
    total_harvest = EXCLUDED.total_harvest,
    total_hunters = EXCLUDED.total_hunters,
    percent_success = EXCLUDED.percent_success,
    total_rec_days = EXCLUDED.total_rec_days;
