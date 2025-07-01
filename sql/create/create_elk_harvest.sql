CREATE TABLE IF NOT EXISTS elk_harvest (
    unit BIGINT,
    bulls BIGINT,
    cows BIGINT,
    calves BIGINT,
    total_harvest BIGINT,
    total_hunters BIGINT,
    percent_success BIGINT,
    total_rec_days BIGINT,
    year INTEGER,
    season VARCHAR,
    PRIMARY KEY (unit, year, season)
)