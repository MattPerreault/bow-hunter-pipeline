CREATE TABLE IF NOT EXISTS elk_population (
    dau VARCHAR,
    herd_name VARCHAR,
    gmu_list VARCHAR,
    post_hunt_estimate BIGINT,
    bull_cow_ratio DOUBLE,
    year BIGINT,
    PRIMARY KEY (herd_name, year)
)
