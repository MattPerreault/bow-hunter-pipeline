CREATE TABLE IF NOT EXISTS population_production (
    state VARCHAR,
    species VARCHAR,
    herd_name VARCHAR,
    post_hunt_estimate BIGINT,
    male_female_ratio DOUBLE,
    year INT,
    unit INT,
    PRIMARY KEY (state, species, year, unit)
);
