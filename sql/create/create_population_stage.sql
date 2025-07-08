CREATE TABLE IF NOT EXISTS population_stage (
    state VARCHAR,
    species VARCHAR,
    herd_name VARCHAR,
    post_hunt_estimate BIGINT,
    male_female_ratio DOUBLE,
    year INT,
    gmu_list VARCHAR
);
