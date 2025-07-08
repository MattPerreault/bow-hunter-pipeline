DELETE FROM population_stage;

INSERT INTO population_stage (
    state,
    species,
    herd_name,
    post_hunt_estimate,
    male_female_ratio,
    year,
    gmu_list
)
SELECT
    state,
    species,
    herd_name,
    post_hunt_estimate,
    male_female_ratio,
    year,
    gmu_list
FROM read_parquet('s3://grand-lake/processed/*/*/population/*/*.parquet')