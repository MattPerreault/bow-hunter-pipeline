INSERT INTO population_production (
    state,
    species,
    herd_name,
    post_hunt_estimate,
    male_female_ratio,
    year,
    unit
)
SELECT
    state,
    species,
    herd_name,
    post_hunt_estimate,
    male_female_ratio,
    year,
    gmu_exploded AS unit
FROM (
    SELECT
        state,
        species,
        herd_name,
        post_hunt_estimate,
        male_female_ratio,
        year,
        UNNEST(CAST(STR_SPLIT(gmu_list, ',') AS INTEGER[])) AS gmu_exploded
    FROM population_stage
    WHERE TRIM(gmu_list) ~ '^[0-9 ,]+$'
)
ON CONFLICT (state, species, year, unit) DO UPDATE SET
    post_hunt_estimate = EXCLUDED.post_hunt_estimate,
    male_female_ratio = EXCLUDED.male_female_ratio;
