INSERT INTO elk_population_stage 
SELECT 
    dau,
    herd_name,
    gmu_list,
    post_hunt_estimate,
    bull_cow_ratio, 
    year 
FROM read_parquet('./data/processed/elk/population/*/*.parquet')
ON CONFLICT (herd_name, year) DO UPDATE SET
    dau = EXCLUDED.dau,
    gmu_list = EXCLUDED.gmu_list,
    post_hunt_estimate = EXCLUDED.post_hunt_estimate,
    bull_cow_ratio = EXCLUDED.bull_cow_ratio;
