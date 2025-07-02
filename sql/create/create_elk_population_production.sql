CREATE OR REPLACE TABLE elk_population AS
WITH exploded AS (
	SELECT
	    herd_name,
	    post_hunt_estimate,
	    bull_cow_ratio,
	    year,
	    UNNEST(
	    	CASE
	    		WHEN LEFT(TRIM(gmu_list), 1) = '[' THEN
	    			CAST(json_extract(gmu_list, '$') AS INTEGER[])
	    		ELSE
	    			CAST(STR_SPLIT(gmu_list, ',') AS INTEGER[])
	    	END
	    ) AS unit
	FROM elk_population_stage
	WHERE gmu_list != 'notin' AND gmu_list IS NOT NULL
)
SELECT *
FROM exploded
WHERE unit IS NOT NULL
