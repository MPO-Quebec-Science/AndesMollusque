-- This CTE provides a list of responses for couverture balanes (usually the first 30 specimens of the set)
WITH balane_cte AS (
    SELECT
        shared_models_sample.sample_number,
        shared_models_referencecatch.code,
        shared_models_observation.observation_value
    FROM shared_models_specimen
    LEFT JOIN shared_models_observation
        ON shared_models_specimen.id=shared_models_observation.specimen_id
    LEFT JOIN shared_models_observationtype
        ON shared_models_observationtype.id=shared_models_observation.observation_type_id
    LEFT JOIN shared_models_basket
        ON shared_models_basket.id=shared_models_specimen.basket_id
    LEFT JOIN shared_models_catch
        ON shared_models_catch.id=shared_models_basket.catch_id
    LEFT JOIN shared_models_referencecatch
        ON shared_models_catch.reference_catch_id=shared_models_referencecatch.id
    LEFT JOIN shared_models_sample
        ON shared_models_sample.id=shared_models_catch.sample_id
    LEFT JOIN shared_models_mission
        ON shared_models_mission.id=shared_models_sample.mission_id
    WHERE shared_models_mission.is_active=1
    AND shared_models_observationtype.export_name='couverture_balanes'
    ),
-- We would like to know the percent of specimens that is affected by the presence of barnacles.
-- build a second CTE that looks at the proportion that has barnacles (i.e., whose observation_value is not 0)
-- This transforms the specimen-level observation into a catch-level observation (what the legacy-OracleDB wants)
abondance_epibiont_cte AS (
    SELECT 
        AVG(CASE WHEN (balane_cte.observation_value=0) THEN 0 ELSE 1 END) AS avg_affected,
        balane_cte.sample_number,
        balane_cte.code
    FROM balane_cte
    GROUP BY
    balane_cte.sample_number,
    balane_cte.code
),
-- We would like to know the average surface coverage of those that are affected by the presence of barnacles.
-- build a second CTE that looks at the proportion that has barnacles (i.e., whose observation_value is not 0)
-- This transforms the specimen-level observation into a catch-level observation (what the legacy-OracleDB wants)
couverture_epibiont_cte AS (
    SELECT 
        AVG(
            CASE 
                WHEN (balane_cte.observation_value=1) THEN (0+1)/6.0 -- use midpoint
                WHEN (balane_cte.observation_value=2) THEN (1+2)/6.0 -- use midpoint
                WHEN (balane_cte.observation_value=3) THEN (2+3)/6.0 -- use midpoint
            END
        ) AS avg_covered,
        balane_cte.sample_number,
        balane_cte.code
    FROM balane_cte
    GROUP BY
    balane_cte.sample_number,
    balane_cte.code
),
-- Finally, combine the two metrics as a catch-level table
-- The table includes a strap column which will need to be filtered per-catch later
-- The table reports raw values which will need to be converted into a category code later
-- This transforms the specimen-level observation into a catch-level observation (what the legacy-OracleDB wants)
epibiont_cte AS (
    SELECT
        couverture_epibiont_cte.sample_number AS NO_TRAIT,
        couverture_epibiont_cte.code AS COD_STRAP,
        abondance_epibiont_cte.avg_affected AS abondance_epibiont,
        couverture_epibiont_cte.avg_covered AS couverture_epibiont
    FROM couverture_epibiont_cte
    LEFT JOIN abondance_epibiont_cte ON abondance_epibiont_cte.sample_number=couverture_epibiont_cte.sample_number AND abondance_epibiont_cte.code=couverture_epibiont_cte.code
)
-- For debugging, you may view the cte tables by finishing the above SQL statement with a SELECT command
-- SELECT * FROM balane_cte
-- SELECT * FROM abondance_epibiont_cte
-- SELECT * FROM couverture_epibiont_cte
-- SELECT * FROM epibiont_cte
-- For using, you may load the cte tables by sourcing this file before your SQL statement.
