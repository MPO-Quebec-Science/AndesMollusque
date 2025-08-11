-- This is meant to be executed against an ANDES DB
SELECT
    shared_models_sample.sample_number AS IDENT_NO_TRAIT,
    MAX(CASE WHEN (shared_models_sampleobservationtype.export_name='NO_ENGIN') THEN value ELSE '' END) AS NO_ENGIN,
    MAX(CASE WHEN (shared_models_sampleobservationtype.export_name='gear_type') THEN value ELSE '' END) AS COD_ENG_GEN,
    MAX(CASE WHEN (shared_models_sampleobservationtype.export_name='fill_percent') THEN value ELSE '' END) AS REMPLISSAGE
FROM shared_models_sampleobservation
LEFT JOIN shared_models_sampleobservationtype
	ON shared_models_sampleobservationtype.id=shared_models_sampleobservation.sample_observation_type_id
LEFT JOIN shared_models_sample 
	ON shared_models_sample.id=shared_models_sampleobservation.sample_id
LEFT JOIN shared_models_mission
    ON shared_models_sample.mission_id=shared_models_mission.id
-- Filters
-- Just data for the active mission
WHERE shared_models_mission.is_active=1
GROUP BY
    IDENT_NO_TRAIT
ORDER BY IDENT_NO_TRAIT ASC
