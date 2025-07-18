SELECT
	shared_models_mission.id,
	shared_models_mission.description as DESC_SOURCE_INFO_F, -- this be mapped to a COD_SOURCE_INFO
    shared_models_mission.survey_number as NO_RELEVE,
    shared_models_mission.vessel_nbpc as COD_NBPC,
    shared_models_mission.season as ANNEE
-- Filters
    -- Just data for the active mission
FROM shared_models_mission 
WHERE shared_models_mission.is_active=1
;