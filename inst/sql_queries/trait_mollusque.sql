SELECT
    shared_models_sample.sample_number as IDENT_NO_TRAIT,
    shared_models_station.name as NO_STATION -- this needs to be numeric, will have to strip alphabetic characters
FROM shared_models_sample
LEFT JOIN shared_models_mission
    ON shared_models_sample.mission_id=shared_models_mission.id
LEFT JOIN shared_models_station
    ON shared_models_sample.station_id=shared_models_station.id

-- Filters
-- Just data for the active mission
WHERE shared_models_mission.is_active=1
ORDER BY shared_models_sample.id ASC
;