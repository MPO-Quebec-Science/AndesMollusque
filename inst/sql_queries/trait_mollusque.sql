SELECT
    shared_models_sample.sample_number as IDENT_NO_TRAIT,
    shared_models_station.name as NO_STATION, -- this needs to be numeric, will have to strip alphabetic characters
    shared_models_mission.area_of_operation as desc_secteur_releve_f, -- this ANDES field take a special role to determine COD_SECTEUR_RELEVE
    shared_models_operation.abbrev as operation, -- fish or ctd, will help with COD_TYP_TRAIT if ctd
    shared_models_stratificationtype.description_fra as desc_stratification -- will help with COD_TYP_TRAIT if operation is fish
FROM shared_models_sample
LEFT JOIN shared_models_mission
    ON shared_models_sample.mission_id=shared_models_mission.id
LEFT JOIN shared_models_station
    ON shared_models_sample.station_id=shared_models_station.id
LEFT JOIN shared_models_sample_operations
    ON shared_models_sample.id=shared_models_sample_operations.sample_id
LEFT JOIN shared_models_operation
    ON shared_models_operation.id=shared_models_sample_operations.operation_id
LEFT JOIN shared_models_stratificationtype 
ON shared_models_stratificationtype.id=shared_models_mission.stratification_type_id
-- Filters
-- Just data for the active mission
WHERE shared_models_mission.is_active=1
ORDER BY shared_models_sample.id ASC
;