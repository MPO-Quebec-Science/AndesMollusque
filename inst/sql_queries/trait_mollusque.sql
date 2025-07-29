SELECT
    shared_models_sample.sample_number AS IDENT_NO_TRAIT,
    shared_models_station.name AS NO_STATION, -- this needs to be numeric, will have to strip alphabetic characters
    shared_models_mission.area_of_operation AS desc_secteur_releve_f, -- this ANDES field take a special role to determine COD_SECTEUR_RELEVE
    shared_models_operation.abbrev AS operation, -- fish or ctd, will help with COD_TYP_TRAIT if ctd
    shared_models_stratificationtype.description_fra AS desc_stratification, -- will help with COD_TYP_TRAIT if operation is fish
    MAX(CASE WHEN (shared_models_sampleobservationtype.export_name='set_result') THEN value ELSE '' END) AS COD_RESULT_OPER,
    shared_models_sample.start_date AS DATE_DEB_TRAIT,
    shared_models_sample.end_date AS DATE_FIN_TRAIT,
    -- also copy these previous two for HEURE_DEB_TRAIT and HEURE_DEB_TRAIT
    shared_models_sample.start_latitude AS LAT_DEB_TRAIT,
    shared_models_sample.end_latitude AS LAT_FIN_TRAIT,
    shared_models_sample.start_longitude AS LON_DEB_TRAIT,
    shared_models_sample.end_longitude AS LON_FIN_TRAIT,
    MAX(CASE WHEN (shared_models_sampleobservationtype.export_name='start_depth_m') THEN value ELSE '' END) AS PROF_DEB,
    MAX(CASE WHEN (shared_models_sampleobservationtype.export_name='end_depth_m') THEN value ELSE '' END) AS PROF_FIN,
    shared_models_sample.remarks AS REM_TRAIT_MOLL

FROM shared_models_sampleobservation
LEFT JOIN shared_models_sampleobservationtype
	ON shared_models_sampleobservationtype.id=shared_models_sampleobservation.sample_observation_type_id
LEFT JOIN shared_models_sample 
	ON shared_models_sample.id=shared_models_sampleobservation.sample_id
LEFT JOIN shared_models_station
    ON shared_models_sample.station_id=shared_models_station.id
LEFT JOIN shared_models_mission
    ON shared_models_sample.mission_id=shared_models_mission.id
LEFT JOIN shared_models_sample_operations
    ON shared_models_sample.id=shared_models_sample_operations.sample_id
LEFT JOIN shared_models_operation
    ON shared_models_operation.id=shared_models_sample_operations.operation_id
LEFT JOIN shared_models_stratificationtype 
	ON shared_models_stratificationtype.id=shared_models_mission.stratification_type_id
-- Filters
-- Just data for the active mission
WHERE shared_models_mission.is_active=1
GROUP BY
    IDENT_NO_TRAIT,
    NO_STATION, -- this needs to be numeric, will have to strip alphabetic characters
    desc_secteur_releve_f, -- this ANDES field take a special role to determine COD_SECTEUR_RELEVE
    operation, -- fish or ctd, will help with COD_TYP_TRAIT if ctd
    desc_stratification,
    DATE_DEB_TRAIT,
    DATE_FIN_TRAIT,
    LAT_DEB_TRAIT,
    LAT_FIN_TRAIT,
    LON_DEB_TRAIT,
    LON_FIN_TRAIT,
    REM_TRAIT_MOLL
ORDER BY IDENT_NO_TRAIT ASC
;