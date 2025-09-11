SELECT 
	shared_models_specimen.id as id_specimen,
	-- secteur
    shared_models_sample.sample_number AS trait,
    shared_models_sample.start_date AS set_start_date,
    shared_models_station.name AS station,
	shared_models_observationgroup.nom as collection,
    MAX(CASE WHEN (shared_models_observationtype.export_name='code_coquille') THEN shared_models_observation.observation_value ELSE '' END) AS code_coquille,
    MAX(CASE WHEN (shared_models_observationtype.export_name='taille') THEN shared_models_observation.observation_value ELSE '' END) AS taille,
    MAX(CASE WHEN (shared_models_observationtype.export_name='poids_vif') THEN shared_models_observation.observation_value ELSE '' END) AS pds_vif,
    MAX(CASE WHEN (shared_models_observationtype.export_name='poids_muscle') THEN shared_models_observation.observation_value ELSE '' END) AS pds_musc,
    MAX(CASE WHEN (shared_models_observationtype.export_name='poids_gonade') THEN shared_models_observation.observation_value ELSE '' END) AS pds_gon,
    MAX(CASE WHEN (shared_models_observationtype.export_name='poids_visceres') THEN shared_models_observation.observation_value ELSE '' END) AS pds_visc,
    MAX(CASE WHEN (shared_models_observationtype.export_name='sex') THEN shared_models_observation.observation_value ELSE '' END) AS sexe,
    MAX(CASE WHEN (shared_models_observationtype.special_type='7') THEN shared_models_observation.observation_value ELSE '' END) AS collect_specimen,
    shared_models_specimen.comment as comment,
    shared_models_referencecatch.code as strap
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
LEFT JOIN shared_models_station
    ON shared_models_station.id=shared_models_sample.station_id
LEFT JOIN shared_models_mission
    ON shared_models_mission.id=shared_models_sample.mission_id
LEFT JOIN shared_models_samplingprotocol 
	ON shared_models_samplingprotocol.id=shared_models_mission.sampling_protocol_id 
LEFT JOIN shared_models_samplingrequirement
	ON shared_models_samplingrequirement.sampling_protocol_id=shared_models_mission.sampling_protocol_id
LEFT JOIN shared_models_observationgroup
	ON shared_models_observationgroup.sampling_requirement_id=shared_models_samplingrequirement.id
-- Filters
-- -- Will append these in R-code: 
-- WHERE shared_models_mission.is_active=1
-- AND shared_models_observationgroup.nom="Conserver pour biométrie 16E"
-- GROUP BY
-- 	shared_models_specimen.id,
-- 	shared_models_observationgroup.nom,
-- 	shared_models_sample.sample_number,
-- 	shared_models_specimen.comment,
-- 	shared_models_referencecatch.code
-- HAVING collect_specimen=1
-- ORDER BY code_coquille ASC

    
