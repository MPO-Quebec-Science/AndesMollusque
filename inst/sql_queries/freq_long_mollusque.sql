SELECT
    shared_models_sample.sample_number,
    shared_models_sample.sample_number AS IDENT_NO_TRAIT,
    shared_models_referencecatch.code AS strap_code, -- will need to be converted to cod_esp_eng
--    shared_models_sizeclass.code, -- will need to be converted to COD_TYP_ETAT
    shared_models_sizeclass.description_fra, -- will need to be converted to COD_TYP_ETAT
    shared_models_observation.observation_value as VALEUR_LONG_MOLL,
    shared_models_observation.observation_type_id as observation_type_id, -- will need to be filtered to just length-type
    shared_models_specimen.id -- will be replaced with no_mollusque
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
LEFT JOIN shared_models_samplingprotocol 
	ON shared_models_samplingprotocol.id=shared_models_mission.sampling_protocol_id 
LEFT JOIN shared_models_sizeclass 
	ON shared_models_sizeclass.sampling_protocol_id=shared_models_samplingprotocol.id and shared_models_sizeclass.code=shared_models_basket.size_class
-- Filters
-- Will append these in R-code: 
-- WHERE shared_models_mission.is_active=1
-- AND shared_models_observationtype.special_type=1 -- special_type=1 is official-length
-- AND shared_models_observationtype.id=7 -- id 7 is length
