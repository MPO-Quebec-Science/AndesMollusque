SELECT 
-- DISTINCT shared_models_catch.id,
shared_models_sample.sample_number AS IDENT_NO_TRAIT,
shared_models_referencecatch.code AS strap_code, -- will need to be converted to cod_esp_eng
-- shared_models_basket.*
shared_models_sizeclass.description_fra
FROM shared_models_catch 
LEFT JOIN shared_models_sample 
	ON shared_models_catch.sample_id=shared_models_sample.id 
LEFT JOIN shared_models_mission
	ON shared_models_sample.mission_id=shared_models_mission.id 
LEFT JOIN shared_models_referencecatch 
	ON shared_models_catch.reference_catch_id=shared_models_referencecatch.id 
-- # # this part is to remove 'NA' size-class baskets
LEFT JOIN shared_models_basket 
	ON shared_models_basket.catch_id=shared_models_catch.id
LEFT JOIN shared_models_samplingprotocol 
	ON shared_models_samplingprotocol.id=shared_models_mission.sampling_protocol_id 
LEFT JOIN shared_models_sizeclass 
	ON shared_models_sizeclass.sampling_protocol_id=shared_models_samplingprotocol.id and shared_models_sizeclass.code=shared_models_basket.size_class
-- Filters
-- Just data for the active mission
-- WHERE shared_models_mission.is_active=1
	-- ORDER BY ecosystem_survey_catch.id ASC 
-- {size_class_filter_query if size_class_filter_query else ''} 
-- {aphia_id_filter_query if aphia_id_filter_query else ''} 
