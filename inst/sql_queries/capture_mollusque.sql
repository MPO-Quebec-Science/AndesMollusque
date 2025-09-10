-- This is meant to be executed against an ANDES DB
SELECT 
-- DISTINCT shared_models_catch.id,
shared_models_sample.sample_number AS IDENT_NO_TRAIT,
shared_models_referencecatch.code AS strap_code, -- will need to be converted to cod_esp_eng
shared_models_sizeclass.description_fra,
MAX(CASE WHEN (shared_models_sampleobservationtype.export_name='number_of_successful_dredge_baskets') THEN value ELSE '' END) AS FRACTION_PECH
FROM shared_models_catch 
LEFT JOIN shared_models_sample 
	ON shared_models_catch.sample_id=shared_models_sample.id 
LEFT JOIN shared_models_mission
	ON shared_models_sample.mission_id=shared_models_mission.id 
LEFT JOIN shared_models_referencecatch 
	ON shared_models_catch.reference_catch_id=shared_models_referencecatch.id 
LEFT JOIN shared_models_sampleobservation 
	ON shared_models_sample.id=shared_models_sampleobservation.sample_id
LEFT JOIN shared_models_sampleobservationtype
	ON shared_models_sampleobservationtype.id=shared_models_sampleobservation.sample_observation_type_id
-- # # this part is to remove 'NA' and Biodiversity size-class baskets
LEFT JOIN shared_models_basket 
	ON shared_models_basket.catch_id=shared_models_catch.id
LEFT JOIN shared_models_samplingprotocol 
	ON shared_models_samplingprotocol.id=shared_models_mission.sampling_protocol_id 
LEFT JOIN shared_models_sizeclass 
	ON shared_models_sizeclass.sampling_protocol_id=shared_models_samplingprotocol.id and shared_models_sizeclass.code=shared_models_basket.size_class
-- Need to add these...
-- WHERE shared_models_mission.is_active=1
-- GROUP BY
--     IDENT_NO_TRAIT,
--     strap_code, 
--     shared_models_sizeclass.description_fra
-- ORDER BY IDENT_NO_TRAIT ASC

