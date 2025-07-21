SELECT
	shared_models_mission.id,
	shared_models_mission.description as DESC_SOURCE_INFO_F, -- this descr will be mapped to COD_SOURCE_INFO
    shared_models_mission.survey_number as NO_RELEVE,
    shared_models_mission.vessel_nbpc as COD_NBPC,
    shared_models_mission.season as ANNEE,
    shared_models_stratificationtype.code as COD_TYP_STRATIF, -- make sure ANDES code is setup to correspond with the Oracle code
    -- shared_models_stratificationtype.description_fra as stratificationtype_description_fra -- Consider validating the code with the description
    shared_models_mission.start_date as DATE_DEB_PROJET,
    shared_models_mission.end_date as DATE_FIN_PROJET,
    shared_models_mission.mission_number as NO_NOTIF_IML,
    shared_models_mission.chief_scientist as CHEF_MISSION,
    shared_models_mission.vessel_name as vessel_name, -- this hacking of the vessel name will be be re-formatted as SEQ_PECHEUR 
    shared_models_mission.targeted_trawl_duration as DUREE_TRAIT_VISEE,
    shared_models_mission.targeted_trawl_speed as VIT_TOUAGE_VISEE,
    shared_models_mission.targeted_trawl_distance as DIST_CHALUTE_VISEE,
    shared_models_mission.samplers as NOM_SCIENCE_NAVIRE,
    shared_models_mission.notes as REM_PROJET_MOLL -- make sure this is under 255 characters
    FROM shared_models_mission 
    LEFT JOIN shared_models_stratificationtype
        ON shared_models_mission.stratification_type_id=shared_models_stratificationtype.id
-- Filters
-- Just data for the active mission
WHERE shared_models_mission.is_active=1
;