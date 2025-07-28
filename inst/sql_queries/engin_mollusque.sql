SELECT
	shared_models_mission.id,
-- Filters
-- Just data for the active mission
WHERE shared_models_mission.is_active=1
;