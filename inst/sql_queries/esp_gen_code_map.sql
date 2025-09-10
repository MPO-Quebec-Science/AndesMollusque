-- This is meant to be run on MSAccess or Oracle databases
-- It returns a map between COD_ESP_GEN and COD_STRAP
-- on Access DBs NOM_NORME is called STRAP, but on Oracle it is called STRAP_IML :)
-- This is only meant to be used on the Access DB but might as well write the query so it is agnostic
-- Please delete these comments before sending the query, the DB engin goes not support comments. 
SELECT 
    ESPECE_NORME.COD_ESP_GEN,
    ESPECE_NORME.COD_ESPECE
FROM ESPECE_NORME
LEFT JOIN NORME
    ON ESPECE_NORME.COD_NORME=NORME.COD_NORME
WHERE (NORME.NOM_NORME='STRAP' OR NORME.NOM_NORME='STRAP_IML')
