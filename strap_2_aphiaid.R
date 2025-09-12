
query = 
    "WITH strap2aphia
    AS (
        SELECT *
        FROM (
            SELECT COD_ESP_GEN, COD_ESPECE, NOM_NORME 
            FROM ESPECE_NORME
            LEFT JOIN NORME
            ON ESPECE_NORME.COD_NORME=NORME.COD_NORME
            )
            PIVOT(
            sum(COD_ESPECE)
            FOR NOM_NORME
            IN ('STRAP_IML', 'AphiaId')
            )
    )
    SELECT *
    FROM strap2aphia
    LEFT JOIN ESPECE_GENERAL
    ON ESPECE_GENERAL.COD_ESP_GEN = strap2aphia.COD_ESP_GEN
    ;"

library(DBI)
con <- dbConnect(odbc::odbc(), "IMLP", UID="pse_iml_ro", PWD="Pseimlro_1!")

user <- "pse_iml_ro"
password <- "Pseimlro_1!"

host <- "NATP71.NAT.DFO-MPO.CA"
database <- "OKENP27"
port <- 1523

  connection = dbConnect(
    odbc(), 
    dsns[[db]], 
    UID = uid, 
    PWD = pwd, 
    encoding = "latin1"
  )