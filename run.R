
devtools::load_all()
devtools::document()

url_bd <- "iml-science-4.ent.dfo-mpo.ca"
port_bd <- 25988
nom_bd <- "andesdb"
nom_usager <- Sys.getenv("NOM_USAGER_BD")
mot_de_passe <- Sys.getenv("MOT_DE_PASSE_BD")

# établir connexion BD (il faut être sur le réseau MPO)
andes_db_connection <- andes_db_connect(
  url_bd = url_bd,
  port_bd = port_bd,
  nom_usager = nom_usager,
  mot_de_passe = mot_de_passe,
  nom_bd = nom_bd
)

################################################
# EXTERNAL INPUT

# "Indice d'abondance zone 16E - pétoncle"
# "Indice d'abondance zone 16F - pétoncle"
# "Indice d'abondance zone 20 - pétoncle"
# "Indice d'abondance buccin"
desc_serie_hist_f <- "Indice d'abondance zone 16E - pétoncle"

proj <- get_projet_mollusc(andes_db_connection)

key <- get_ref_key(
    table = "Source_Info",
    pkey_col = "COD_SOURCE_INFO",
    col = "DESC_SOURCE_INFO_F",
    val = "Évaluation de stocks IML - Pétoncle Minganie")
key

DBI::dbDisconnect(access_db_connection)
