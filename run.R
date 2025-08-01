
rm()
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
desc_serie_hist_f <- "Indice d'abondance zone 16E - pétoncle"

devtools::load_all()


proj <- get_projet_mollusque(andes_db_connection)

proj <- init_cod_serie_hist(proj, desc_serie_hist_f)
validate_projet_mollusque(proj)

# proj -> trait -> engine -> capture -> FreqLongMollusque -> 
# BiometrieMollusque 
# PoidsBiometrie

devtools::load_all()

# usefull strap codes:
cod_petoncle_island <- 4167
cod_petoncle_geant <- 4179

code_filter <- c(cod_petoncle_island)

code_filter <- c(4167, 4179)
basket_class_filter <- c(4167, 4179)


p_i <- 1
for (p_i in seq_len(nrow(proj))) {
  trait <- get_trait_mollusque(andes_db_connection, proj = proj[p_i,])
  validate_trait_mollusque(trait)

  engin <- get_engin_mollusque(andes_db_connection, proj = proj[p_i,])
  validate_engin_mollusque(engin)
}
# choose which species we want to have using code_filter
# choose which basket class have using basket_class_filter

capt <- get_caputre_mollusque(andes_db_connection, proj, code_filter=code_filter, basket_class_filter=basket_class_filter)

trait <- get_trait_mollusque(andes_db_connection, proj = proj)
View(trait)
View(trait)




names(trait)


devtools::load_all()

file_path <- create_new_access_db()
access_db_write_connection <- access_db_connect(paste("./", file_path, sep = ""))

write_projet_mollusque(proj, access_db_write_connection)

write_trait_mollusque(trait, access_db_write_connection)

write_engin_mollusque(engin, access_db_write_connection)

DBI::dbDisconnect(access_db_write_connection)
