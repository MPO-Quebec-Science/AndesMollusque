
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

# proj -> trait -> engine -> capture -> FreqLongMollusque -> 
# BiometrieMollusque 
# PoidsBiometrie

devtools::load_all()

for(pi in seq_len(nrow(proj))) {
  p <- proj[pi,]
  trait <- get_trait_mollusque(andes_db_connection, proj=p)
  # trait_db <- get_trait_mollusque_db(andes_db_connection)
}

trait <- get_trait_mollusque(andes_db_connection, proj=proj)
View(trait)
View(trait)

# then, lookup the strat code
trait$COD_STRAT <- lookup_cod_strat(strat)



trait

lapply(trait$NO_STATION, strip_alphabetic)
trait$NO_STATION

format_no_station(trait)

format_cod_zone_gest_moll(trait)
determine_zone_name(trait)
trait
sapply()

determine_zone_name("dsd", desc_serie_hist_f)

apply(trait, determine_zone_name)
trait

file_path <- create_new_access_db()
access_db_write_connection <- access_db_connect(paste("./", file_path, sep=""))


write_projet_mollusque(proj, access_db_write_connection)

DBI::dbDisconnect(access_db_write_connection)
