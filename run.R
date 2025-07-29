
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
  trait <- get_trait_mollusque(andes_db_connection, proj = p)
  # trait_db <- get_trait_mollusque_db(andes_db_connection)
}

trait <- get_trait_mollusque(andes_db_connection, proj = proj)
View(trait)
View(trait)




names(trait)



file_path <- create_new_access_db()
access_db_write_connection <- access_db_connect(paste("./", file_path, sep=""))


write_projet_mollusque(proj, access_db_write_connection)

write_trait_mollusque(trait, access_db_write_connection)
# CANNOT WRITE, this following statement fails with error: Type de données incompatible dans l'expression du critèr
" INSERT INTO TRAIT_MOLLUSQUE (IDENT_NO_TRAIT, NO_STATION, COD_RESULT_OPER, DATE_DEB_TRAIT, DATE_FIN_TRAIT, LAT_DEB_TRAIT, LAT_FIN_TRAIT, LONG_DEB_TRAIT, LONG_FIN_TRAIT, PROF_DEB, PROF_FIN, REM_TRAIT_MOLL, COD_SOURCE_INFO, NO_RELEVE, COD_NBPC, NO_CHARGEMENT, COD_STRATE, COD_ZONE_GEST_MOLL, COD_TYP_TRAIT, HRE_DEB_TRAIT, HRE_FIN_TRAIT, COD_TYP_HEURE, COD_FUSEAU_HORAIRE, COD_METHOD_POS, LATLONG_P, DISTANCE_POS, DISTANCE_POS_P, VIT_TOUAGE, VIT_TOUAGE_P, DUREE_TRAIT, DUREE_TRAIT_P, TEMP_FOND, TEMP_FOND_P, PROF_DEB_P, PROF_FIN_P)  VALUES (' 1', '01', '1', '2025-05-03', '2025-05-03', '5011.543', '5011.501', '-6335.832', '-6335.729', '52', '52', 'Andes n''a pas fonctionné sur ce trait.\n**start/end dates repopulated**', '18', '36', '4', NULL, ' 8', '7', '10', '2025-05-03 10:44:45', '2025-05-03 10:47:20', ' 1', '1', '6', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)  ;\n"



DBI::dbDisconnect(access_db_write_connection)
