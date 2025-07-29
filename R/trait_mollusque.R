
#' Gets trait_mollusque_db (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the TRAIT_MOLLUSQUE table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_trait_mollusque` 
#'
#' @param andes_db_connection a connection object to the ANDES database. 
#' @return A dataframe containing fishing set data.
#' @seealso [get_trait_mollusque()] for the formatted results
#' @export
get_trait_mollusque_db<- function(andes_db_connection) {
    query <- readr::read_file(system.file("sql_queries",
                                          "trait_mollusque.sql",
                                          package = "ANDESMollusque"))
    result <- DBI::dbSendQuery(andes_db_connection, query)
    proj <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)
    return(proj)
}


#' Gets trait_mollusque (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data 
#' to construct the TRAIT_MOLLUSQUE table and formats the results.
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @param proj A formatted projet_mollusque dataframe
#' @return A dataframe containing formatted fishing set data.
#' @export
get_trait_mollusque <- function(andes_db_connection, proj = NULL) {
    # Validate input
    if (is.null(proj)) {
        logger::log_error("Must provide a formatted projet_mollusque dataframe.")
        stop("Must provide a formatted projet_mollusque dataframe.")
    }
    
    # Get raw data
    trait <- get_trait_mollusque_db(andes_db_connection)
    
    # Take data from projet_mollusque
    trait$COD_SOURCE_INFO <- proj$COD_SOURCE_INFO
    trait$NO_RELEVE <- proj$NO_RELEVE
    trait$COD_NBPC <- proj$COD_NBPC
    trait$NO_CHARGEMENT <- proj$NO_CHARGEMENT



    # temporarily get desc_serie_hist_f from proj to trait, it will provide the context to correctly get the zone/strate
    desc_serie_hist_f <- get_ref_key(
        table = "Indice_Suivi_Etat_Stock",
        pkey_col = "DESC_SERIE_HIST_F",
        col = "COD_SERIE_HIST",
        val = proj$COD_SERIE_HIST)
    # temporarily get cod_sect_releve, this is obtained from desc_secteur_releve_f
    # all sets shold have the same desc_serie_hist_f, verify this
    if (length(unique(trait$desc_secteur_releve_f)) != 1){
        logger::log_error("The sets do not have the same desc_secteur_releve_f")
        stop("The sets do not have the same desc_secteur_releve_f")
    }
    # they are all the same, as it should be be, so just take the first one)
    desc_secteur_releve_f <- trait$desc_secteur_releve_f[1]
    #  the first letter, capitalized and without accents becomes secteur_releve
    secteur_releve <- toupper(substr(desc_secteur_releve_f, 1, 1))

    # we can now get rid of that column
    trait <- subset(trait, select = -c(desc_secteur_releve_f))

    cod_secteur_releve <- get_ref_key(
        table = "SECTEUR_RELEVE_MOLL",
        pkey_col = "COD_SECTEUR_RELEVE",
        col = "SECTEUR_RELEVE",
        val = secteur_releve)


    # add COD_STRAT
    trait <- format_cod_strat(trait, desc_serie_hist_f, cod_secteur_releve)

    # add COD_ZONE_GEST_MOLL
    trait <- format_zone(trait, desc_serie_hist_f)

    # it is now fine to rename (if needed) the stations
    trait <- format_no_station(trait)


    # The mission-level desc_stratification was part of the set
    # they are all the same, as it should be be, so just take the first one)
    if (length(unique(trait$desc_stratification)) != 1){
        logger::log_error("The sets do not have the same desc_secteur_releve_f")
        stop("The sets do not have the same desc_secteur_releve_f")
    }

    desc_stratification <- trait$desc_stratification[1]
    # we can now get rid of the desc_stratification column
    trait <- subset(trait, select = -c(desc_stratification))

    trait <- format_cod_typ_trait(trait, desc_stratification)
    # can get rid of operation column
    trait <- subset(trait, select = -c(operation))


    # make a copy for for the times before we format the dates
    trait$HRE_DEB_TRAIT <- trait$DATE_DEB_TRAIT
    trait$HRE_FIN_TRAIT <- trait$DATE_FIN_TRAIT

    trait <- format_date_trait(trait)

    # call format_cod_typ_heure before changing to oracle time
    trait <- format_cod_typ_heure(trait)

    # ok, now change andes time string to oracle time string
    trait <- format_date_hre_trait(trait)

    # all times are cast to America/Toronto, which is called "Québec"
    cod_fuseau_horaire <- get_ref_key(
            table="FUSEAU_HORAIRE",
            pkey_col="COD_FUSEAU_HORAIRE",
            col="DESC_FUSEAU_HORAIRE_F",
            val="Québec")

    trait <- add_hard_coded_value(trait, col_name = "COD_FUSEAU_HORAIRE", value = cod_fuseau_horaire)


    # 0 -> Inconnue
    # 1 -> Estimation
    # 2 -> Radar
    # 3 -> Decca
    # 4 -> Loran
    # 5 -> Satellite (GPS)
    # 6 -> Satellite (DGPS)
    trait <- add_hard_coded_value(trait, col_name = "COD_METHOD_POS", value = 6)

    trait <- format_coordinates(trait)
    trait <- add_hard_coded_value(trait, col_name = "LATLONG_P", value = NA)

    trait <- add_hard_coded_value(trait, col_name = "DISTANCE_POS", value = NA)
    trait <- add_hard_coded_value(trait, col_name = "DISTANCE_POS_P", value = NA)

    trait <- add_hard_coded_value(trait, col_name = "VIT_TOUAGE", value = NA)
    trait <- add_hard_coded_value(trait, col_name = "VIT_TOUAGE_P", value = NA)

    trait <- add_hard_coded_value(trait, col_name = "DUREE_TRAIT", value = NA)
    trait <- add_hard_coded_value(trait, col_name = "DUREE_TRAIT_P", value = NA)

    trait <- add_hard_coded_value(trait, col_name = "TEMP", value = NA)
    trait <- add_hard_coded_value(trait, col_name = "TEMP_P", value = NA)

    
    trait <- add_hard_coded_value(trait, col_name = "SALINITE_FOND", value = NA)
    trait <- add_hard_coded_value(trait, col_name = "SALINITE_FOND_P", value = NA)

    trait <- add_hard_coded_value(trait, col_name = "PROF_DEB_P", value = NA)
    trait <- add_hard_coded_value(trait, col_name = "PROF_FIN_P", value = NA)

    trait <- add_hard_coded_value(trait, col_name = "COD_TYP_ECH_TRAIT", value = NA)


    

    return(trait)
}

