
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

    cod_secteur_releve <- get_ref_key(
        table = "SECTEUR_RELEVE_MOLL",
        pkey_col = "COD_SECTEUR_RELEVE",
        col = "SECTEUR_RELEVE",
        val = secteur_releve)


    # add COD_STRAT
    trait <- format_cod_strat(trait, desc_serie_hist_f, cod_sect_releve)

    # add COD_ZONE_GEST_MOLL
    trait <- format_zone(trait, desc_serie_hist_f)


    # it is now fine to rename( if needed) the stations
    trait <- format_no_station(trait)


    # Format dates
    # trait <- format_date_trait(trait)
    
    # Format coordinates
    # trait <- format_coordinates(trait)
    
    # Format zone and sector
    # trait <- format_zone_secteur(trait)
    
    # Add hard-coded or computed values
    # trait <- add_hard_coded_values(trait)
    
    return(trait)
}

