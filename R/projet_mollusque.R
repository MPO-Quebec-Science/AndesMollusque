

#' Gets fishing projet_mollusc (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the PROJET_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_projet_mollusc` 
#'
#' @param andes_db_connection a connection object to the ANDES database. Please
#' @return A dataframe containing fishing set data.
#' @seealso [get_projet_mollusc()] for the formatted results
#' @export
get_projet_mollusque_db<- function(andes_db_connection) {
    query <- readr::read_file(system.file("sql_queries",
                                          "projet_mollusque.sql",
                                          package = "ANDESMollusque"))
    result <- DBI::dbSendQuery(andes_db_connection, query)
    proj <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)
    return(proj)
}


#' Gets fishing projet_mollusc (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the PROJET_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' @param andes_db_connection a connection object to the ANDES database. Please
#' @return A dataframe containing fishing set data.
#' @seealso [get_projet_mollusc_db()] for the raw database results
#' @export
get_projet_mollusque <- function(andes_db_connection) {
    proj <- get_projet_mollusque_db(andes_db_connection)

    # add COD_SOURCE_INFO from the description
    proj <- format_cod_source_info(proj)
    # DESC_SOURCE_INFO_F can be removed
    proj <- subset(proj, select = -c(DESC_SOURCE_INFO_F))

    # format the start and end dates
    proj <- format_date_deb_projet(proj)
    proj <- format_date_fin_projet(proj)

    # use vessel_name to obtain SEQ_PECHEUR (i.e., Captaine Leim)
    proj <- format_seq_pecheur(proj)
    # drop vessel_name
    proj <- subset(proj, select = -c(vessel_name))

    # cleanup the text block (remove new lines)
    proj <- cleanup_text(proj, col_name = "NOM_SCIENCE_NAVIRE", max_chars = 250)

    proj <- cleanup_text(proj, col_name = "REM_PROJET_MOLL", max_chars = 255)

    proj <- add_hard_coded_value(proj, col_name = "DUREE_TRAIT_VISEE_P", value = 0.1)
    proj <- add_hard_coded_value(proj, col_name = "VIT_TOUAGE_VISEE_P", value = 0.1)
    proj <- add_hard_coded_value(proj, col_name = "RAPPORT_FUNE_VISEE_P", value = 1.0)

    # # TODO, this value could be obtained as a set observation type ?
    # # Oracle is structured as a mission variable, not set (which does not reflect reality)
    proj <- add_hard_coded_value(proj, col_name = "RAPPORT_FUNE_VISEE", value = 1.0)

    proj <- add_hard_coded_value(proj, col_name = "NOM_EQUIPE_NAVIRE", value = "")

    proj <- add_hard_coded_value(proj, col_name = "NO_CHARGEMENT", value = NULL)

    return(proj)
}

