

#' Gets fishing projet_mollusque (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the PROJET_MOLLUSQUE table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_projet_mollusque` 
#'
#' @param andes_db_connection a connection object to the ANDES database. 
#' @return A dataframe containing fishing set data.
#' @seealso [get_projet_mollusque()] for the formatted results
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


#' Gets fishing projet_mollusque (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the PROJET_MOLLUSQUE table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @return A dataframe containing fishing set data.
#' @seealso [get_projet_mollusque_db()] for the raw database results
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
    proj <- add_hard_coded_value(proj, col_name = "DIST_CHALUTE_VISEE_P", value = 1.0)

    # # TODO, this value could be obtained as a set observation type ?
    # # Oracle is structured as a mission variable, not set (which does not reflect reality)
    proj <- add_hard_coded_value(proj, col_name = "RAPPORT_FUNE_VISEE", value = 1.0)

    proj <- add_hard_coded_value(proj, col_name = "NOM_EQUIPE_NAVIRE", value = "")

    proj <- add_hard_coded_value(proj, col_name = "NO_CHARGEMENT", value = NA)

    proj <- cols_to_numeric(proj, col_names = c("NO_CHARGEMENT"))

    # drop id
    proj <- subset(proj, select = -c(id))

    return(proj)

}

#' Perform validation checks on the dataframe before writing to a database table
#' @export
validate_projet_mollusque <- function(df) {
    is_valid <- TRUE
    # check all required cols are present
    result <- check_columns_present(df, col_names = c(
        "COD_SOURCE_INFO",
        "NO_RELEVE",
        "COD_NBPC",
        "ANNEE",
        "COD_SERIE_HIST",
        "COD_TYP_STRATIF",
        "DATE_DEB_PROJET",
        "DATE_FIN_PROJET",
        "NO_NOTIF_IML",
        "CHEF_MISSION",
        "SEQ_PECHEUR",
        "DUREE_TRAIT_VISEE",
        "DUREE_TRAIT_VISEE_P",
        "VIT_TOUAGE_VISEE",
        "VIT_TOUAGE_VISEE_P",
        "DIST_CHALUTE_VISEE",
        "DIST_CHALUTE_VISEE_P",
        "RAPPORT_FUNE_VISEE",
        "RAPPORT_FUNE_VISEE_P",
        "NOM_EQUIPE_NAVIRE",
        "NOM_SCIENCE_NAVIRE",
        "REM_PROJET_MOLL",
        "NO_CHARGEMENT"
    ))
    is_valid <- is_valid & result

    # check all not-null columns do not have nulls
    result <- check_cols_contains_na(df, col_names = c(
        "COD_SOURCE_INFO",
        "NO_RELEVE",
        "COD_NBPC",
        "COD_SERIE_HIST",
        "COD_TYP_STRATIF"
    ))
    is_valid <- is_valid & result

    result <- check_other_columns(df, col_names = c(
        "COD_SOURCE_INFO",
        "NO_RELEVE",
        "COD_NBPC",
        "ANNEE",
        "COD_SERIE_HIST",
        "COD_TYP_STRATIF",
        "DATE_DEB_PROJET",
        "DATE_FIN_PROJET",
        "NO_NOTIF_IML",
        "CHEF_MISSION",
        "SEQ_PECHEUR",
        "DUREE_TRAIT_VISEE",
        "DUREE_TRAIT_VISEE_P",
        "VIT_TOUAGE_VISEE",
        "VIT_TOUAGE_VISEE_P",
        "DIST_CHALUTE_VISEE",
        "DIST_CHALUTE_VISEE_P",
        "RAPPORT_FUNE_VISEE",
        "RAPPORT_FUNE_VISEE_P",
        "NOM_EQUIPE_NAVIRE",
        "NOM_SCIENCE_NAVIRE",
        "REM_PROJET_MOLL",
        "NO_CHARGEMENT"
    ))
    is_valid <- is_valid & result

    result <- check_numeric_columns(df, col_names = c(
        "COD_SOURCE_INFO",
        "NO_RELEVE",
        "ANNEE",
        "COD_SERIE_HIST",
        "COD_TYP_STRATIF",
        "SEQ_PECHEUR",
        "DUREE_TRAIT_VISEE",
        "DUREE_TRAIT_VISEE_P",
        "VIT_TOUAGE_VISEE",
        "VIT_TOUAGE_VISEE_P",
        "DIST_CHALUTE_VISEE",
        "DIST_CHALUTE_VISEE_P",
        "RAPPORT_FUNE_VISEE",
        "RAPPORT_FUNE_VISEE_P",
        "NO_CHARGEMENT"
    ))
    is_valid <- is_valid & result

    return(is_valid)
}

#' @export
write_projet_mollusque <- function(proj, access_db_write_connection=NULL) {
    # write the dataframe to the database
    if (is.null(access_db_write_connection)) {
        logger::log_error("Failed to provide a new MS Acces connection.")
        stop("Failed to provide a new MS Acces connection")
    }
    
    statement <- generate_sql_insert_statement(proj[1, ], "PROJET_MOLLUSQUE")
    result <- DBI::dbExecute(access_db_write_connection, statement)

    if (result!=1) {
        logger::log_error("Failed to write the projet_mollusque to the database.")
        stop("Failed to write the projet_mollusque to the database.")
    } else {
        logger::log_info("Successfully wrote the projet_mollusque to the database.")
    }
    # DBI::dbClearResult(result)
}