

#' Gets engin_mollusc_db (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the ENGIN_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_engin_mollusc`
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @return A dataframe containing fishing set data.
#' @seealso [get_engin_mollusque()] for the formatted results
#' @export
get_engin_mollusque_db <- function(andes_db_connection) {
    query <- readr::read_file(system.file("sql_queries",
                                          "engin_mollusque.sql",
                                          package = "ANDESMollusque"))
    result <- DBI::dbSendQuery(andes_db_connection, query)
    df <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)
    return(df)
}



#' Gets engin_mollusc (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the ENGIN_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' Structurally, it would make sense to send a Trait instance to for every engin
#' but here we can get away with proj (due to how ANDES is structured)
#' @param andes_db_connection a connection object to the ANDES database.
#
#' @return A dataframe containing engin table data.
#' @seealso [get_engin_mollusque_db()] for the db results
#' @export
get_engin_mollusque <- function(andes_db_connection, proj = NULL) {
    # Validate input
    if (is.null(proj)) {
        logger::log_error("Must provide a formatted proj_mollusque dataframe.")
        stop("Must provide a formatted proj_mollusque dataframe.")
    }

    engin <- get_engin_mollusque_db(andes_db_connection)

    # Take data from trait_mollusque
    engin$COD_SOURCE_INFO <- proj$COD_SOURCE_INFO
    engin$NO_RELEVE <- proj$NO_RELEVE
    engin$COD_NBPC <- proj$COD_NBPC
    engin$NO_CHARGEMENT <- proj$NO_CHARGEMENT

    engin <- format_cod_typ_panier(engin)

    engin <- cols_to_numeric(engin, col_names = c("COD_ENG_GEN", "NO_ENGIN", "REMPLISSAGE"))

    engin <- add_hard_coded_value(engin, col_name = "REMPLISSAGE_P", value = NA)

    engin <- add_hard_coded_value(engin, col_name = "LONG_FUNE", value = NA)
    engin <- add_hard_coded_value(engin, col_name = "LONG_FUNE_P", value = NA)

    engin <- add_hard_coded_value(engin, col_name = "NB_PANIER", value = 4)

    # engin <- add_hard_coded_value(engin, col_name = "REM_ENGIN_MOLL", value = NA)

    return(engin)
}

#' Perform validation checks on the dataframe before writing to a database table
#' @export
validate_engin_mollusque <- function(df) {
    not_null_columns <- c(
        "COD_SOURCE_INFO",
        "NO_RELEVE",
        "COD_NBPC",
        "IDENT_NO_TRAIT",
        "COD_ENG_GEN",
        "COD_TYP_PANIER",
        "NO_ENGIN"
    )
    if (cols_contains_na(df, col_names = not_null_columns)) {
        logger::log_error("dataframe cannot be written as DB table")
        return(FALSE)
    }

    return(TRUE)
}

#' @export
write_engin_mollusque <- function(engin, access_db_write_connection = NULL) {
   # write the dataframe to the database
    if (is.null(access_db_write_connection)) {
        logger::log_error("Failed to provide a new MS Access connection.")
        stop("Failed to provide a new MS Access connection")
    }

    # insert make one row at a time
    for (i in seq_len(nrow(engin))) {
        statement <- generate_sql_insert_statement(engin[i, ], "ENGIN_MOLLUSQUE")
        logger::log_debug("Writing the following statement to the database: {statement}")
        result <- DBI::dbExecute(access_db_write_connection, statement)
        if (result != 1) {
            logger::log_error("Failed to write a row to the ENGIN_MOLLUSQUE Table, row: {i}")
            stop("Failed to write a row to the ENGIN_MOLLUSQUE Table")
        } else {
            logger::log_debug("Successfully added a row to the ENGIN_MOLLUSQUE Table")
        }
    }
    logger::log_info("Successfully wrote the engin_mollusque to the database.")
}