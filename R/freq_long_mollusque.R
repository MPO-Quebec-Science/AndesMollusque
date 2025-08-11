
#' Gets freq_long_mollusque_db (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the FREQ_LONG_MOLLUSQUE table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_freq_long_mollusque`
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @return A dataframe containing fishing set data.
#' @seealso [get_freq_long_mollusque()] for the formatted results
#' @export
get_freq_long_mollusque_db<- function(andes_db_connection) {

    # mnake a small pre-query to get the observation type for length.
    # this is purposefully not inside the main query in case we want to change it later ...

    pre_query <- "SELECT shared_models_observationtype.id FROM shared_models_observationtype WHERE shared_models_observationtype.special_type=1"

    result <- DBI::dbSendQuery(andes_db_connection, pre_query)
    length_type <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)



    query <- readr::read_file(system.file("sql_queries",
                                          "freq_long_mollusque.sql",
                                          package = "ANDESMollusque"))

    # Select active mission
    query <- paste(query, "WHERE shared_models_mission.is_active=1 ")

    # just select the official length (obtained with pre-query above)
    query <- paste(query, "AND shared_models_observationtype.id=", length_type, sep = "")
    query <- paste(query, "ORDER BY shared_models_specimen.id ASC")


    result <- DBI::dbSendQuery(andes_db_connection, query)
    freq <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)
    return(freq)
}

#' Gets freq_long_mollusque (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data
#' to construct the TRAIT_MOLLUSQUE table and formats the results.
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @param capt A formatted capture_mollusque dataframe
#' @return A dataframe containing formatted fishing set data.
#' @export
get_freq_long_mollusque <- function(andes_db_connection, capt = NULL) {
    # Validate input
    if (is.null(capt)) {
        logger::log_error("Must provide a formatted projet_mollusque dataframe.")
        stop("Must provide a formatted projet_mollusque dataframe.")
    }
    
    # Get raw data
    freq <- get_freq_long_mollusque_db(andes_db_connection)

    cols_from_capt <- c(
        "IDENT_NO_TRAIT",
        "COD_SOURCE_INFO",
        "NO_RELEVE",
        "COD_NBPC",
        "NO_CHARGEMENT",
        "COD_TYP_PANIER",
        "COD_ENG_GEN",
        "NO_ENGIN",
        "COD_ESP_GEN"
    )

    data_from_capt <- capt[, names(capt) %in% cols_from_capt]

    freq <- left_join(freq, data_from_capt, on = "IDENT_NO_TRAIT")

    freq <- add_hard_coded_value(freq, col_name = "VALEUR_LONG_MOLL_P", value = NA)

    freq <- format_cod_typ_long(freq)

    freq <- format_cod_tech_mesure_long(freq)

    freq <- format_cod_typ_etat(freq)

    freq <- format_no_mollusque(freq)

    # convert these strings to numeric
    freq <- cols_to_numeric(freq, col_names = c("VALEUR_LONG_MOLL"))


}

#' Perform validation checks on the dataframe before writing to a database table
#' @param delete_extra If True, it delete extra columns
#' @export
validate_freq_long_mollusque <- function(df) {
    # check all required cols are present
    check_columns_present(df, col_names = c(
        "COD_ESP_GEN",
        "COD_ENG_GEN",
        "COD_SOURCE_INFO",
        "NO_RELEVE",
        "IDENT_NO_TRAIT",
        "COD_TYP_PANIER",
        "COD_NBPC",
        "NO_ENGIN",
        "VALEUR_LONG_MOLL",
        "NO_MOLLUSQUE",
        "COD_TYP_LONG",
        "VALEUR_LONG_MOLL_P",
        "COD_TYP_ETAT",
        "NO_CHARGEMENT",
        "COD_TECH_MESURE_LONG"
    ))

    # check all not-null columns do not have nulls
    check_cols_contains_na(df, col_names = c(
        "COD_ESP_GEN",
        "IDENT_NO_TRAIT",
        "NO_RELEVE",
        "COD_SOURCE_INFO",
        "COD_NBPC",
        "COD_ENG_GEN",
        "COD_TYP_PANIER",
        "NO_ENGIN",
        "COD_TYP_LONG",
        "COD_TYP_ETAT",
        "COD_TECH_MESURE_LONG"
    ))

    # check if extra columns are in the dataframe
    # SELECT COLUMN_NAME
    # FROM INFORMATION_SCHEMA.COLUMNS
    # WHERE TABLE_NAME = 'FREQ_LONG_MOLLUSQUE';

    check_other_columns(df, col_names = c(
        "COD_ESP_GEN",
        "IDENT_NO_TRAIT",
        "NO_RELEVE",
        "COD_SOURCE_INFO",
        "COD_NBPC",
        "COD_ENG_GEN",
        "COD_TYP_PANIER",
        "NO_ENGIN",
        "NO_MOLLUSQUE",
        "COD_TYP_LONG",
        "VALEUR_LONG_MOLL",
        "VALEUR_LONG_MOLL_P",
        "COD_TYP_ETAT",
        "COD_TECH_MESURE_LONG",
        "NO_CHARGEMENT"
    ))

    return(TRUE)
}
