

#' Gets capture_mollusc_db (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the CAPTURE_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_capture_mollusc`
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @return A dataframe containing fishing set data.
#' @seealso [get_capture_mollusque()] for the formatted results
#' @export
get_capture_mollusque_db <- function(andes_db_connection, code_filter = NULL, basket_class_filter = NULL) {
    query <- readr::read_file(system.file("sql_queries",
                                          "capture_mollusque.sql",
                                          package = "ANDESMollusque"))

    # add mission filter
    # use the active misison, one day you can choose a different mission,
    query <- paste(query, "WHERE shared_models_mission.is_active=1")
    # add code filter
    if (!is.null(code_filter)) {
        code_filter_clause <- NULL
        if (length(code_filter) == 1 ) {
            code_filter_clause <- paste(" AND shared_models_referencecatch.code=", code_filter[1], sep="")
        } else {
            code_filter_clause <- " AND ( FALSE"
            for (code in code_filter) {
                code_filter_clause <- paste(code_filter_clause, " OR shared_models_referencecatch.code=", code, sep = "")
            }
            code_filter_clause <- paste(code_filter_clause, ")")
        }
        # add the clause to the query
        query <- paste(query, code_filter_clause)
    }
    # basket_class_filter <- c(22, 33)
    # add basket size class filter
    if (!is.null(basket_class_filter)) {
        basket_class_filter_clause <- NULL
        if (length(basket_class_filter) == 1 ) {
            basket_class_filter_clause <- paste(" AND shared_models_sizeclass.code=", basket_class_filter[1], sep="") 
        } else {
            basket_class_filter_clause <- " AND ( FALSE"
            for (basket_class in basket_class_filter) {
                basket_class_filter_clause <- paste(basket_class_filter_clause, " OR shared_models_sizeclass.code=", basket_class, sep = "")
            }
            basket_class_filter_clause <- paste(basket_class_filter_clause, ")")
        }
        # add the clause to the query
        query <- paste(query, basket_class_filter_clause)
    }

    query <- paste(query, "GROUP BY IDENT_NO_TRAIT, strap_code, shared_models_sizeclass.description_fra ")

    query <- paste(query, "ORDER BY IDENT_NO_TRAIT ASC")

    # shared_models_sample.sample_number AS IDENT_NO_TRAIT,
    # shared_models_referencecatch.code AS strap_code, -- will need to be converted to cod_esp_eng
    # shared_models_sizeclass.description_fra,

    result <- DBI::dbSendQuery(andes_db_connection, query)
    df <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)

    return(df)
}



#' Gets capture_mollusque (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the CAPTURE_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' Structurally, it would make sense to send a Trait instance to for every capture_mollusque
#' but here we can get away with proj (due to how ANDES is structured)
#' @param andes_db_connection a connection object to the ANDES database.
#
#' @return A dataframe containing capture_mollusque table data.
#' @seealso [get_capture_mollusque_db()] for the db results
#' @export
get_capture_mollusque <- function(andes_db_connection, engin = NULL, code_filter = NULL, basket_class_filter = NULL) {
    # Validate input
    if (is.null(engin)) {
        logger::log_error("Must provide a formatted engin_mollusque dataframe.")
        stop("Must provide a formatted engin_mollusque dataframe.")
    }

    capt <- get_capture_mollusque_db(andes_db_connection, code_filter = code_filter, basket_class_filter = basket_class_filter)

    # grab data from parent engin
    cols_from_engin <- c(
        "IDENT_NO_TRAIT",
        "COD_SOURCE_INFO",
        "NO_RELEVE",
        "COD_NBPC",
        "NO_CHARGEMENT",
        "COD_TYP_PANIER",
        "COD_ENG_GEN",
        "NO_ENGIN"
    )
    data_from_engin <- engin[, names(engin) %in% cols_from_engin]
    capt <- left_join(capt, data_from_engin, on = "IDENT_NO_TRAIT")

    # FRACTION_ECH : nombre de paniers de drague ayant bien peché: ex 4 ou 3 (de ANDES)
    # FRACTION_PECH : nombre de paniers de drague ayant été echantionné: ex 4 ou 3 (d'habitude meme que NBR_CAPT)
    capt$FRACTION_ECH <- capt$FRACTION_PECH

    capt <- add_hard_coded_value(capt, col_name = "FRACTION_PECH_P", value = NA)
    capt <- add_hard_coded_value(capt, col_name = "FRACTION_ECH_P", value = NA)
    capt <- add_hard_coded_value(capt, col_name = "NBR_CAPT", value = NA)
    capt <- add_hard_coded_value(capt, col_name = "NBR_ECH", value = NA)

    capt <- add_hard_coded_value(capt, col_name = "PDS_CAPT", value = NA)
    capt <- add_hard_coded_value(capt, col_name = "PDS_CAPT_P", value = NA)

    capt <- add_hard_coded_value(capt, col_name = "PDS_ECH", value = NA)
    capt <- add_hard_coded_value(capt, col_name = "PDS_ECH_P", value = NA)

    
    capt <- format_cod_descrip_capt(capt)

    capt <- format_cod_typ_mesure(capt)

    capt <- format_epibiont(capt, andes_db_connection, code_filter)

    capt <- format_cod_esp_gen(capt)

    # can get rid of strap_code column
    capt <- subset(capt, select = -c(strap_code))
    

    # convert datatypes
    capt <- cols_to_numeric(capt, col_names = c("FRACTION_ECH", "FRACTION_PECH"))

    return(capt)
}

#' Perform validation checks on the dataframe before writing to a database table
#' @export
validate_capture_mollusque <- function(df) {
    not_null_columns <- c(
        "COD_SOURCE_INFO",
        "COD_ESP_GEN",
        "NO_RELEVE",
        "COD_ENG_GEN",
        "IDENT_NO_TRAIT",
        "COD_TYP_PANIER",
        "COD_NBPC",
        "FRACTION_PECH",
        "NO_ENGIN",
        "FRACTION_ECH",
        "COD_TYP_MESURE"
    )
    if (cols_contains_na(df, col_names = not_null_columns)) {
        logger::log_error("dataframe cannot be written as DB table")
        return(FALSE)
    }
    return(TRUE)
}

#' @export
write_capture_mollusque <- function(df, access_db_write_connection = NULL) {
   # write the dataframe to the database
    if (is.null(access_db_write_connection)) {
        logger::log_error("Failed to provide a new MS Access connection.")
        stop("Failed to provide a new MS Access connection")
    }

    # insert make one row at a time
    for (i in seq_len(nrow(df))) {
        statement <- generate_sql_insert_statement(df[i, ], "CAPTURE_MOLLUSQUE")
        logger::log_debug("Writing the following statement to the database: {statement}")
        result <- DBI::dbExecute(access_db_write_connection, statement)
        if (result != 1) {
            logger::log_error("Failed to write a row to the CAPTURE_MOLLUSQUE Table, row: {i}")
            stop("Failed to write a row to the CAPTURE_MOLLUSQUE Table")
        } else {
            logger::log_debug("Successfully added a row to the CAPTURE_MOLLUSQUE Table")
        }
    }
    logger::log_info("Successfully wrote the capture_mollusque to the database.")
}