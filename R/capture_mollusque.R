

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
get_capture_mollusque_db <- function(andes_db_connection) {
    query <- readr::read_file(system.file("sql_queries",
                                          "epibiont_cte.sql",
                                          package = "ANDESMollusque"))

    result <- DBI::dbSendQuery(andes_db_connection, query)
    df <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)

    return(df)
}

#' This fetches the specimen-level coverage data and
#' coverts it to an average set-level metric in accordance to the legacy Oracle database. 
#' @export
get_epibiont <- function(andes_db_connection) {
    query <- readr::read_file(system.file("sql_queries",
                                          "epibiont_cte.sql",
                                          package = "ANDESMollusque"))
    # finish the query from the primed CTE statement
    # the easy way is to grab the final epibiont_cte table
    # query <- paste(query,"SELECT * FROM epibiont_cte")

    # the hard way is to take the balane_cte table and re-compute the post-porcessing.
    # let's to the hard-way, this effectively move SQL post-precessing into R post-processing
    # Moving the post-processing out of SQL and into R makes it easier to maintain / debug


    query <- paste(query, "SELECT * FROM balane_cte")
    result <- DBI::dbSendQuery(andes_db_connection, query)
    df <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)

    # tag each specimen has having barnacles or not ( observation_value is anything but category 0)
    has_barnacles <- as.numeric(df$observation_value > 0)
    abondance_epibiont <- aggregate(
        x = list(ave_with_barnacles = has_barnacles),
        by = list(code = df$code, sample_number = df$sample_number),
        FUN = mean
    )
    # View(abondance_epibiont)
    # done with abondance_epibiont

    # assign each coverage category with a numerical value (based o the midpoint)
    code_map <- data.frame(observation_value = c("1", "2", "3"), coverage = c((0+1)/6., (1+2)/6., (2+3)/6.))
    # res <- merge(desc_typ_trait, code_map, by = "desc", all.x = TRUE, sort = FALSE)
    coverage <- left_join(df, code_map, by = "observation_value")$coverage

    # category <- c ("1", "2", "3")
    # value <- c ((0+1)/6. ,(1+2)/6. ,(2+3)/6. )

    couverture_epibiont <- aggregate(
        x = list(ave_coverage = coverage),
        by = list(code = df$code, sample_number = df$sample_number),
        FUN = mean, na.rm = TRUE
    )
    # View(couverture_epibiont)
    # done with couverture_epibiont

    # now we combine them
    joined <- merge(x = abondance_epibiont, y = couverture_epibiont, all = TRUE, sort = FALSE)
    return(joined)
}


#' Gets capture_mollusque (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the CAPTURE_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' Structurally, it would make sense to send a Trait instance to for every engin
#' but here we can get away with proj (due to how ANDES is structured)
#' @param andes_db_connection a connection object to the ANDES database.
#
#' @return A dataframe containing engin table data.
#' @seealso [get_capture_mollusque_db()] for the db results
#' @export
get_caputre_mollusque <- function(andes_db_connection, proj = NULL) {
    # Validate input
    if (is.null(proj)) {
        logger::log_error("Must provide a formatted proj_mollusque dataframe.")
        stop("Must provide a formatted proj_mollusque dataframe.")
    }

    capt <- get_capture_mollusque_db(andes_db_connection)

    # Take data from trait_mollusque
    capt$COD_SOURCE_INFO <- proj$COD_SOURCE_INFO
    capt$NO_RELEVE <- proj$NO_RELEVE
    capt$COD_NBPC <- proj$COD_NBPC
    capt$NO_CHARGEMENT <- proj$NO_CHARGEMENT

    # capt <- add_hard_coded_value(capt, col_name = "REM_ENGIN_MOLL", value = NA)

    return(capt)
}

#' Perform validation checks on the dataframe before writing to a database table
#' @export
validate_engin_mollusque <- function(df) {
    # not_null_columns <- c(
    #     "COD_SOURCE_INFO",
    #     "NO_RELEVE",
    #     "COD_NBPC",
    #     "IDENT_NO_TRAIT",
    #     "COD_ENG_GEN",
    #     "COD_TYP_PANIER",
    #     "NO_ENGIN"
    # )
    # if (cols_contains_na(df, col_names = not_null_columns)) {
    #     logger::log_error("dataframe cannot be written as DB table")
    #     return(FALSE)
    # }

    # return(TRUE)
}

#' @export
write_engin_mollusque <- function(engin, access_db_write_connection = NULL) {
#    # write the dataframe to the database
#     if (is.null(access_db_write_connection)) {
#         logger::log_error("Failed to provide a new MS Access connection.")
#         stop("Failed to provide a new MS Access connection")
#     }

#     # insert make one row at a time
#     for (i in seq_len(nrow(engin))) {
#         statement <- generate_sql_insert_statement(engin[i, ], "ENGIN_MOLLUSQUE")
#         logger::log_debug("Writing the following statement to the database: {statement}")
#         result <- DBI::dbExecute(access_db_write_connection, statement)
#         if (result != 1) {
#             logger::log_error("Failed to write a row to the ENGIN_MOLLUSQUE Table, row: {i}")
#             stop("Failed to write a row to the ENGIN_MOLLUSQUE Table")
#         } else {
#             logger::log_debug("Successfully added a row to the ENGIN_MOLLUSQUE Table")
#         }
#     }
#     logger::log_info("Successfully wrote the engin_mollusque to the database.")
}