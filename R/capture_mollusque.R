

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

    query <- paste(query, "ORDER BY shared_models_sample.sample_number ASC, shared_models_catch.id ASC")

    result <- DBI::dbSendQuery(andes_db_connection, query)
    df <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)

    return(df)
}

#' This fetches the specimen-level coverage data and
#' coverts it to an average set-level metric in accordance to the legacy Oracle database. 
#' @export
get_epibiont <- function(andes_db_connection, code_filter) {
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

    # apply the code filter, ideally it would have been applied in the SQL, but here we are
    if (!is.null(code_filter)) {
        df <- df[df$code %in% code_filter, ]
    }

    # tag each specimen as having barnacles or not ( observation_value is anything but category 0)
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


        # self.data["COD_ESP_GEN"] = self.get_cod_esp_gen()
        # self.data["FRACTION_PECH"] = self.get_fraction_peche()
        # self.data["FRACTION_ECH"] = self.get_fraction_ech()
        # self.data["COD_DESCRIP_CAPT"] = self.get_cod_descrip_capt()
        # self.data["FRACTION_ECH_P"] = self.get_fraction_ech_p()
        # self.data["COD_TYP_MESURE"] = self.get_cod_type_mesure()
        # self.data["NBR_CAPT"] = self.get_nbr_capt()
        # self.data["FRACTION_PECH_P"] = self.get_fraction_peche_p()
        # self.data["NBR_ECH"] = self.get_nbr_ech()
        # self.data["PDS_CAPT"] = self.get_pds_capt()
        # self.data["PDS_CAPT_P"] = self.get_pds_capt_p()
        # self.data["PDS_ECH"] = self.get_pds_ech()
        # self.data["PDS_ECH_P"] = self.get_pds_ech()

    epibiont_data <- get_epibiont(andes_db_connection, code_filter)
    epibiont_data <- format_epibiont(epibiont_data)

    # capt <- add_hard_coded_value(capt, col_name = "REM_ENGIN_MOLL", value = NA)
    capt <- left_join(capt, epibiont_data, by = c("IDENT_NO_TRAIT", "strap_code"))

    # can get rid of temporary columns
    capt <- subset(capt, select = -c(ave_with_barnacles))
    capt <- subset(capt, select = -c(ave_coverage))
    capt <- subset(capt, select = -c(description_fra))

    return(capt)
}

#' Perform validation checks on the dataframe before writing to a database table
#' @export
validate_capture_mollusque <- function(df) {
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
write_capture_mollusque <- function(df, access_db_write_connection = NULL) {
#    # write the dataframe to the database
#     if (is.null(access_db_write_connection)) {
#         logger::log_error("Failed to provide a new MS Access connection.")
#         stop("Failed to provide a new MS Access connection")
#     }

#     # insert make one row at a time
#     for (i in seq_len(nrow(df))) {
#         statement <- generate_sql_insert_statement(df[i, ], "CAPTURE_MOLLUSQUE")
#         logger::log_debug("Writing the following statement to the database: {statement}")
#         result <- DBI::dbExecute(access_db_write_connection, statement)
#         if (result != 1) {
#             logger::log_error("Failed to write a row to the CAPTURE_MOLLUSQUE Table, row: {i}")
#             stop("Failed to write a row to the CAPTURE_MOLLUSQUE Table")
#         } else {
#             logger::log_debug("Successfully added a row to the CAPTURE_MOLLUSQUE Table")
#         }
#     }
#     logger::log_info("Successfully wrote the capture_mollusque to the database.")
}