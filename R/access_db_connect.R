

#' Establish a connection to an MS Access database
#'
#' This is a wrapper for the `DBI::dbConnect`, see it's documentation for more details.
#' @param file_path full path to the MS Access file 
#' The default will be the Access template database
#' @return A connection object to the MS ACCESS database.
#' @export
access_db_connect <- function(file_path = NULL) {

    if (is.null(file_path)) {
        file_path <- system.file("ref_data",
                    "access_template.mdb",
                    package = "ANDESMollusque")
    }
    connection_string <- paste(
        "Driver={Microsoft Access Driver (*.mdb, *.accdb)};",
        "DBQ=", file_path,
        sep = "")
    access_db_connection <- DBI::dbConnect(odbc::odbc(),
                    .connection_string = connection_string,
                    timeout = 10)
    # TODO
    # DBI::dbDisconnect(access_db_connection)

    return(access_db_connection)
}


#' gets the reference key corresponding to a value (usually from the Oracle / MSAccess reference database)
#' @param table: The name of the Oracle table, defaults to "tablename"
#' @param pkey_col: The column name that holds the key, defaults to "columnofprimarykey"
#' @param col: The column that holds the value to match, defaults to "columnname"
#' @param val: The value to match, defaults to "entryvalue"
#' @param optional_query: additional string to append to the query
#' @return: The value found in the pkey column for the entry with the value
#' @export
get_ref_key <- function(table="tablename",
                        pkey_col="columnofprimarykey",
                        col="columnname",
                        val="entryvalue",
                        optional_query="") {

    sanitized_val <- gsub("'", "''", val)
    query <- paste("SELECT ", pkey_col,
                  " FROM ", table,
                  " WHERE ", col, "='", sanitized_val, "'",
                  " ", optional_query,
                  sep = "")
    access_db_connection <- access_db_connect()
    result <- DBI::dbSendQuery(access_db_connection, query)
    ref_key <- DBI::dbFetch(result, n = Inf)[, pkey_col]
    DBI::dbDisconnect(access_db_connection)

    if (length(ref_key) != 1) {
        stop("The reference query: ", query, " returned ", length(ref_key), " results. Expected 1 result.")
    }
    # TODO: add sanity checks, 
        #     if len(res) == 1:
        #     return res[0][0]
        # elif len(res) == 0:
        #     self.logger.error("No match found for query: %s", query)
        #     raise ValueError
        # else:
        #     self.logger.error("More than one match found for query: %s", query)
        #     raise ValueError

    return(ref_key)
}

#' Builds a list of legal choices (descriptions) for get ref key
get_ref_choices <- function(table="tablename",
                        col="columnname",
                        optional_query="") {
    
    query <- paste("SELECT ", col,
            " FROM ", table,
            " ", optional_query,
            sep = "")
    # print(query)
    access_db_connection <- access_db_connect()
    result <- DBI::dbSendQuery(access_db_connection, query)
    choices <- DBI::dbFetch(result, n = Inf)[, col]
    DBI::dbDisconnect(access_db_connection)
    return(choices)
}