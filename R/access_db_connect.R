

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
        "DBQ=", file_path, ";",
        sep = "")

    # if opening the access template, set to read only.
    if (file_path == system.file("ref_data", "access_template.mdb", package = "ANDESMollusque")) {
        connection_string <- paste(
            connection_string,
            "ReadOnly=1",
            sep = "")
    }
    access_db_connection <- DBI::dbConnect(odbc::odbc(),
                    .connection_string = connection_string,
                    timeout = 10)
    return(access_db_connection)
}

#' @export
create_new_access_db <- function(fname = "new_access_db.mdb") {
    template_file_path <- system.file("ref_data",
            "access_template.mdb",
            package = "ANDESMollusque")
    res <- file.copy(template_file_path, fname, overwrite = FALSE)
    if (res) {
        return(fname)
    } else {
        logger::log_error("Failed to create a new Access database file {fname}")
        stop("Failed to copy the template file to ", fname)
        return(FALSE)
    }
}



#' gets the reference key corresponding to a value (usually from the Oracle / MSAccess reference database)
#' @param table: The name of the Oracle table, defaults to "tablename"
#' @param pkey_col: The column name that holds the key, defaults to "columnofprimarykey"
#' @param col: The column that holds the value to match, defaults to "columnname"
#' @param val: The value to match, defaults to "entryvalue"
#' @param optional_query: additional string to append to the query
#' @return: The value found in the pkey column for the entry with the value
#' @export
get_ref_key <- function(table = "tablename",
                        pkey_col = "columnofprimarykey",
                        col = "columnname",
                        val = "entryvalue",
                        optional_query = "") {
    # sanitize the strings
    if (is.character(val)) {
        # sanitize the value, double up existing quotes
        val <- gsub("'", "''", val)
        # sanitize the value, wrap in single quotes
        val <- paste("'", val, "'", sep = "")
    }
    query <- paste("SELECT ", pkey_col,
                  " FROM ", table,
                  " WHERE ", col, "=", val,
                  " ", optional_query,
                  sep = "")
    access_db_connection <- access_db_connect()
    result <- DBI::dbSendQuery(access_db_connection, query)
    ref_key <- DBI::dbFetch(result, n = Inf)[, pkey_col]
    DBI::dbClearResult(result)
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
#' @export
get_ref_choices <- function(table = "tablename",
                        col = "columnname",
                        optional_query = "") {

    query <- paste("SELECT ", col,
            " FROM ", table,
            " ", optional_query,
            sep = "")
    access_db_connection <- access_db_connect()
    result <- DBI::dbSendQuery(access_db_connection, query)
    choices <- DBI::dbFetch(result, n = Inf)[, col]
    DBI::dbClearResult(result)
    DBI::dbDisconnect(access_db_connection)
    return(choices)
}


#' this function is meant to help the validation
#' automatically getting colnames, no-null cols and datatypes.
#' but I cannot get it to work here yet... (but works in Dbeaver)
get_access_table_properties <- function(table_name = NULL) {
    if (is.null(table_name)) {
        stop("Must supply a table name to verify properties")
    }

    access_db_connection <- access_db_connect()

    query <- readr::read_file(system.file("sql_queries",
                                          "table_property.sql",
                                          package = "ANDESMollusque"))
    # manually delete the comments from the SQL query... because ACCESS...
    query <- gsub("--.*?\n", "", query)

    query <- paste(query, " WHERE TABLE_NAME='", table_name, "'", sep = "")

    result <- DBI::dbSendQuery(access_db_connection, query)
    table_props <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)
    DBI::dbDisconnect(access_db_connection)
}