

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
get_projet_mollusc_db<- function(andes_db_connection) {
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
get_projet_mollusc <- function(andes_db_connection) {
    proj <- get_projet_mollusc_db(andes_db_connection)

    # add COD_SOURCE_INFO from the description
    proj <- format_cod_source_info(proj)
    # DESC_SOURCE_INFO_F can be removed
    proj <- subset(proj, select = -c(DESC_SOURCE_INFO_F))

    return(proj)
}

