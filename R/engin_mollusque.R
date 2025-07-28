

#' Gets engin_mollusc_db (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the ENGIN_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_engin_mollusc` 
#'
#' @param andes_db_connection a connection object to the ANDES database. Please
#' @return A dataframe containing fishing set data.
#' @seealso [get_projet_mollusque()] for the formatted results
#' @export
get_engin_mollusque_db<- function(andes_db_connection) {
    query <- readr::read_file(system.file("sql_queries",
                                          "engin_mollusque.sql",
                                          package = "ANDESMollusque"))
    result <- DBI::dbSendQuery(andes_db_connection, query)
    proj <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)
    return(proj)
}



#' Gets engin_mollusc (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the ENGIN_MOLLUSC table.
#' The current ANDES active mission will determine for which data are returned.
#'
#'
#' @param andes_db_connection a connection object to the ANDES database. Please
#' @return A dataframe containing fishing set data.
#' @seealso [get_projet_mollusque_db()] for the db results
#' @export
get_engin_mollusque<- function(andes_db_connection) {
    engin <- get_engin_mollusque_db(andes_db_connection)

    query <- readr::read_file(system.file("sql_queries",
                                          "engin_mollusque.sql",
                                          package = "ANDESMollusque"))
    result <- DBI::dbSendQuery(andes_db_connection, query)
    proj <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)
    return(proj)
}
