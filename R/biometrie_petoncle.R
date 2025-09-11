#' Gets capture_mollusc_db (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the biometry table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_biometrie_petoncle`
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @param collection_name Optional. A string with the name of the colelction, (e.g., "Conserver pour biométrie 16E").
#' @return A dataframe containing fishing set data.
#' @seealso [get_biometrie_petoncle()] for the formatted results
#' @export
get_biometrie_petoncle_db <- function(andes_db_connection, collection_name = NULL) {
    query <- readr::read_file(system.file("sql_queries",
                                          "biometrie_petoncle.sql",
                                          package = "ANDESMollusque"))

    # add mission filter
    # use the active misison, one day you can choose a different mission,
    query <- paste(query, "WHERE shared_models_mission.is_active=1")

    # add collection_name filter
    if (!is.null(collection_name)) {
        query <- paste(
            query,
            " AND shared_models_observationgroup.nom='",
            collection_name,
            "'",
            sep = ""
            )
    }

    query <- paste(query, "GROUP BY shared_models_specimen.id, shared_models_sample.sample_number, shared_models_specimen.comment")
    # important, only select those specimens that we chose to keep!
    query <- paste(query, "HAVING collect_specimen=1")
    # order by code collection coquil
    query <- paste(query, "ORDER BY no ASC")

    result <- DBI::dbSendQuery(andes_db_connection, query)
    df <- DBI::dbFetch(result, n = Inf)
    DBI::dbClearResult(result)

    return(df)
}

#' Get a list of legal collection names as a filter for get_biometrie_petoncle()
#' 
#' These must match the shared_models_observationgroup.nom as the ANDES collection.
#' The secteur (16E, 16F, centre, ouest) are taken from the last part of the string.
#' @return A dataframe containing get_biometrie_petoncle table data.
get_legal_collection_names <- function() {
  return(c(
    "Conserver pour biométrie 16E",
    "Conserver pour biométrie 16F",
    "Conserver pour biométrie centre",
    "Conserver pour biométrie ouest"
  ))
}


#' Gets get_biometrie_petoncle (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the biometry table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' @param andes_db_connection a connection object to the ANDES database.
#
#' @return A dataframe containing get_biometrie_petoncle table data.
#' @seealso [get_biometrie_petoncle_db(), get_legal_collection_names()] for the db results
#' @export
get_biometrie_petoncle <- function(andes_db_connection, collection_name = NULL) {
    # Validate input
    if (is.null(collection_name)) {
        logger::log_error("Must provide a formatted collection_name string.")
        stop("Must provide a formatted collection_name string.")
    }
    if (! collection_name %in% get_legal_collection_names()) {
        logger::log_error(paste0("collection_name must be one of: ", paste(get_legal_collection_names(), collapse = ", ")))
        stop(paste0("collection_name must be one of: ", paste(get_legal_collection_names(), collapse = ", ")))
    }

    biometrie <- get_biometrie_petoncle_db(andes_db_connection, collection_name = collection_name)
    # TODO
    # need to delete columns :
    #  - collection
    #  - station
    #  - collect_specimen

    # need to add a secteur column. use the last poartt of the collection_name
    split_string <- strsplit(collection_name, " ")[[1]]
    secteur <- split_string[length(split_string)]
    biometrie$secteur <- secteur

    # can get rid of columns: collection, station, collect_specimen
    biometrie <- subset(biometrie, select = -c(collection, station, collect_specimen))

    return(biometrie)
}