
format_cod_source_info <- function(df) {
    desc_source_info_f <-  df[, which(names(df) == "DESC_SOURCE_INFO_F")]
    df["CODE_SOURCE_INFO"] <- unlist(lapply(desc_source_info_f, lookup_cod_source_info))
    return(df)
}

lookup_cod_source_info <- function(desc_source_info_f) {

    # HACK, hard-code the lookup here
    cod_source_info_map <- c(
        "Évaluation de stocks IML - Pétoncle Minganie" = "18",
        "Évaluation de stocks IML - Pétoncle I de M" = "19", # MSACCESS
        "Évaluation de stocks IML - Pétoncle Îles-de-la-Madeleine" = "19", # ORACLE
        "Relevé buccin Haute Côte-Nord" = "22"
    )
    return(cod_source_info_map[desc_source_info_f])

    # TODO: better to lookup the values from the mdb file...

    # access_db <- readr::read_file(system.file("ref_data",
    #                                       "access_template.mdb",
    #                                       package = "ANDESMollusque"))
    # channel <- RODBC::odbcConnectAccess2007(access_db)

    # query <- sprintf("
    # SELECT COD_SOURCE_INFO
    # FROM SOURCE_INFO
    # WHERE DESC_SOURCE_INFO_F=%s
    # ;
    # ", desc_source_info_f)

    # res <- RODBC::sqlQuery(channel,
    #                     query,
    #                     errors = FALSE)
    # RODBC::odbcClose(channel)
    # return(res)
}

lookup_cod_serie_hist <- function(desc_serie_hist_f) {
    # HACK hard lookuip here
    cod_serie_hist_map <- c(
        "Indice d'abondance zone 16E - pétoncle" <- 15,
        "Indice d'abondance zone 16F - pétoncle" <- 16,
        "Indice d'abondance zone 20 - pétoncle" <- 18,
        "Indice d'abondance buccin" <- 20
    )
    return(cod_serie_hist_map[desc_serie_hist_f])
    # query <- sprintf("
    # SELECT COD_SERIE_HIST
    # FROM Indice_Suivi_Etat_Stock
    # WHERE DESC_SERIE_HIST_F=%S,
    # ;", desc_serie_hist_f)
}