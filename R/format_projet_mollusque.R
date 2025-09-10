
#' @export
format_cod_source_info <- function(df) {
    desc_source_info_f <-  df[, which(names(df) == "DESC_SOURCE_INFO_F")]
    df["COD_SOURCE_INFO"] <- unlist(lapply(desc_source_info_f, lookup_cod_source_info))
    return(df)
}

#' @export
lookup_cod_source_info <- function(desc_source_info_f) {
    # HACK, hard-code the lookup here
    # cod_source_info_map <- c(
    #     "Évaluation de stocks IML - Pétoncle Minganie" = "18",
    #     "Évaluation de stocks IML - Pétoncle I de M" = "19", # MSACCESS
    #     "Évaluation de stocks IML - Pétoncle Îles-de-la-Madeleine" = "19", # ORACLE
    #     "Relevé buccin Haute Côte-Nord" = "22"
    # )
    # return(cod_source_info_map[desc_source_info_f])

    key <- get_ref_key(
        table = "Source_Info",
        pkey_col = "COD_SOURCE_INFO",
        col = "DESC_SOURCE_INFO_F",
        val = desc_source_info_f)
    return(key)
}


#' Add the cod_serie_hist to the whole dataframe
#' This value is not present in ANDES so it will have to be specified here.
#' Run this without desc_serie_hist_f to get a list of choices.
#' @export
init_cod_serie_hist <- function(df, desc_serie_hist_f = NULL) {
    # only buld a list of choices if no descriptioin is specified.
    if (is.null(desc_serie_hist_f)) {
        # May have to update this list, perhaps we should build it fro mthe MS access db
        choices <- get_ref_choices(
            table = "Indice_Suivi_Etat_Stock",
            col = "DESC_SERIE_HIST_F"
        )
        stop("Please specify desc_serie_hist_f. Choices are: ", paste(choices, collapse = ", "))
        logger::log_error("Andes does not know about the COD_SERIE_HIST, showing possible choices...")
    }
    logger::log_info("Andes does not know about the COD_SERIE_HIST, so it was initialized as from {desc_serie_hist_f}.")

    # do not need to validate desc_serie_hist_f, the lookup will raise an arror if it is not legal.
    cod_serie_hist <- lookup_cod_serie_hist(desc_serie_hist_f)
    df["COD_SERIE_HIST"] <- cod_serie_hist
    # desc_serie_hist_f <-  df[, which(names(df) == "DESC_SERIE_HIST_F")]
    # df["COD_SERIE_HIST"] <- unlist(lapply(desc_serie_hist_f, lookup_cod_serie_hist))
    return(df)
}

#' @export
lookup_cod_serie_hist <- function(desc_serie_hist_f) {
    # # HACK hard lookup here
    # cod_serie_hist_map <- c(
    #     "Indice d'abondance zone 16E - pétoncle" <- 15,
    #     "Indice d'abondance zone 16F - pétoncle" <- 16,
    #     "Indice d'abondance zone 20 - pétoncle" <- 18,
    #     "Indice d'abondance buccin" <- 20
    # )
    # return(cod_serie_hist_map[desc_serie_hist_f])

    key <- get_ref_key(
        table = "Indice_Suivi_Etat_Stock",
        pkey_col = "COD_SERIE_HIST",
        col = "DESC_SERIE_HIST_F",
        val = desc_serie_hist_f)
    return(key)
}

#' @export
format_date_deb_projet <- function(df) {
    # get the col
    date_projet <- df[, which(names(df) == "DATE_DEB_PROJET")]
    df["DATE_DEB_PROJET"] <- unlist(lapply(date_projet, andes_str_to_oracle_date))
    return(df)
}

#' @export
format_date_fin_projet <- function(df) {
    # get the col
    date_projet <- df[, which(names(df) == "DATE_FIN_PROJET")]
    df["DATE_FIN_PROJET"] <- unlist(lapply(date_projet, andes_str_to_oracle_date))
    return(df)
}

#' @export
format_seq_pecheur <- function(df) {
    # get the col
    vessel_name <- df[, which(names(df) == "vessel_name")]
    # sad hack... :(
    if (vessel_name == "Leim") {
        pecheur <- "Capitaine Leim"
    } else {
        stop("The only supported vessel is the Leim for now. Cannot determine SEQ_PECHEUR for vessel: ", vessel_name)
        logger::log_error("The only supported vessel is the Leim for now. Cannot determine SEQ_PECHEUR for vessel: {vessel_name}")
    }

    logger::log_info("Assuming {pecheur} as NOM_PECHEUR (from the vessel {vessel_name})")

    seq_pecheur <- get_ref_key(table = "Pecheur",
                                pkey_col = "SEQ_PECHEUR",
                                col = "NOM_PECHEUR",
                                val = pecheur)

    df["SEQ_PECHEUR"] <- seq_pecheur
    # we are done with, the vessel_name column, it can be deleted.
    return(df)
}
