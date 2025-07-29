
#' @export
format_cod_strate <- function(trait, desc_serie_hist_f, cod_secteur_releve) {
    lookup_cod_strate <- function(strate_name, cod_sect_releve) {
        optional_query <- paste("AND COD_SECTEUR_RELEVE=", cod_secteur_releve, sep="")
        key <- get_ref_key(
            table = "TYPE_STRATE_MOLL",
            pkey_col = "COD_STRATE",
            col = "STRATE",
            val = strate_name,
            optional_query = optional_query)
        return(key)
    }

    # first get the strat from the desc_serie_hist_f and NO_STATION
    strate <- lapply(trait$NO_STATION, get_strate, desc_serie_hist_f)
    # then, lookup the strat code
    # trait$COD_STRATE <- lapply(strat, lookup_cod_strat, cod_secteur_releve)

    # make a lookup table
    value <- unique(strate)
    code <- lapply(value, lookup_cod_strate, cod_secteur_releve)
    code_map <- data.frame(code = unlist(code), value = unlist(value))

    # use merge to apply the map
    strate <- data.frame(value = unlist(strate))
    # res <- merge(strate, code_map, by = "value", all.x = TRUE, sort = FALSE)
    res <- left_join(strate, code_map, by = "value")

    trait$COD_STRATE <- res$code
    return(trait)
}

#' @export
get_strate <- function(nom_station, desc_serie_hist_f) {
    #' This requires opening station reference data to determine the zone.
    lookup_station <- function(station_name = NULL, zone = NULL, species = NULL) {
        logger::log_error("lookup_station is not fully implemented.")

        file_path <- system.file("ref_data",
                "STATION_MOLL.csv",
                package = "ANDESMollusque")

        ref_station <-read.csv(file_path, sep = ",")
        # # filter out survey target species
        # ref_station < ref_station[ref_station$ESPECE == "BUCCIN", ]
        # zone <- 
        logger::log_warn("Whelk zone determination is not implemented, returning default zone 1.")
        return("1")
    }


    if(desc_serie_hist_f=="Indice d'abondance zone 16E - pétoncle"){
        # For 16E, the strate is the first letter of the station name
        strate <- substring(nom_station, 1, 1)
        return(strate)
    } else if(desc_serie_hist_f=="Indice d'abondance zone 16F - pétoncle"){
        # For 16F, the strate is the first letter of the station name
        strate <- substring(nom_station, 1, 1)
        return(strate)
    } else if(desc_serie_hist_f=="Indice d'abondance zone 20 - pétoncle"){
        # For IdM, need a lookup table from station
        station <- lookup_station(station_name=nom_station, zone="20", species="PETONCLE")
        strate <- station$STRATE
        return(strate)
    } else if(desc_serie_hist_f=="Indice d'abondance buccin") {
        # For buccin, need a lookup table from station
        station <- lookup_station(station_name=nom_station, species="BUCCIN")
        strate <- station$STRATE
        return(strate)
    } else {
        logger::log_error("get strate for {desc_serie_hist_f} has not been implemented.")
        stop("Cannot determine zone name")
    }
}


#' @export
format_zone <- function(trait, desc_serie_hist_f) {

    get_zone <- function(nom_station, desc_serie_hist_f){
        if(desc_serie_hist_f=="Indice d'abondance zone 16E - pétoncle"){
            # For 16E
            zone <- "16E"
            return(zone)
        } else if(desc_serie_hist_f=="Indice d'abondance zone 16F - pétoncle"){
            # For 16F
            zone <- "16F"
            return(zone)
        } else if(desc_serie_hist_f=="Indice d'abondance zone 20 - pétoncle"){
            # For IdM
            zone <- "20"
            return(zone)
        } else if(desc_serie_hist_f=="Indice d'abondance buccin") {
            stop("Buccin is not implemented yet.")
            return("")
        } else {
            logger::log_error("get strat for {desc_serie_hist_f} has not been implemented.")
            stop("Cannot determine zone name")
        }
    }

    lookup_cod_zone_gest_moll <- function(zone_name) {
        # code from zone_name is one of
        # 7 -> 16E
        # 8 -> 16F
        # 17 -> 20
        # 18 -> 1
        # 19 -> 2
        key <- get_ref_key(
            table = "ZONE_GEST_MOLL",
            pkey_col = "COD_ZONE_GEST_MOLL",
            col = "ZONE_GEST_MOLL",
            val = zone_name)
        return(key)
    }

    # first, get the zone from the desc_serie_hist_f and NO_STATION
    zone <- lapply(trait$NO_STATION, get_zone, desc_serie_hist_f)
    # then lookup the code, add to dataframe
    # trait$COD_ZONE_GEST_MOLL <- lapply(zone, lookup_cod_zone_gest_moll)


    # make a lookup table
    value <- unique(zone)
    code <- lapply(value, lookup_cod_zone_gest_moll)
    code_map <- data.frame(code = unlist(code), value = unlist(value))

    # use merge to apply the map
    zone <- data.frame(value = unlist(zone))
    # res <- merge(zone, code_map, by = "value", all.x = TRUE, sort = FALSE)
    res <- left_join(zone, code_map, by = "value")
    trait$COD_ZONE_GEST_MOLL <- res$code

    return(trait)
}



#' @export
format_no_station <- function(trait) {
    trait$NO_STATION <- sapply(trait$NO_STATION, strip_alphabetic)
    return(trait)
}

#' This function removes alphabetic characters from a string
strip_alphabetic <- function(my_string) {
    # It is mostly used to reformat pétoncle minganie station names that start with a letter
    numeric_string <- gsub("[A-Za-z]", "", my_string)
    return(numeric_string)
}



format_cod_typ_trait <- function(trait, desc_stratification) {

    lookup_cod_typ_trait <- function(desc_typ_trait) {
            key <- get_ref_key(
            table = "TYPE_TRAIT",
            pkey_col = "COD_TYP_TRAIT",
            col = "DESC_TYP_TRAIT_F",
            val = desc_typ_trait)
        return(key)
    }

    desc_typ_trait <- lapply(trait$operation, get_desc_typ_trait, desc_stratification)

    # the naive way (commented-out here) is to simply lookup every cod, but it is slow, so make a map ahead of time
    # trait$cod_typ_trait <- lapply(desc_typ_trait, lookup_cod_typ_trait)

    # make a lookup table
    desc <-  unique(desc_typ_trait)
    code <- lapply(desc, lookup_cod_typ_trait)
    code_map <- data.frame(code = unlist(code), desc = unlist(desc))

    # use merge to apply the map
    desc_typ_trait <- data.frame(desc = unlist(desc_typ_trait))
    # res <- merge(desc_typ_trait, code_map, by = "desc", all.x = TRUE, sort = FALSE)
    res <- left_join(desc_typ_trait, code_map, by = "desc")

    trait$COD_TYP_TRAIT <- res$code
    return(trait)

}

get_desc_typ_trait <- function(operation, desc_stratification) {
    if (operation=="ctd") {
        return("Océanographie seulement")
    } else if (operation=="fish"){
        # this is a fishing operation, must return the mission's stratification type 
        return(desc_stratification)
    } else {
        logger::log_error("cannot get desc_typ_trait, verify that operatin is one of ctd or fish")
        stop("cannot get desc_typ_trait, verify that operation is one of ctd or fish")
    }
}



#' Format dates for TRAIT_MOLLUSQUE
#'
#' Converts date fields to appropriate format. uses andes_str_to_oracle_date
#'
#' @param df Input dataframe
#' @return Formatted dataframe
#' @export
format_date_trait <- function(trait) {
    # Convert start and end dates
    trait$DATE_DEB_TRAIT <- unlist(lapply(trait$DATE_DEB_TRAIT, andes_str_to_oracle_date))
    trait$DATE_FIN_TRAIT <- unlist(lapply(trait$DATE_FIN_TRAIT, andes_str_to_oracle_date))
    return(trait)
}

#' Format dates for TRAIT_MOLLUSQUE
#'
#' Converts and time fields to appropriate format uses andes_str_to_oracle_datetime
#'
#' @param df Input dataframe
#' @return Formatted dataframe
#' @export
format_date_hre_trait <- function(trait) {
    # Convert start and end dates
    trait$HRE_DEB_TRAIT <- unlist(lapply(trait$HRE_DEB_TRAIT, andes_str_to_oracle_datetime))
    trait$HRE_FIN_TRAIT <- unlist(lapply(trait$HRE_FIN_TRAIT, andes_str_to_oracle_datetime))
    return(trait)
}

format_cod_typ_heure <- function(trait){

    lookup_cod_typ_heure <-function(is_dst){
        if (is.na(is_dst)) {
            return(NA)
        } else if (is_dst==TRUE) {
            desc <- "Avancée"
        } else if (is_dst==FALSE) {
            desc <- "Normale"
        } else {
            return(NA)
        }

        key <- get_ref_key(
            table = "TYPE_HEURE",
            pkey_col = "COD_TYP_HEURE",
            col = "DESC_TYP_HEURE_F",
            val=desc)
        return(key)
    }
    # use set start as reference
    # hopefully, HRE_DEB_TRAIT is still in ANDES format, call this before format_date_hre_trait()
    is_dst <- lapply(trait$HRE_DEB_TRAIT, is_andes_time_str_dst)

    # we can how map to cod_type_heure
    # make a lookup table
    desc <-  unique(is_dst)
    code <- lapply(desc, lookup_cod_typ_heure)
    code_map <- data.frame(code = unlist(code), desc = unlist(desc))

    # use merge to apply the map
    is_dst <- data.frame(desc = unlist(is_dst))
    res <- left_join(is_dst, code_map, by = "desc")
    trait$COD_TYP_HEURE <- res$code
    return(trait)
}


#' Format coordinates for TRAIT_MOLLUSQUE
#'
#' Converts coordinates to Oracle-specific format
#'
#' @param df Input dataframe
#' @return Formatted dataframe
#' @export
format_coordinates <- function(trait) {
    # Convert latitude and longitude to Oracle coordinate encoding
    trait$LAT_DEB_TRAIT <- unlist(lapply(trait$LAT_DEB_TRAIT, to_oracle_coord))
    trait$LAT_FIN_TRAIT <- unlist(lapply(trait$LAT_FIN_TRAIT, to_oracle_coord))

    # the longitudes need a negative
    trait$LONG_DEB_TRAIT <- unlist(lapply(trait$LONG_DEB_TRAIT, to_oracle_coord))
    trait$LONG_FIN_TRAIT <- unlist(lapply(trait$LONG_FIN_TRAIT, to_oracle_coord))
    return(trait)
}

