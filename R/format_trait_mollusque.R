
#' @export
format_cod_strat <- function(trait, desc_serie_hist_f, cod_sect_releve) {
    # first get the strat from the desc_serie_hist_f and NO_STATION
    strat <- lapply(trait$NO_STATION, get_strat, desc_serie_hist_f)
    # then, lookup the strat code
    # trait$COD_STRAT <- lapply(strat, lookup_cod_strat, cod_sect_releve)

    # make a lookup table
    value <- unique(strat)
    code <- lapply(value, lookup_cod_strat, cod_sect_releve)
    code_map <- cbind(code = code, value=value)

    # use merge to apply the map
    strat <- list(value = unlist(strat))
    res <- merge(strat, code_map, by = "value")
    trait$COD_STRAT <- res$code
    return(trait)
}

#' @export
get_strat <- function(nom_station, desc_serie_hist_f){
    if(desc_serie_hist_f=="Indice d'abondance zone 16E - pétoncle"){
        # For 16E, the strat is the first letter of the station name
        strat <- substring(nom_station, 1, 1)
        return(strat)
    } else if(desc_serie_hist_f=="Indice d'abondance zone 16F - pétoncle"){
        # For 16F, the strat is the first letter of the station name
        strat <- substring(nom_station, 1, 1)
        return(strat)
    } else if(desc_serie_hist_f=="Indice d'abondance zone 20 - pétoncle"){
        # For IdM, need a lookup table from station
        station <- lookup_station(station_name=nom_station, zone="20", species="PETONCLE")
        strat <- station$STRAT
        return(strat)
    } else if(desc_serie_hist_f=="Indice d'abondance buccin") {
        # For buccin, need a lookup table from station
        station <- lookup_station(station_name=nom_station, species="BUCCIN")
        strat <- station$STRAT
        return(strat)
    } else {
        logger::log_error("get strat for {desc_serie_hist_f} has not been implemented.")
        stop("Cannot determine zone name")
    }
}


#' @export
lookup_cod_strat <- function(strat_name, cod_sect_releve) {
    optional_query <- paste("AND COD_SECTEUR_RELEVE=", cod_sect_releve, sep="")
    key <- get_ref_key(
        table = "TYPE_STRATE_MOLL",
        pkey_col = "COD_STRATE",
        col = "STRATE",
        val = strat_name,
        optional_query = optional_query)

    return(key)
}

#' @export
format_zone <- function(trait, desc_serie_hist_f) {
    # first, get the zone from the desc_serie_hist_f and NO_STATION
    zone <- lapply(trait$NO_STATION, get_zone, desc_serie_hist_f)
    # then lookup the code, add to dataframe
    # trait$COD_ZONE_GEST_MOLL <- lapply(zone, lookup_cod_zone_gest_moll)


    # make a lookup table
    value <- unique(zone)
    code <- lapply(value, lookup_cod_zone_gest_moll)
    code_map <- cbind(code = code, value=value)

    # use merge to apply the map
    zone <- list(value = unlist(zone))
    res <- merge(zone, code_map, by = "value")
    trait$COD_ZONE_GEST_MOLL <- res$code

    return(trait)
}

#' @export
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

#' @export
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


#' This requires opening station reference data to determine the zone.
lookup_station <- function(station_name=NULL, zone=NULL, species=NULL) {

    file_path <- system.file("ref_data",
            "STATION_MOLL.csv",
            package = "ANDESMollusque")

    ref_station <-read.csv(file_path, sep=",")
    # # filter out survey target species
    # ref_station < ref_station[ref_station$ESPECE == "BUCCIN", ]
    # zone <- 

    logger::log_warn("Whelk zone determination is not implemented, returning default zone 1.")
    return("1")
}




format_cod_typ_trait <- function(trait, desc_stratification) {

    desc_typ_trait <- lapply(trait$operation, get_desc_typ_trait, desc_stratification)

    # the naive way (commented-out here) is to simply lookup every cod, but it is slow, so make a map ahead of time
    # trait$cod_typ_trait <- lapply(desc_typ_trait, lookup_cod_typ_trait)

    # make a lookup table
    desc <-  unique(desc_typ_trait)
    code <- lapply(desc, lookup_cod_typ_trait)
    code_map <- cbind(code = code, desc=desc)

    # user merge to apply the map
    desc_typ_trait <- list(desc = unlist(desc_typ_trait))
    res <- merge(desc_typ_trait, code_map, by = "desc")
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

lookup_cod_typ_trait <- function(desc_typ_trait) {
    key <- get_ref_key(
    table = "TYPE_TRAIT",
    pkey_col = "COD_TYP_TRAIT",
    col = "DESC_TYP_TRAIT_F",
    val = desc_typ_trait)
    return(key)

}
#' Format dates for TRAIT_MOLLUSQUE
#'
#' Converts date and time fields to appropriate format
#'
#' @param df Input dataframe
#' @return Formatted dataframe
#' @export
format_date_trait <- function(trait_df) {
    # Convert start and end dates
    trait_df$DATE_HEURE_DEB_TRAIT <- sapply(trait_df$start_date, andes_str_to_oracle_datetime)
    trait_df$DATE_HEURE_FIN_TRAIT <- sapply(trait_df$end_date, andes_str_to_oracle_datetime)
    
    # Add time type and timezone codes
    trait_df$COD_TYP_HEURE <- determine_time_type(trait_df$start_date)
    trait_df$COD_FUSEAU_HORAIRE <- determine_timezone(trait_df$start_date)
    
    return(trait_df)
}


#' Determine time type (standard or daylight saving)
#'
#' @param datetime Input datetime
#' @return Time type code
#' @export
determine_time_type <- function(datetime) {
    # Implement logic to determine if time is standard or daylight saving
    # Similar to Python's implementation
    is_dst <- lubridate::dst(as.POSIXct(datetime, tz = "America/Montreal"))
    
    if (is_dst) {
        return(1)  # Daylight saving
    } else {
        return(0)  # Standard time
    }
}

#' Determine timezone
#'
#' @param datetime Input datetime
#' @return Timezone code
#' @export
determine_timezone <- function(datetime) {
    # Hardcoded to Quebec timezone
    logger::log_warn("Timezone is hardcoded to Quebec (America/Montreal).")


    return(1)  # Quebec timezone
}

#' Format coordinates for TRAIT_MOLLUSQUE
#'
#' Converts coordinates to Oracle-specific format
#'
#' @param df Input dataframe
#' @return Formatted dataframe
#' @export
format_coordinates <- function(df) {
    # Convert latitude and longitude to Oracle coordinate encoding
    df$LAT_DEB_TRAIT <- sapply(df$start_latitude, to_oracle_coord)
    df$LAT_FIN_TRAIT <- sapply(df$end_latitude, to_oracle_coord)

    # the longitudes need a negative
    df$LONG_DEB_TRAIT <- sapply(df$start_longitude, to_oracle_coord)
    df$LONG_FIN_TRAIT <- sapply(df$end_longitude, to_oracle_coord)

    
    return(df)
}

#' Convert coordinate to Oracle format
#'
#' @param coord Input coordinate
#' @return Formatted coordinate
#' @export
to_oracle_coord <- function(coord) {
    if (is.null(coord)) return(NULL)
    
    degrees <- floor(abs(coord))
    minutes_decimal <- (abs(coord) - degrees) * 60
    
    return(degrees * 100 + minutes_decimal)
}


#' Add hard-coded or computed values
#'
#' @param df Input dataframe
#' @return Dataframe with additional columns
#' @export
add_hard_coded_values <- function(df) {
    # Add hard-coded or computed values not present in original data
    df$COD_METHOD_POS <- 6  # GPS/DGPS
    df$NO_CHARGEMENT <- NULL
    df$DISTANCE_POS <- NULL
    df$VIT_TOUAGE <- NULL
    df$DUREE_TRAIT <- NULL
    
    return(df)
}

#' Format zone and sector information
#'
#' @param df Input dataframe
#' @return Formatted dataframe
#' @export
format_zone_secteur <- function(df) {
    # Implement logic to determine COD_ZONE_GEST_MOLL and COD_SECTEUR_RELEVE
    # This would likely involve lookup tables or specific business logic
    
    # Example placeholder logic
    df$COD_ZONE_GEST_MOLL <- sapply(df$area_of_operation, lookup_zone_gest_moll)
    df$COD_SECTEUR_RELEVE <- sapply(df$area_of_operation, lookup_secteur_releve)
    
    return(df)
}