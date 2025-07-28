#' @export
cleanup_text <- function(df, col_name = NULL, max_chars = NULL) {
    # get the col
    andes_text <- df[, which(names(df) == col_name)]
    # cleanup, remove line breaks from the text block
    andes_text <- gsub("\r?\n|\r", " ", andes_text)

    if (! is.null(max_chars) && nchar(andes_text) > max_chars) {
        # truncate the text to max_chars
        andes_text <- substr(andes_text, 1, max_chars)
        logger::log_warn("The text in column {col_name} was truncated to {max_chars} characters.")
    }

    df[col_name] <- andes_text
    return(df)
}

#' @export
andes_str_to_oracle_date <- function(datetime_str) {
    # as posixlt
    # posixct_date <- unlist(lapply(date_str, parse_andes_datetime))
    posixct_date <- parse_andes_datetime(datetime_str)
    return(format(posixct_date, format = "%Y-%m-%d"))
}

#' @export
andes_str_to_oracle_datetime <- function(datetime_str) {
    # as posixlt
    # posixct_date <- unlist(lapply(date_str, parse_andes_datetime))
    posixct_date <- parse_andes_datetime(datetime_str)
    timezone_str <- "America/Montreal"
    return(format(posixct_date, format = "%Y-%m-%d %H:%M:%S", tz = timezone_str))
}

#' takes a standard ANDES UTC time string and converts it to a POSIXct object
#' @export
parse_andes_datetime <- function(andes_time_str) {
  # if (is.na(andes_time_str)==TRUE) {
  #   return(NA)
  # }
  parsed_time <- as.POSIXct(andes_time_str, format = "%Y-%m-%d %H:%M:%S", tz = "UTC", optional=TRUE)
  # Convert ISO 8601 time to POSIXlt, ANDES DB time values are implicitly in UTC
  return(parsed_time)
}

#' @export
add_hard_coded_value <- function(df, col_name = NULL, value = NULL) {
    if (is.null(col_name)) {
        stop("Both col_name and value must be provided.")
        logger::log_error("add_hard_coded_value was called with bad arguments")
    }
    logger::log_info("A hard-coded value of {value} was added to column {col_name}")
    # add a hard coded value to the dataframe
    df[col_name] <- value
    return(df)
}

#' It will wrap string with an extra set of single quotes.
#' This usualy does nothing to the value itself except inject the NULL string for NA/null and empty strings
#' @export
sanitize_sql_value <- function(value) {
    if (is.null(value)) {
        return("NULL")
    } else if (is.na(value)) {
        return("NULL")
    } else if (is.character(value)) {
        if (nchar(value) == 0) {
            return("NULL")
        } else {
            return (paste("'", value, "'", sep = ""))
        }
    }
    return (value)
}

