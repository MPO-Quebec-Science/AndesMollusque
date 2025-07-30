
left_join <- function(x, y, ...) {
    # https://stackoverflow.com/questions/17878048/merge-two-data-frames-while-keeping-the-original-row-order

    x$join_id_ <- seq_len(nrow(x))
    joined <- merge(x = x, y = y, all.x = TRUE, sort = FALSE, ...)

    cols <- unique(c(colnames(x), colnames(y)))
    return(joined[order(joined$join_id),
         cols[cols %in% colnames(joined) & cols != "join_id_"]])
}

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
    timezone_str <- "America/Toronto"
    return(format(posixct_date, format = "%Y-%m-%d %H:%M:%S", tz = timezone_str))
}

#' @export 
is_andes_time_str_dst <-function(datetime_str) {
    is_dst <- NA
    # ANDES DB times are in UTC
    posixct_date <- parse_andes_datetime(datetime_str)
    # to see if EST vs EDT, convert to America/Toronto and look at offset
    timezone_str <- "America/Toronto"

    utc_offset <- format(posixct_date, format = "%z", tz = timezone_str)
    if (is.na(utc_offset)){
        logger::log_warn("Could not determine if Daylight savings is active for date: {datetime_str}")
        # stop("Could not determine if Daylight savings is active for date")
    } else if (utc_offset == "-0400") {
        is_dst <- TRUE
    } else if (utc_offset == "-0500") {
        is_dst <- FALSE
    } else {
        logger::log_warn("Could not determine if Daylight savings is active for date: {datetime_str}")
        # stop("Could not determine if Daylight savings is active for date")
    }
    return(is_dst)
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
        logger::log_error("add_hard_coded_value was called with bad arguments")
        stop("Both col_name and value must be provided.")
    }
    if (is.null(value)) {
        logger::log_error("A hard-coded NULL-value was added to column {col_name}")
        # this is what sanitize_sql_value() actually ends up doing...
        logger::log_error("DO NOT DO THIS! Please change to NA and switch to NULL when executing the statement")
    } else{
        logger::log_info("A hard-coded value of {value} was added to column {col_name}")
    }
    # add a hard coded value to the dataframe
    df[col_name] <- value
    return(df)
}

#' It will wrap string with an extra set of single quotes.
#' It will escape every single quote by doubling it up
#' This usualy does nothing to the value itself except inject the NULL string for NA/null and empty strings
#' @export
sanitize_sql_value <- function(value) {
    if (is.null(value) || is.na(value)) {
        return("NULL")
    } else if (is.character(value)) {
        if (nchar(value) == 0) {
            return("NULL")
        } else {
            # escape single quotes
            value <- gsub("'", "''", value)
            # wrap the whole in single single quotes
            value <- paste("'", value, "'", sep = "") 
            return (value)
        }
    }
    return (value)
}

#' Convert coordinate to Oracle format
#'
#'        For example, the latitude of 47.155927
#'        is decomposed into:
#'        whole_degrees = 47
# '       whole_minutes = 9
#'        decimal_minues = 35562
#'        and yields: 4709.35562
#' @param coord Input coordinate
#' @return Formatted coordinate
#' @export
to_oracle_coord <- function(coord) {
    if (is.null(coord) || is.na(coord)) {
        return(NA)
    }
    degrees <- floor(abs(coord))
    minutes_decimal <- (abs(coord) - degrees) * 60

    if (coord<0) {
        return(-1 * (degrees * 100 + minutes_decimal))
    } else {
        return(degrees * 100 + minutes_decimal)
    }
}

#' generate a SQL inster statement for the single dataframe row as a new row into table_name
#' The dataframe must have named columns that correspond to the columns of the table
#' The values must have the correct data types t(here will be some SQL value sanitizing)
#' The statement will look like:" INSERT INTO {table_name} {col_names_str} VALUES {col_values_str}"
#' Where {col_names_str} is list of column names with parentheses: "(NO_RELEVE COD_NBPC ANNEE COD_TYP_STRATIF DATE_DEB_PROJET...)"
#' and {col_values_str} is list of column values with parentheses: "(36 4 2025 7 '2025-05-03'...)"
#' @export
generate_sql_insert_statement <- function(df_row, table_name) {
    col_names <- NULL
    # remove id col if present
    if ("id" %in% names(df_row)) df_row <- subset(df_row, select = -c(id))

    for (col in colnames(df_row)) {
        col_names <- paste(col_names, col, sep = ", ")
    }
    # remove the leading comma and space
    col_names <- substr(col_names, 3, nchar(col_names))
    # add parenthesis
    col_names <- paste("(", col_names, ") ", sep = "")

    # run sanitize_sql_value() on every column of this row
    col_values_str <- lapply(df_row, sanitize_sql_value)
    # collapse all values into one string with columns separated by a comma
    col_values_str <- paste(unlist(col_values_str), collapse = ", ")
    # add parenthesis
    col_values_str <- paste("(", col_values_str, ") ", sep="")
    # the list is now build, we can create the INSERT statement.
    statement <- paste(
                    "INSERT INTO",
                    table_name,
                    col_names,
                    "VALUES",
                    col_values_str,
                    ";",
                    sep = " ")
    return(statement)
}

#'
#' Convert all dataframe cols named in the col_names to a numeric value
#' @param df: the datafram to modify
#' @param col_names: a list of column names which will be converted to numeric
#' @export
cols_to_numeric <- function(df, col_names = NULL) {
    if (is.null(col_names)) {
        stop("Must supply a list of column names")
    }
    for (i in seq_len(length(col_names))) {
        if (! col_names[i] %in% names(df)) {
            logger::log_error("Cannot convert type, {col_names[i]} is not a column name")
            stop("Cannot convert type, not a column name")
        }
        logger::log_debug("Converting column {col_names[i]} to numeric")
        df[, names(df) == col_names[i]] <- as.numeric(df[, names(df) == col_names[i]])
    }
    return (df)
}

#'
#' Checks if all dataframe cols named in the col_names contain NA
#' This is useful to validate if a dataframe can be written to a DB table (where some columns values cannot be null)
#' @param df: the datafram to modify
#' @param col_names: a list of column names which will be converted to numeric
#' @export
cols_contains_na <- function(df, col_names = NULL) {
    if (is.null(col_names)) {
        stop("Must supply a list of column names")
    }
    for (i in seq_len(length(col_names))) {
        if (! col_names[i] %in% names(df)) {
            logger::log_error("Cannot verify, {col_names[i]} is not a column name")
            stop("Cannot verify, not a column name")
        }
        if (any(is.na(df[, names(df) == col_names[i]]))) {
            logger::log_error("Found NA in column {col_names[i]}")
            return(TRUE)
        }
    }
    return(FALSE)
}