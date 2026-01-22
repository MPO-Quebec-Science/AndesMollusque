# Establish a connection to an MS Access database

This is a wrapper for the \`DBI::dbConnect\`, see it's documentation for
more details.

## Usage

``` r
access_db_connect(file_path = NULL)
```

## Arguments

- file_path:

  full path to the MS Access file. The default (if NULL) will be the
  Access template database

## Value

A DBI connection object to the MS ACCESS database.
