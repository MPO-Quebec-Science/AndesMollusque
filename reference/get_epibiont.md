# This fetches the specimen-level coverage data and coverts it to an average set-level metric in accordance to the legacy Oracle database.

This fetches the specimen-level coverage data and coverts it to an
average set-level metric in accordance to the legacy Oracle database.

## Usage

``` r
get_epibiont(andes_db_connection, code_filter)
```

## Arguments

- andes_db_connection:

  A connection object to the ANDES database.

- code_filter:

  a list of species code to filter on, or NULL for no filtering
