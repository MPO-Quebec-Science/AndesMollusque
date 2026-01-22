# Gets trait_mollusque (formatted results)

This function executes a SQL query to retrieve the needed andes data to
construct the TRAIT_MOLLUSQUE table and formats the results.

## Usage

``` r
get_trait_mollusque(andes_db_connection, proj = NULL)
```

## Arguments

- andes_db_connection:

  a connection object to the ANDES database.

- proj:

  A formatted projet_mollusque dataframe

## Value

A dataframe containing formatted fishing set data.
