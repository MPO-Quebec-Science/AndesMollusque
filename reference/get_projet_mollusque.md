# Gets fishing projet_mollusque (formatted results)

This function executes a SQL query to retrieve the needed andes data to
construct the PROJET_MOLLUSQUE table. The current ANDES active mission
will determine for which data are returned.

## Usage

``` r
get_projet_mollusque(andes_db_connection)
```

## Arguments

- andes_db_connection:

  a connection object to the ANDES database.

## Value

A dataframe containing fishing set data.

## See also

\[get_projet_mollusque_db()\] for the raw database results
