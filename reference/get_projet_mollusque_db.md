# Gets fishing projet_mollusque (raw database results)

This function executes a SQL query to retrieve the needed andes data to
construct the PROJET_MOLLUSQUE table. The current ANDES active mission
will determine for which data are returned.

## Usage

``` r
get_projet_mollusque_db(andes_db_connection)
```

## Arguments

- andes_db_connection:

  a connection object to the ANDES database.

## Value

A dataframe containing fishing set data.

## Details

This function is intended for internal use and returns raw results from
the database. It is not meant for direct use in analysis or reporting.
Users should use \`get_projet_mollusque\`

## See also

\[get_projet_mollusque()\] for the formatted results
