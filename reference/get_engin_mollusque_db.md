# Gets engin_mollusc_db (raw database results)

This function executes a SQL query to retrieve the needed andes data to
construct the ENGIN_MOLLUSC table. The current ANDES active mission will
determine for which data are returned.

## Usage

``` r
get_engin_mollusque_db(andes_db_connection)
```

## Arguments

- andes_db_connection:

  a connection object to the ANDES database.

## Value

A dataframe containing fishing set data.

## Details

This function is intended for internal use and returns raw results from
the database. It is not meant for direct use in analysis or reporting.
Users should use \`get_engin_mollusc\`

## See also

\[get_engin_mollusque()\] for the formatted results
