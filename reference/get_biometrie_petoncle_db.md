# Gets capture_mollusc_db (raw database results)

This function executes a SQL query to retrieve the needed andes data to
construct the biometry table. The current ANDES active mission will
determine for which data are returned.

## Usage

``` r
get_biometrie_petoncle_db(andes_db_connection, collection_name = NULL)
```

## Arguments

- andes_db_connection:

  a connection object to the ANDES database.

- collection_name:

  Optional. A string with the name of the colelction, (e.g., "Conserver
  pour biométrie 16E").

## Value

A dataframe containing fishing set data.

## Details

This function is intended for internal use and returns raw results from
the database. It is not meant for direct use in analysis or reporting.
Users should use \`get_biometrie_petoncle\`

## See also

\[get_biometrie_petoncle()\] for the formatted results
