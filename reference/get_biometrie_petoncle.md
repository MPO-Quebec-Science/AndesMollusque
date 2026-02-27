# Gets get_biometrie_petoncle (formatted results)

This function executes a SQL query to retrieve the needed andes data to
construct the biometry table. The current ANDES active mission will
determine for which data are returned.

## Usage

``` r
get_biometrie_petoncle(andes_db_connection, collection_name = NULL)
```

## Arguments

- andes_db_connection:

  a connection object to the ANDES database.

- collection_name:

  Optional. A string with the name of the collection, (e.g., "Conserver
  pour biométrie 16E").

## Value

A dataframe containing get_biometrie_petoncle table data.

## See also

\[get_biometrie_petoncle_db(), get_legal_collection_names()\] for the db
results
