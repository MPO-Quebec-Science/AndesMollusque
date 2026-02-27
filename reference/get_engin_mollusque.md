# Gets engin_mollusc (formatted results)

This function executes a SQL query to retrieve the needed andes data to
construct the ENGIN_MOLLUSC table. The current ANDES active mission will
determine for which data are returned.

## Usage

``` r
get_engin_mollusque(andes_db_connection, proj = NULL)
```

## Arguments

- andes_db_connection:

  a connection object to the ANDES database.

- proj:

  Dataframe

## Value

A dataframe containing engin table data.

## Details

Structurally, it would make sense to send a Trait instance to for every
engin but here we can get away with proj (due to how ANDES is
structured)

## See also

\[get_engin_mollusque_db()\] for the db results
