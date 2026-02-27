# Gets capture_mollusque (formatted results)

This function executes a SQL query to retrieve the needed andes data to
construct the CAPTURE_MOLLUSC table. The current ANDES active mission
will determine for which data are returned.

## Usage

``` r
get_capture_mollusque(
  andes_db_connection,
  engin = NULL,
  code_filter = NULL,
  basket_class_filter = NULL
)
```

## Arguments

- andes_db_connection:

  a connection object to the ANDES database.

- engin:

  Dataframe

- code_filter:

  Optional list of species to filter

- basket_class_filter:

  Optional list of basket class to filter

## Value

A dataframe containing capture_mollusque table data.

## Details

Structurally, it would make sense to send a Trait instance to for every
capture_mollusque but here we can get away with proj (due to how ANDES
is structured)

## See also

\[get_capture_mollusque_db()\] for the db results
