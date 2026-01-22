# Get the reference key corresponding to a value (usually from the Oracle / MSAccess reference database)

Get the reference key corresponding to a value (usually from the Oracle
/ MSAccess reference database)

## Usage

``` r
get_ref_key(
  table = "tablename",
  pkey_col = "columnofprimarykey",
  col = "columnname",
  val = "entryvalue",
  optional_query = ""
)
```

## Arguments

- table::

  The name of the Oracle table, defaults to "tablename"

- pkey_col::

  The column name that holds the key, defaults to "columnofprimarykey"

- col::

  The column that holds the value to match, defaults to "columnname"

- val::

  The value to match, defaults to "entryvalue"

- optional_query::

  additional string to append to the query

## Value

: The value found in the pkey column for the entry with the value
