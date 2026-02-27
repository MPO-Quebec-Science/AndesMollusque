# Builds a list of legal choices (descriptions) for get ref key

Builds a list of legal choices (descriptions) for get ref key

## Usage

``` r
get_ref_choices(table = "tablename", col = "columnname", optional_query = "")
```

## Arguments

- table:

  The name of the Oracle table, defaults to "tablename"

- col:

  The column that holds the value to match, defaults to "columnname"

- optional_query:

  additional string to append to the query

## Value

The value found in the pkey column for the entry with the value

## Details

This essentially returns a list of values in table
