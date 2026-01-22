# Make sure the columns listed in col_names are present in the dataframe

Make sure the columns listed in col_names are present in the dataframe

## Usage

``` r
check_columns_present(df, col_names = NULL, coerce = FALSE)
```

## Arguments

- df::

  the dataframe to verify

- col_names::

  A list of column names. This will verify if the names in the list are
  present.

- coerce::

  A boolean (false by default) to see if the dataframe can be coerced
  into compliance

## Value

A boolean representing if the dataframe is compliant.
