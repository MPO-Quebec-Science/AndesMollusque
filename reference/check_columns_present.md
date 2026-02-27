# Make sure the columns listed in col_names are present in the dataframe

Make sure the columns listed in col_names are present in the dataframe

## Usage

``` r
check_columns_present(df, col_names = NULL, coerce = FALSE)
```

## Arguments

- df:

  Dataframe, the dataframe to verify

- col_names:

  List of column names. This will verify if the names in the list are
  present.

- coerce:

  Logical, (FALSE by default) to see if the dataframe can be coerced
  into compliance

## Value

A Logical representing if the dataframe is compliant.
