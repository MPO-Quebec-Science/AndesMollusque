# Make sure the columns listed in col_names have numeric (or NA) values

Make sure the columns listed in col_names have numeric (or NA) values

## Usage

``` r
check_numeric_columns(df, col_names = NULL, coerce = FALSE)
```

## Arguments

- df:

  the dataframe to verify

- col_names:

  A list of column names. This will veridy if columns not in the list is
  present

- coerce:

  A boolean (false by default) to see if the dataframe can be coerced
  into compliance

## Value

A boolean representing if the dataframe is compliant.
