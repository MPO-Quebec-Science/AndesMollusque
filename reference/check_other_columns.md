# Make sure no other columns than the ones listed in col_names are present in the dataframe

Make sure no other columns than the ones listed in col_names are present
in the dataframe

## Usage

``` r
check_other_columns(df, col_names = NULL, coerce = FALSE)
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
