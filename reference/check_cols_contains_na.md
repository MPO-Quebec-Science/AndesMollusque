# Checks if all dataframe cols named in the col_names contain NA This is useful to validate if a dataframe can be written to a DB table (where some columns values cannot be null)

Checks if all dataframe cols named in the col_names contain NA This is
useful to validate if a dataframe can be written to a DB table (where
some columns values cannot be null)

## Usage

``` r
check_cols_contains_na(df, col_names = NULL)
```

## Arguments

- df:

  The dataframe to modify

- col_names:

  A list of column names which will be converted to numeric

## Value

A boolean representing if the dataframe is compliant.
