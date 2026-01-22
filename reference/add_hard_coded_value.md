# Add a hard-coded column with a specific value to the dataframe

Add a hard-coded column with a specific value to the dataframe

## Usage

``` r
add_hard_coded_value(df, col_name = NULL, value = NULL)
```

## Arguments

- df:

  The original dataframe to modify with a new column

- col_name:

  The new column name

- value:

  The value to add to every row in this column. To add null values in
  the column use NA and not NULL.

## Value

The original dataframe with the new column added
