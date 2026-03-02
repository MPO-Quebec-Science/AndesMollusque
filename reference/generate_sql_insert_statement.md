# Generate a SQL statement

generate a SQL instert statement for the single dataframe row as a new
row into table_name The dataframe must have named columns that
correspond to the columns of the table The values must have the correct
data types (there will be some SQL value sanitizing) The statement will
look like:\`INSERT INTO {table_name} {col_names_str} VALUES
{col_values_str}\` Where \`{col_names_str}\` is list of column names
with parentheses: \`(NO_RELEVE COD_NBPC ANNEE COD_TYP_STRATIF
DATE_DEB_PROJET...)\` and \`{col_values_str}\` is list of column values
with parentheses: \`(36 4 2025 7 '2025-05-03'...)\`

## Usage

``` r
generate_sql_insert_statement(df_row, table_name)
```

## Arguments

- df_row:

  Dataframe row

- table_name:

  String
