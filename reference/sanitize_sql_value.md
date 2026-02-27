# Sanitize value to SQL statement

It will wrap string with an extra set of single quotes. It will escape
every single quote by doubling it up This usualy does nothing to the
value itself except inject the NULL string for NA/null and empty strings

## Usage

``` r
sanitize_sql_value(value)
```

## Arguments

- value:

  The value to sanitize
