# Validate set result consistency

ANDES has a mecanism to tag valid sets, but we also inserted a legacy
set observation for a similar reason. This functions makes sure both
point in the same direction and prints an error code if there are
contradictions.

## Usage

``` r
validate_set_result(trait)
```

## Arguments

- trait:

  Input dataframe

## Value

Formatted dataframe
