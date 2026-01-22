# Formated COD_COUVERTURE_EPIBIONT column to the dataframe This categorizes AND includes the of the following definition: ave_coverage to cod_couverture : 0 -\> Aucune balane 1 -\> 1/3 et moins surface colonisée 2 -\> 1/3 à 2/3 surface colonisée 3 -\> 2/3 et plus surface colonisée

\#' ave_with_barnacles to cod_abondance 0 -\> Aucun des pétoncles ne
porte de balane 1 -\> 1 2 -\> 21 3 -\> 41 4 -\> 61 5 -\> 81

## Usage

``` r
format_epibiont(andes_db_connection, code_filter)
```

## Arguments

- andes_db_connection:

  A connection object to the ANDES database.

- code_filter:

  a list of species code to filter on, or NULL for no filtering

## Value

The input dataframe with columns for categorical codes
