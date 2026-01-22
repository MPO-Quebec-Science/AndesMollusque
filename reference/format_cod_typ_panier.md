# Add the formated COD_TYP_PANIER column to the dataframe This is currently hard-coded to Panier doublé TODO: implement different types depending on chosen gear code, for exmaple:

1-\>'Panier standard' if 'Drague Digby (4 paniers non doublés)'
2-\>'Panier doublé' if 'Drague Digby (4 paniers doublés)' 3-\>'Aucun'
(or '0-\>Pas de panier dans l’engin') for anyhing else

## Usage

``` r
format_cod_typ_panier(engin)
```
