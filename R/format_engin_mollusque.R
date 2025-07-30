
#' Add the formated COD_TYP_PANIER column to the dataframe
#' This is currently hard-coded to Panier doublé
#' TODO: implement different types depending on chosen gear code, for exmaple:
#' 
#'  1->'Panier standard' if 'Drague Digby (4 paniers non doublés)'
#'  2->'Panier doublé' if 'Drague Digby (4 paniers doublés)'
#'  3->'Aucun' (or '0->Pas de panier dans l’engin') for anyhing else
#' @export
format_cod_typ_panier <- function(engin) {
    # don't normally need a lookup, this is always Panier doublé -> 2
    # but I'd rather hard-code a description than a code, it's easier to interpret.
    desc_typ_panier_f <- "Panier doublé"
    cod_typ_panier <- get_ref_key(
            table = "TYPE_PANIER",
            pkey_col = "COD_TYP_PANIER",
            col = "DESC_TYP_PANIER_F",
            val = desc_typ_panier_f)

    engin <- add_hard_coded_value(engin, col_name = "COD_TYP_PANIER", value = cod_typ_panier)
    return(engin)
}

