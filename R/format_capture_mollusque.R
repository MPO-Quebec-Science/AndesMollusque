
#' Formated COD_COUVERTURE_EPIBIONT column to the dataframe
#' This categorizes AND includes the of the following definition:
#' 0 -> Aucune balane
#' 1 -> 1/3 et moins surface colonisée
#' 2 -> 1/3 à 2/3 surface colonisée
#' 3 -> 2/3 et plus surface colonisée
#' @export
format_epibiont_data <- function(epibiont_data) {

    assert_col(epibiont_data, "ave_with_barnacles")
    assert_col(epibiont_data, "ave_coverage")

    #' 0 -> Aucune balane
    #' 1 -> 1/3 et moins surface colonisée
    #' 2 -> 1/3 à 2/3 surface colonisée
    #' 3 -> 2/3 et plus surface colonisée
    breaks <- c(0, 1/3., 2/3., 1)
    categories <- c(1, 2, 3)
    # We will handle case 0 later, no barnacle cases should have ave_coverage=NA

    # this should take care of all legal values of area
    cod_couverture <- cut(epibiont_data$ave_coverage, breaks = breaks, labels = categories, )
    # we don't actually want a factor, but a list with values from categories
    cod_couverture <- categories[as.integer(cod_couverture)]

    # now handle edge case when there is no legal area (because there are no barnacles)
    is_na_because_no_barnacles <- is.na(cod_couverture) & (epibiont_data$ave_with_barnacles == 0)
    cod_couverture[is_na_because_no_barnacles] <- 0

    epibiont_data$cod_couverture_epibiont <- cod_couverture

    return(capt)
}
