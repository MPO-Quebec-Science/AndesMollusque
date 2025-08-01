
#' Formated COD_COUVERTURE_EPIBIONT column to the dataframe
#' This categorizes AND includes the of the following definition:
#' ave_coverage to cod_couverture :
#' 0 -> Aucune balane
#' 1 -> 1/3 et moins surface colonisée
#' 2 -> 1/3 à 2/3 surface colonisée
#' 3 -> 2/3 et plus surface colonisée
#' 
#' 
#' #' ave_with_barnacles to cod_abondance
#' 0 -> Aucun des pétoncles ne porte de balane
#' 1 -> 1% à 20%% des pétoncles portent des balanes
#' 2 -> 21% à 40%% des pétoncles portent des balanes
#' 3 -> 41% à 60%% des pétonlces portent des balanes
#' 4 -> 61% à 80%% despétoncles portent des balanes
#' 5 -> 81% à 100%% des pétonlces portent des balanes
#' @param epibiont_data the dataframe must contain columns "ave_with_barnacles" and "ave_coverage"
#' @value The input dataframe with columns for categorical codes
#' @export
format_epibiont_data <- function(epibiont_data) {

    assert_col(epibiont_data, "ave_with_barnacles")
    assert_col(epibiont_data, "ave_coverage")

    # cod_couverture categories:
    #' 0 -> Aucune balane
    #' 1 -> 1/3 et moins surface colonisée
    #' 2 -> 1/3 à 2/3 surface colonisée
    #' 3 -> 2/3 et plus surface colonisée
    breaks <- c(0, 1 / 3., 2 / 3., 1)
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


    # cod_abondance categories:
    # 0 -> Aucun des pétoncles ne porte de balane
    # 1 -> 1% à 20%% des pétoncles portent des balanes
    # 2 -> 21% à 40%% des pétoncles portent des balanes
    # 3 -> 41% à 60%% des pétonlces portent des balanes
    # 4 -> 61% à 80%% despétoncles portent des balanes
    # 5 -> 81% à 100%% des pétonlces portent des balanes
    breaks <- c(0, 0.01, 0.21, 0.41, 0.61, 0.81, 1.0)
    categories <- c(0, 1, 2, 3, 4, 5)

    # this should take care of all legal values of area
    cod_abondance <- cut(epibiont_data$ave_with_barnacles, breaks = breaks, labels = categories, include.lowest=TRUE)
    # we don't actually want a factor, but a list with values from categories
    cod_abondance <- categories[as.integer(cod_abondance)]
    epibiont_data$cod_abondance <- cod_abondance

    return(epibiont_data)
}
