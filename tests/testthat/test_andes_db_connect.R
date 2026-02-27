# test msg erreur
testthat::test_that("Show error when input is bad", {
  testthat::expect_error(
    get_access_table_properties(table_name = NULL),
    "Must supply a table name"
  )
})
