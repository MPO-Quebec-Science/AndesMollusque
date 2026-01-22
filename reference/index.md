# Package index

## All functions

- [`access_db_connect()`](access_db_connect.md) : Establish a connection
  to an MS Access database
- [`add_hard_coded_value()`](add_hard_coded_value.md) : Add a hard-coded
  column with a specific value to the dataframe
- [`andes_db_connect()`](andes_db_connect.md) : Establish a connection
  to the andes database
- [`andes_str_to_oracle_date()`](andes_str_to_oracle_date.md) : Convert
  andes datetime string form DB to Oracle date format
- [`andes_str_to_oracle_datetime()`](andes_str_to_oracle_datetime.md) :
  Convert andes datetime string form DB to Oracle datetime format
- [`check_cols_contains_na()`](check_cols_contains_na.md) : Checks if
  all dataframe cols named in the col_names contain NA This is useful to
  validate if a dataframe can be written to a DB table (where some
  columns values cannot be null)
- [`check_columns_present()`](check_columns_present.md) : Make sure the
  columns listed in col_names are present in the dataframe
- [`check_numeric_columns()`](check_numeric_columns.md) : Make sure the
  columns listed in col_names have numeric (or NA) values
- [`check_other_columns()`](check_other_columns.md) : Make sure no other
  columns than the ones listed in col_names are present in the dataframe
- [`cols_to_numeric()`](cols_to_numeric.md) : Convert all dataframe cols
  named in the col_names to a numeric value
- [`format_cod_descrip_capt()`](format_cod_descrip_capt.md) : For now,
  we will just keep this blank It is a function here so that one day we
  can use the ANDES relative abundance category But it is not used for
  commercial stocks, so we skip it
- [`format_cod_typ_mesure()`](format_cod_typ_mesure.md) : For now, we
  will just assume all data is quantitative since we are limiting
  ourselves to commercial (scallops and whelk). One day, this function
  should be generalized to also lookup the data and determine it.
- [`format_cod_typ_panier()`](format_cod_typ_panier.md) : Add the
  formated COD_TYP_PANIER column to the dataframe This is currently
  hard-coded to Panier doublé TODO: implement different types depending
  on chosen gear code, for exmaple:
- [`format_coordinates()`](format_coordinates.md) : Format coordinates
  for TRAIT_MOLLUSQUE
- [`format_date_hre_trait()`](format_date_hre_trait.md) : Format dates
  for TRAIT_MOLLUSQUE
- [`format_date_trait()`](format_date_trait.md) : Format dates for
  TRAIT_MOLLUSQUE
- [`format_epibiont()`](format_epibiont.md) : Formated
  COD_COUVERTURE_EPIBIONT column to the dataframe This categorizes AND
  includes the of the following definition: ave_coverage to
  cod_couverture : 0 -\> Aucune balane 1 -\> 1/3 et moins surface
  colonisée 2 -\> 1/3 à 2/3 surface colonisée 3 -\> 2/3 et plus surface
  colonisée
- [`generate_sql_insert_statement()`](generate_sql_insert_statement.md)
  : generate a SQL inster statement for the single dataframe row as a
  new row into table_name The dataframe must have named columns that
  correspond to the columns of the table The values must have the
  correct data types t(here will be some SQL value sanitizing) The
  statement will look like:" INSERT INTO table_name col_names_str VALUES
  col_values_str" Where col_names_str is list of column names with
  parentheses: "(NO_RELEVE COD_NBPC ANNEE COD_TYP_STRATIF
  DATE_DEB_PROJET...)" and col_values_str is list of column values with
  parentheses: "(36 4 2025 7 '2025-05-03'...)"
- [`get_access_table_properties()`](get_access_table_properties.md) :
  this function is meant to help the validation automatically getting
  colnames, no-null cols and datatypes. but I cannot get it to work here
  yet... (but works in Dbeaver)
- [`get_biometrie_petoncle()`](get_biometrie_petoncle.md) : Gets
  get_biometrie_petoncle (formatted results)
- [`get_biometrie_petoncle_db()`](get_biometrie_petoncle_db.md) : Gets
  capture_mollusc_db (raw database results)
- [`get_capture_mollusque()`](get_capture_mollusque.md) : Gets
  capture_mollusque (formatted results)
- [`get_capture_mollusque_db()`](get_capture_mollusque_db.md) : Gets
  capture_mollusc_db (raw database results)
- [`get_engin_mollusque()`](get_engin_mollusque.md) : Gets engin_mollusc
  (formatted results)
- [`get_engin_mollusque_db()`](get_engin_mollusque_db.md) : Gets
  engin_mollusc_db (raw database results)
- [`get_epibiont()`](get_epibiont.md) : This fetches the specimen-level
  coverage data and coverts it to an average set-level metric in
  accordance to the legacy Oracle database.
- [`get_freq_long_mollusque()`](get_freq_long_mollusque.md) : Gets
  freq_long_mollusque (formatted results)
- [`get_freq_long_mollusque_db()`](get_freq_long_mollusque_db.md) : Gets
  freq_long_mollusque_db (raw database results)
- [`get_legal_collection_names()`](get_legal_collection_names.md) : Get
  a list of legal collection names as a filter for
  get_biometrie_petoncle()
- [`get_projet_mollusque()`](get_projet_mollusque.md) : Gets fishing
  projet_mollusque (formatted results)
- [`get_projet_mollusque_db()`](get_projet_mollusque_db.md) : Gets
  fishing projet_mollusque (raw database results)
- [`get_ref_choices()`](get_ref_choices.md) : Builds a list of legal
  choices (descriptions) for get ref key
- [`get_ref_key()`](get_ref_key.md) : Get the reference key
  corresponding to a value (usually from the Oracle / MSAccess reference
  database)
- [`get_trait_mollusque()`](get_trait_mollusque.md) : Gets
  trait_mollusque (formatted results)
- [`get_trait_mollusque_db()`](get_trait_mollusque_db.md) : Gets
  trait_mollusque_db (raw database results)
- [`init_cod_serie_hist()`](init_cod_serie_hist.md) : Add the
  cod_serie_hist to the whole dataframe This value is not present in
  ANDES so it will have to be specified here. Run this without
  desc_serie_hist_f to get a list of choices.
- [`is_andes_time_str_dst()`](is_andes_time_str_dst.md) : Verify is the
  ANDES dattime string is in daylight savings time
- [`left_join()`](left_join.md) : a merge that preserves row and column
  order shamelessly stolen from
  https://stackoverflow.com/questions/17878048/merge-two-data-frames-while-keeping-the-original-row-order
- [`parse_andes_datetime()`](parse_andes_datetime.md) : Convert ANDES
  UTC time string and converts it to a POSIXct object
- [`sanitize_sql_value()`](sanitize_sql_value.md) : It will wrap string
  with an extra set of single quotes. It will escape every single quote
  by doubling it up This usualy does nothing to the value itself except
  inject the NULL string for NA/null and empty strings
- [`strip_alphabetic()`](strip_alphabetic.md) : This function removes
  alphabetic characters from a string
- [`to_oracle_coord()`](to_oracle_coord.md) : Convert coordinate to
  Oracle format
- [`validate_capture_mollusque()`](validate_capture_mollusque.md) :
  Perform database validation checks on the dataframe
- [`validate_engin_mollusque()`](validate_engin_mollusque.md) : Perform
  database validation checks on the dataframe
- [`validate_freq_long_mollusque()`](validate_freq_long_mollusque.md) :
  Perform database validation checks on the dataframe
- [`validate_projet_mollusque()`](validate_projet_mollusque.md) :
  Perform database validation checks on the dataframe
- [`validate_set_result()`](validate_set_result.md) : Validate set
  result consistency
- [`validate_trait_mollusque()`](validate_trait_mollusque.md) : Perform
  database validation checks on the dataframe
