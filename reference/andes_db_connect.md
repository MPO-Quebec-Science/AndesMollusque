# Establish a connection to the andes database

This is a wrapper for the \`DBI::dbConnect\`, see it's documentation for
more details.

## Usage

``` r
andes_db_connect(url_bd, port_bd, nom_usager, mot_de_passe, nom_bd = "andesdb")
```

## Arguments

- url_bd:

  URL of the ANDES database server.

- port_bd:

  Port number for the ANDES database connection.

- nom_usager:

  Username for the ANDES database.

- mot_de_passe:

  Password for the ANDES database.

- nom_bd:

  Name of the ANDES database. Default is "andesdb".

## Value

A connection object to the ANDES database.
