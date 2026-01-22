# Convert coordinate to Oracle format

For example, the latitude of 47.155927 is decomposed into: whole_degrees
= 47 decimal_minues = 35562 and yields: 4709.35562

## Usage

``` r
to_oracle_coord(coord)
```

## Arguments

- coord:

  Input coordinate

## Value

Formatted coordinate

## Details

The Oracle Coordinates (including longitude) are not negative.
