# koron/csv2avro

[![PkgGoDev](https://pkg.go.dev/badge/github.com/koron/csv2avro)](https://pkg.go.dev/github.com/koron/csv2avro)
[![Actions/Go](https://github.com/koron/csv2avro/workflows/Go/badge.svg)](https://github.com/koron/csv2avro/actions?query=workflow%3AGo)
[![Go Report Card](https://goreportcard.com/badge/github.com/koron/csv2avro)](https://goreportcard.com/report/github.com/koron/csv2avro)

Convert CSV/TSV file to AVRO file.

## How to install

```console
$ go install github.com/koron/csv2avro@latest
```

## Features

* Detect CSV or TSV by extension of file name
* Auto detect a header row

## Usage

```console
$ csv2avro -schema {SCHEMA} [-input {INPUT}] [-output {OUTPUT}] [OPTIONS]
```

* `SCHEMA`: AVRO schema, mandatory
* `INPUT`: optional, default is STDIN
    It will be treated as TSV when a file name ends with ".tsv".
    Otherwise it is treated as CSV.
* `OUTPUT`: optional, default is STDOUT

Other options:

* `-tsv` Force input as TSV.

Example:

```console
$ csv2avro -schema my_shcema.avsc < input.csv > output.avro

$ csv2avro -schema my_shcema.avsc -tsv < input.tsv > output.avro
```

## Supported Avro's types

* `string`
* `int`
* `long`
* `float`
* `double`
* `boolean`
* `null` - always null.

## Misc for development

Test command:

```console
$ go build
$ ./csv2avro -schema testdata/sample1.avsc -input testdata/sample1.csv -output testdata/sample1.avro
```
