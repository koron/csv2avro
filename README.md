# koron/csv2avro

[![PkgGoDev](https://pkg.go.dev/badge/github.com/koron/csv2avro)](https://pkg.go.dev/github.com/koron/csv2avro)
[![Actions/Go](https://github.com/koron/csv2avro/workflows/Go/badge.svg)](https://github.com/koron/csv2avro/actions?query=workflow%3AGo)
[![Go Report Card](https://goreportcard.com/badge/github.com/koron/csv2avro)](https://goreportcard.com/report/github.com/koron/csv2avro)

## How to install

```console
$ go install github.com/koron/csv2avro@latest
```

## Usage

```console
$ ./csv2avro -schema {SCHEMA} [-input {INPUT}] -output [OUTPUT]
```

* `SCHEMA`: AVRO schema, mandatory
* `INPUT`: optional, default is STDIN
* `OUTPUT`: optional, default is STDOUT

Example:

```console
$ ./csv2avro -schema my_shcema.avsc < input.csv > output.avro
```

## Supported Avro's types

* `string`
* `int`
* `long`
* `float`
* `double`
* `boolean`
* `null` - always null.

## Misc

Test command:

```console
$ ./csv2avro -schema testdata/sample1.avsc -input testdata/sample1.csv -output testdata/sample1.avro
```
