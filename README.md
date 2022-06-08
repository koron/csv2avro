# koron/cvs2avro

[![PkgGoDev](https://pkg.go.dev/badge/github.com/koron/cvs2avro)](https://pkg.go.dev/github.com/koron/cvs2avro)
[![Actions/Go](https://github.com/koron/cvs2avro/workflows/Go/badge.svg)](https://github.com/koron/cvs2avro/actions?query=workflow%3AGo)
[![Go Report Card](https://goreportcard.com/badge/github.com/koron/cvs2avro)](https://goreportcard.com/report/github.com/koron/cvs2avro)

## How to install

```console
$ go install github.com/koron/csv2avro@latest
```

## Usage

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
$ ./cvs2avro -schema testdata/sample1.avsc -input testdata/sample1.csv -output testdata/sample1.avro
```
