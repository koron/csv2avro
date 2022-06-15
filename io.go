package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/hamba/avro"
)

func loadSchema(name string) (avro.Schema, error) {
	if name == "" {
		return nil, fmt.Errorf("no schema specified")
	}
	return avro.ParseFiles(name)
}

func openInput(name string) (io.Reader, io.Closer, error) {
	if name == "" {
		return os.Stdin, nil, nil
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

func openCSVReader(name string, forceTSV bool) (*csv.Reader, io.Closer, error) {
	in, c, err := openInput(name)
	if err != nil {
		return nil, nil, err
	}
	r := csv.NewReader(in)
	if forceTSV || strings.ToLower(filepath.Ext(name)) == ".tsv" {
		r.Comma = '\t'
	}
	return r, c, nil
}

func openOutput(name string) (io.Writer, error) {
	if name == "" {
		return os.Stdout, nil
	}
	return os.Create(name)
}
