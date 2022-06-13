package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/hamba/avro"
)

var (
	schemaFile string
	inputFile  string
	outputFile string
	forceTSV   bool
)

func loadSchema() (avro.Schema, error) {
	if schemaFile == "" {
		return nil, fmt.Errorf("no schema specified")
	}
	return avro.ParseFiles(schemaFile)
}

func openInput() (io.Reader, io.Closer, error) {
	if inputFile == "" {
		return os.Stdin, nil, nil
	}
	f, err := os.Open(inputFile)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

func openCSVReader() (*csv.Reader, io.Closer, error) {
	in, c, err := openInput()
	if err != nil {
		return nil, nil, err
	}
	r := csv.NewReader(in)
	if forceTSV || strings.ToLower(filepath.Ext(inputFile)) == ".tsv" {
		r.Comma = '\t'
	}
	return r, c, nil
}

func openOutput() (io.Writer, error) {
	if outputFile == "" {
		return os.Stdout, nil
	}
	return os.Create(outputFile)
}

func run() error {
	sch, err := loadSchema()
	if err != nil {
		return fmt.Errorf("failed to load schema: %w", err)
	}
	recSch, ok := sch.(*avro.RecordSchema)
	if !ok {
		return fmt.Errorf("type of root schema isn't record: %s", schemaFile)
	}

	in, rc, err := openCSVReader()
	if err != nil {
		return err
	}
	if rc != nil {
		defer rc.Close()
	}

	out, err := openOutput()
	if err != nil {
		return err
	}
	if c, ok := out.(io.Closer); ok {
		defer c.Close()
	}

	return csv2avro(recSch, in, out)
}

func main() {
	flag.StringVar(&schemaFile, "schema", "", "avro schema file")
	flag.StringVar(&inputFile, "input", "", "input file, default STDIN")
	flag.StringVar(&outputFile, "output", "", "output file, default STDOUT")
	flag.BoolVar(&forceTSV, "tsv", false, "force input is TSV")
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("csv2avro failed: %s", err)
	}
}
