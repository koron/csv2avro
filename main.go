package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/hamba/avro"
)

var (
	schemaFile string
	inputFile  string
	outputFile string
	forceTSV   bool
)

func run() error {
	sch, err := loadSchema(schemaFile)
	if err != nil {
		return fmt.Errorf("failed to load schema: %w", err)
	}
	recSch, ok := sch.(*avro.RecordSchema)
	if !ok {
		return fmt.Errorf("type of root schema isn't record: %s", schemaFile)
	}

	in, rc, err := openCSVReader(inputFile, forceTSV)
	if err != nil {
		return err
	}
	if rc != nil {
		defer rc.Close()
	}

	out, err := openOutput(outputFile)
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
