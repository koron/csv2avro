package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/hamba/avro"
)

var (
	schemaFile string
	inputFile  string
	outputFile string
)

func loadSchema() (avro.Schema, error) {
	return avro.ParseFiles(schemaFile)
}

func openInput() (io.Reader, error) {
	if inputFile == "" {
		return os.Stdin, nil
	}
	return os.Open(inputFile)
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
		return err
	}
	recSch, ok := sch.(*avro.RecordSchema)
	if !ok {
		return fmt.Errorf("type of root schema isn't record: %s", schemaFile)
	}
	in, err := openInput()
	if err != nil {
		return err
	}
	if c, ok := in.(io.Closer); ok {
		defer c.Close()
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
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("cvs2avro failed: %s", err)
	}
}
