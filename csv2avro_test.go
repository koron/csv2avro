package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/hamba/avro"
)

func testCsvToAvro(t *testing.T, name string, sch *avro.RecordSchema, want []byte) {
	r, rc, err := openCSVReader(name, false)
	if err != nil {
		t.Fatalf("failed to open CSV/TSV: %s", err)
	}
	if rc != nil {
		defer rc.Close()
	}
	w := &bytes.Buffer{}
	err = csv2avro(sch, r, w)
	if err != nil {
		t.Errorf("csv2avro failed: %s", err)
		return
	}
	if !bytes.Equal(w.Bytes(), want) {
		t.Error("don't match")
	}
}

func TestCsvToAvro(t *testing.T) {
	want, err := os.ReadFile("testdata/sample1.avro")
	if err != nil {
		t.Fatalf("failed to read answer file: %s", err)
	}
	sch, err := loadRecordSchema("testdata/sample1.avsc")
	if err != nil {
		t.Fatalf("failed to load schema: %s", err)
	}

	t.Run("CSV", func(t *testing.T) {
		testCsvToAvro(t, "testdata/sample1.csv", sch, want)
	})
	t.Run("TSV", func(t *testing.T) {
		testCsvToAvro(t, "testdata/sample1.tsv", sch, want)
	})
	t.Run("TSV+header", func(t *testing.T) {
		testCsvToAvro(t, "testdata/sample1a.tsv", sch, want)
	})
	t.Run("TSV+header+reorder", func(t *testing.T) {
		testCsvToAvro(t, "testdata/sample1b.tsv", sch, want)
	})
}
