package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/hamba/avro"
)

type Converter interface {
	Convert(string) (interface{}, error)
}

type fieldConverter struct {
	name string
	conv Converter
}

func newFieldConverter(*avro.Field) (fieldConverter, error) {
	// TODO:
	return fieldConverter{}, nil
}

type recordConverter struct {
	fieldConvs []fieldConverter
}

func newRecordConverter(recSch *avro.RecordSchema) (*recordConverter, error) {
	fields := recSch.Fields()
	convs := make([]fieldConverter, 0, len(fields))
	for _, f := range fields {
		fc, err := newFieldConverter(f)
		if err != nil {
			return nil, err
		}
		convs = append(convs, fc)
	}
	return &recordConverter{fieldConvs: convs}, nil
}

func (cv *recordConverter) Convert(src []string) (map[string]interface{}, error) {
	dst := map[string]interface{}{}
	for i, s := range src {
		if i >= len(cv.fieldConvs) {
			return nil, fmt.Errorf("no field converters provided at column #%d", i)
		}
		fc := cv.fieldConvs[i]
		v, err := fc.conv.Convert(s)
		if err != nil {
			return nil, err
		}
		dst[fc.name] = v
	}
	return nil, nil
}

func csv2avro(recSch *avro.RecordSchema, in io.Reader, out io.Writer) error {
	// generate value converters from avro.Schema
	cv, err := newRecordConverter(recSch)
	if err != nil {
		return err
	}
	w := avro.NewEncoderForSchema(recSch, out)
	r := csv.NewReader(in)
	// FIXME: configure CSV reader.
	for {
		// read a row from CSV/TSV.
		src, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		// compose an record.
		dst, err := cv.Convert(src)
		if err != nil {
			return err
		}
		if dst == nil {
			continue
		}
		// output an record as Avro.
		err = w.Encode(dst)
		if err != nil {
			return err
		}
	}
	return nil
}
