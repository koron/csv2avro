package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/hamba/avro"
)

type Converter interface {
	Convert(string) (interface{}, error)
}

type convertFunc func(string) (interface{}, error)

func (f convertFunc) Convert(s string) (interface{}, error) {
	return f(s)
}

type fieldConverter struct {
	name string
	conv Converter
}

func newFieldConverter(f *avro.Field) (fieldConverter, error) {
	switch f.Type().Type() {
	case avro.String:
		return fieldConverter{
			name: "string",
			conv: convertFunc(func(s string) (interface{}, error) {
				return s, nil
			}),
		}, nil

	case avro.Int:
		return fieldConverter{
			name: "int",
			conv: convertFunc(func(s string) (interface{}, error) {
				return strconv.ParseInt(s, 10, 32)
			}),
		}, nil

	case avro.Long:
		return fieldConverter{
			name: "long",
			conv: convertFunc(func(s string) (interface{}, error) {
				return strconv.ParseInt(s, 10, 64)
			}),
		}, nil

	case avro.Float:
		return fieldConverter{
			name: "float",
			conv: convertFunc(func(s string) (interface{}, error) {
				return strconv.ParseFloat(s, 32)
			}),
		}, nil

	case avro.Double:
		return fieldConverter{
			name: "double",
			conv: convertFunc(func(s string) (interface{}, error) {
				return strconv.ParseFloat(s, 64)
			}),
		}, nil

	case avro.Boolean:
		return fieldConverter{
			name: "bool",
			conv: convertFunc(func(s string) (interface{}, error) {
				return strconv.ParseBool(s)
			}),
		}, nil

	case avro.Null:
		return fieldConverter{
			name: "null",
			conv: convertFunc(func(s string) (interface{}, error) {
				return nil, nil
			}),
		}, nil

	default:
		return fieldConverter{}, fmt.Errorf("unsupported type: %s", f.Type().Type())
	//case avro.Record:
	//case avro.Error:
	//case avro.Ref:
	//case avro.Enum:
	//case avro.Array:
	//case avro.Map:
	//case avro.Union:
	//case avro.Fixed:
	//case avro.Bytes:
	}
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
