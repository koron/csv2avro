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

func newConverter(sch avro.Schema) (Converter, error) {
	switch sch.Type() {
	case avro.String:
		return convertFunc(func(s string) (interface{}, error) {
			return s, nil
		}), nil

	case avro.Int:
		return convertFunc(func(s string) (interface{}, error) {
			v, err := strconv.ParseInt(s, 10, 32)
			return int(v), err
		}), nil

	case avro.Long:
		return convertFunc(func(s string) (interface{}, error) {
			return strconv.ParseInt(s, 10, 64)
		}), nil

	case avro.Float:
		return convertFunc(func(s string) (interface{}, error) {
			v, err := strconv.ParseFloat(s, 32)
			return float32(v), err
		}), nil

	case avro.Double:
		return convertFunc(func(s string) (interface{}, error) {
			return strconv.ParseFloat(s, 64)
		}), nil

	case avro.Boolean:
		return convertFunc(func(s string) (interface{}, error) {
			return strconv.ParseBool(s)
		}), nil

	case avro.Null:
		return convertFunc(func(s string) (interface{}, error) {
			return nil, nil
		}), nil

	default:
		return nil, fmt.Errorf("unsupported type: %s", sch.Type())
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
		c, err := newConverter(f.Type())
		if err != nil {
			return nil, err
		}
		convs = append(convs, fieldConverter{
			name: f.Name(),
			conv: c,
		})
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
	return dst, nil
}

func csv2avro(recSch *avro.RecordSchema, r *csv.Reader, out io.Writer) error {
	// generate value converters from avro.Schema
	cv, err := newRecordConverter(recSch)
	if err != nil {
		return err
	}
	w := avro.NewEncoderForSchema(recSch, out)
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
			return fmt.Errorf("failed to convert values: %w", err)
		}
		if dst == nil {
			continue
		}
		// output an record as Avro.
		err = w.Encode(dst)
		if err != nil {
			return fmt.Errorf("failed to encode: %w", err)
		}
	}
	return nil
}
