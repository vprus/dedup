package main

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/apache/arrow/go/v12/parquet/schema"
	"os"
)

// Read manifest file and return a map from relative path to base64-encoded checksum
func readParquet(filename string) map[string]string {
	reader, err := file.OpenParquetFile(filename, true)
	if err != nil {
		panic(nil)
	}
	cr0, _ := reader.RowGroup(0).Column(0)
	scr0 := cr0.(*file.ByteArrayColumnChunkReader)
	cr1, _ := reader.RowGroup(0).Column(1)
	scr1 := cr1.(*file.ByteArrayColumnChunkReader)

	total := int64(0)

	paths := make([]parquet.ByteArray, 100)
	checksums := make([]parquet.ByteArray, 100)
	def1 := make([]int16, 100)
	rpt1 := make([]int16, 100)
	def2 := make([]int16, 100)
	rpt2 := make([]int16, 100)

	result := make(map[string]string)
	for total < reader.NumRows() {
		here, _, err := scr0.ReadBatch(100, paths, def1, rpt1)
		if err != nil {
			panic(err)
		}
		_, _, err = scr1.ReadBatch(100, checksums, def2, rpt2)
		if err != nil {
			panic(err)
		}
		for i := int64(0); i < here; i++ {
			result[string(paths[i])] = string(checksums[i])
		}
		total = total + here
	}
	fmt.Printf("Read %v rows\n", total)
	return result
}

// White all checksum entries from 'checksums' stream into a Parquet
// file.
func writeParquet(filename string, checksums <-chan PathChecksum) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "path", Type: arrow.BinaryTypes.String},
			{Name: "checksum", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	var records []arrow.Record

	var pathColumnValues []string
	var checksumColumnValues []string

	createRecord := func() {
		b.Field(0).(*array.StringBuilder).AppendValues(pathColumnValues, nil)
		b.Field(1).(*array.StringBuilder).AppendValues(checksumColumnValues, nil)
		records = append(records, b.NewRecord())
		pathColumnValues = nil
		checksumColumnValues = nil
	}

	for c := range checksums {
		pathColumnValues = append(pathColumnValues, c.relativePath)
		checksumColumnValues = append(checksumColumnValues, c.checksum)
		if len(pathColumnValues) == 1000 {
			createRecord()
		}
	}
	if len(pathColumnValues) > 0 {
		createRecord()
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	println("Writing to parquet:", tbl.NumRows())

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err.(any))
	}
	defer f.Close()
	pqarrow.WriteTable(tbl, f, 100000, nil, pqarrow.DefaultWriterProps())
}

// Write all entries from checksums map to a Parquet file, using Arrow interfaces.
// This function is not used by the rest of code, and serves to illustrate how it can
// be done if we don't care about streaming and can just store entire map
func writeParquetViaArrow(filename string, checksums map[string]string) error {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "path", Type: arrow.BinaryTypes.String},
			{Name: "checksum", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	var records []arrow.Record
	for k, v := range checksums {
		b.Field(0).(*array.StringBuilder).AppendValues([]string{k}, nil)
		b.Field(1).(*array.StringBuilder).AppendValues([]string{v}, nil)
		records = append(records, b.NewRecord())
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err.(any))
	}
	defer f.Close()
	pqarrow.WriteTable(tbl, f, 100000, nil, pqarrow.DefaultWriterProps())

	return nil
}

// Write all entries from checksums map to a Parquet file, using low-leve interfaces.
// Just like writeParquetArrow, his function is not used by the rest of code, and
// serve to illustrate just how complicated the low-level interfaces are
func writeParquetLowLevel(filename string, checksums map[string]string) error {

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err.(any))
	}

	fields := make([]schema.Node, 2)
	fields[0], _ = schema.NewPrimitiveNodeLogical("path", parquet.Repetitions.Optional, &schema.StringLogicalType{}, parquet.Types.ByteArray, -1, -1)
	fields[1], _ = schema.NewPrimitiveNodeLogical("checksum", parquet.Repetitions.Optional, &schema.StringLogicalType{}, parquet.Types.ByteArray, -1, -1)
	node, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	schema := schema.NewSchema(node)

	opts := make([]parquet.WriterProperty, 0)
	for i := 0; i < 1; i++ {
		opts = append(opts, parquet.WithCompressionFor(schema.Column(i).Name(), compress.Codecs.Snappy))
	}

	props := parquet.NewWriterProperties(opts...)

	print(schema.Root().NumFields())
	writer := file.NewParquetWriter(f, schema.Root(), file.WithWriterProps(props))

	var values, values2 []parquet.ByteArray
	var def []int16
	for path, checksum := range checksums {
		values = append(values, parquet.ByteArray(path))
		values2 = append(values2, parquet.ByteArray(checksum))
		def = append(def, 1)
	}

	rgw := writer.AppendRowGroup()

	cw, _ := rgw.NextColumn()
	scw := cw.(*file.ByteArrayColumnChunkWriter)
	scw.WriteBatch(values, def, nil)
	cw.Close()

	cw2, _ := rgw.NextColumn()
	scw2 := cw2.(*file.ByteArrayColumnChunkWriter)
	scw2.WriteBatch(values2, def, nil)
	cw2.Close()

	rgw.Close()
	writer.Close()

	f.Close()
	return nil
}
