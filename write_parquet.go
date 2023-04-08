
package main

import (
	"sort"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/xitongsys/parquet-go/parquet"
)

type ParquetChecksumRecord struct {
	Path string `parquet:"name=path, type=UTF8, encoding=DELTA_BYTE_ARRAY"`
	// We need to use string type here, as otherwise Go Parquet library will 
	Checksum string `parquet:"name=checksum, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY"`
}

func writeParquet(filename string, checksums map[string][]byte) error {
	var err error
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		return err
	}

	pw, err := writer.NewParquetWriter(fw, new(ParquetChecksumRecord), 4)
	if err != nil {
		return err
	}

	parquetRecords := make([]ParquetChecksumRecord, 0, len(checksums))
	for k, v := range checksums {
		parquetRecords = append(parquetRecords, ParquetChecksumRecord {
			Path: k,
			Checksum: string(v),
		})
	}
	sort.Slice(parquetRecords, func(i, j int) bool {
		return strings.Compare(parquetRecords[i].Path, parquetRecords[j].Path) < 0
	})

	pw.RowGroupSize = 1 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	for _, record := range parquetRecords {
		if err = pw.Write(record); err != nil {
			return err
		}
	}
	if err = pw.WriteStop(); err != nil {
		return err
	}
	fw.Close()
	return nil
}