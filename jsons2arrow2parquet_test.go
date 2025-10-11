package jsons2arrow2parquet

import (
	"bytes"
	"context"
	"iter"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
)

func TestJsonsToParquet(t *testing.T) {
	// 1. Prepare input JSON
	var jsonStr string = `{"foo": 1, "bar": "hello"}
{"foo": 2, "bar": "world"}`
	var jsonRdr *strings.Reader = strings.NewReader(jsonStr)

	// 2. Define schema
	var schema *arrow.Schema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "foo", Type: arrow.PrimitiveTypes.Int64},
			{Name: "bar", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// 3. Convert JSON to Arrow records
	var aropts ArrayReadOptions = ArrayReadOptions{
		Schema: schema,
	}
	var recordsIter iter.Seq2[arrow.RecordBatch, error] = aropts.JsonsToIter(jsonRdr)

	// 4. Convert Arrow records to Parquet buffer
	var parquetBuf bytes.Buffer
	var pwopts ParquetWriteOpts = ParquetWriteOpts{
		Schema: schema,
	}

	var err error = pwopts.IterToWriter(recordsIter, &parquetBuf)
	assert.NoError(t, err)

	// 5. Read Parquet buffer and compare results
	var parquetRdr *bytes.Reader = bytes.NewReader(parquetBuf.Bytes())
	fileReader, err := file.NewParquetReader(parquetRdr)
	assert.NoError(t, err)

	pr, err := pqarrow.NewFileReader(fileReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	assert.NoError(t, err)

	tbl, err := pr.ReadTable(context.Background())
	assert.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, int64(2), tbl.NumRows())
	assert.Equal(t, int64(2), tbl.NumCols())

	// check column values
	var col1 *arrow.Column = tbl.Column(0)
	var int64chunked *arrow.Chunked = col1.Data()
	assert.Equal(t, 1, len(int64chunked.Chunks()))
	var int64arr *array.Int64 = int64chunked.Chunk(0).(*array.Int64)
	assert.Equal(t, []int64{1, 2}, int64arr.Int64Values())

	var col2 *arrow.Column = tbl.Column(1)
	var stringchunked *arrow.Chunked = col2.Data()
	assert.Equal(t, 1, len(stringchunked.Chunks()))
	var stringarr *array.String = stringchunked.Chunk(0).(*array.String)
	assert.Equal(t, "hello", stringarr.Value(0))
	assert.Equal(t, "world", stringarr.Value(1))
}
