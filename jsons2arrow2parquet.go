// Package jsons2arrow2parquet provides tools to convert a stream of JSON objects
// into the Parquet format using the Apache Arrow memory format as an
// intermediate representation.
package jsons2arrow2parquet

import (
	"bufio"
	"errors"
	"io"
	"iter"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	ja "github.com/takanoriyanagitani/go-jsons2arrow"
)

// ArrayReadOptions holds the configuration for reading JSON data into Arrow
// format.
type ArrayReadOptions struct {
	*arrow.Schema

	// Opts holds Arrow array construction options.
	Opts []array.Option
}

// WithChunkSize returns a new ArrayReadOptions with the specified chunk size.
func (o ArrayReadOptions) WithChunkSize(n int) ArrayReadOptions {
	return ArrayReadOptions{
		Schema: o.Schema,
		Opts:   append(o.Opts, array.WithChunk(n)),
	}
}

// JsonsToIter creates an iterator that yields Arrow RecordBatches from a JSON
// input stream.
func (o ArrayReadOptions) JsonsToIter(
	rdr io.Reader,
) iter.Seq2[arrow.RecordBatch, error] {
	jopts := ja.ReadOptions{
		Schema:  o.Schema,
		Options: o.Opts,
	}
	var jrdr ja.JSONReader = jopts.ToReader(rdr)
	return jrdr.ToIter()
}

// ParquetWriteOpts holds the configuration for writing Arrow data to Parquet
// format.
type ParquetWriteOpts struct {
	*arrow.Schema

	// Aopts holds options for the Arrow-to-Parquet writer.
	Aopts []pqarrow.WriterOption
	// Popts holds properties for the Parquet writer.
	Popts []parquet.WriterProperty
}

// WithAllocator returns new ParquetWriteOpts with the specified memory allocator.
func (o ParquetWriteOpts) WithAllocator(mem memory.Allocator) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  append(o.Aopts, pqarrow.WithAllocator(mem)),
		Popts:  o.Popts,
	}
}

// WithDeprecatedInt96Timestamps returns new ParquetWriteOpts that configures
// the handling of deprecated Int96 timestamps.
func (o ParquetWriteOpts) WithDeprecatedInt96Timestamps(enabled bool) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  append(o.Aopts, pqarrow.WithDeprecatedInt96Timestamps(enabled)),
		Popts:  o.Popts,
	}
}

// WithNoMapLogicalType returns new ParquetWriteOpts that disables the map
// logical type.
func (o ParquetWriteOpts) WithNoMapLogicalType() ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  append(o.Aopts, pqarrow.WithNoMapLogicalType()),
		Popts:  o.Popts,
	}
}

// WithStoreSchema returns new ParquetWriteOpts that configures the writer to
// store the Arrow schema in the Parquet file metadata.
func (o ParquetWriteOpts) WithStoreSchema() ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  append(o.Aopts, pqarrow.WithStoreSchema()),
		Popts:  o.Popts,
	}
}

// WithTruncatedTimestamps returns new ParquetWriteOpts that allows truncating
// timestamps.
func (o ParquetWriteOpts) WithTruncatedTimestamps(allow bool) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  append(o.Aopts, pqarrow.WithTruncatedTimestamps(allow)),
		Popts:  o.Popts,
	}
}

// WithAdaptiveBloomFilterEnabled returns new ParquetWriteOpts that enables or
// disables the adaptive bloom filter.
func (o ParquetWriteOpts) WithAdaptiveBloomFilterEnabled(
	enabled bool,
) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithAdaptiveBloomFilterEnabled(enabled)),
	}
}

// WithBloomFilterEnabled returns new ParquetWriteOpts that enables or disables
// bloom filters.
func (o ParquetWriteOpts) WithBloomFilterEnabled(enabled bool) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithBloomFilterEnabled(enabled)),
	}
}

// WithBloomFilterFPP returns new ParquetWriteOpts with the specified bloom
// filter false positive probability.
func (o ParquetWriteOpts) WithBloomFilterFPP(fpp float64) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithBloomFilterFPP(fpp)),
	}
}

// WithBloomFilterNDV returns new ParquetWriteOpts with the specified bloom
// filter number of distinct values.
func (o ParquetWriteOpts) WithBloomFilterNDV(ndv int64) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithBloomFilterNDV(ndv)),
	}
}

// WithCoerceTimestamps returns new ParquetWriteOpts that coerces timestamps to
// the specified time unit.
func (o ParquetWriteOpts) WithCoerceTimestamps(
	unit arrow.TimeUnit,
) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  append(o.Aopts, pqarrow.WithCoerceTimestamps(unit)),
		Popts:  o.Popts,
	}
}

// WithCompression returns new ParquetWriteOpts with the specified compression
// codec.
func (o ParquetWriteOpts) WithCompression(c compress.Compression) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithCompression(c)),
	}
}

// WithCompressionLevel returns new ParquetWriteOpts with the specified
// compression level.
func (o ParquetWriteOpts) WithCompressionLevel(level int) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithCompressionLevel(level)),
	}
}

// WithCreatedBy returns new ParquetWriteOpts with the specified "created by"
// metadata.
func (o ParquetWriteOpts) WithCreatedBy(creator string) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithCreatedBy(creator)),
	}
}

// WithDataPageSize returns new ParquetWriteOpts with the specified data page
// size.
func (o ParquetWriteOpts) WithDataPageSize(size int64) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithDataPageSize(size)),
	}
}

// WithDataPageVersion returns new ParquetWriteOpts with the specified data page
// version.
func (o ParquetWriteOpts) WithDataPageVersion(v parquet.DataPageVersion) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithDataPageVersion(v)),
	}
}

// WithDictionaryPageSizeLimit returns new ParquetWriteOpts with the specified
// dictionary page size limit.
func (o ParquetWriteOpts) WithDictionaryPageSizeLimit(size int64) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithDictionaryPageSizeLimit(size)),
	}
}

// WithMaxRowGroupLength returns new ParquetWriteOpts with the specified max row
// group length.
func (o ParquetWriteOpts) WithMaxRowGroupLength(size int64) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithMaxRowGroupLength(size)),
	}
}

// WithStats returns new ParquetWriteOpts that enables or disables statistics
// generation.
func (o ParquetWriteOpts) WithStats(enabled bool) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithStats(enabled)),
	}
}

// WithStoreDecimalAsInteger returns new ParquetWriteOpts that configures storing
// decimals as integers.
func (o ParquetWriteOpts) WithStoreDecimalAsInteger(enabled bool) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithStoreDecimalAsInteger(enabled)),
	}
}

// WithVersion returns new ParquetWriteOpts with the specified Parquet format
// version.
func (o ParquetWriteOpts) WithVersion(version parquet.Version) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithVersion(version)),
	}
}

// WithBatchSize returns new ParquetWriteOpts with the specified batch size.
func (o ParquetWriteOpts) WithBatchSize(size int64) ParquetWriteOpts {
	return ParquetWriteOpts{
		Schema: o.Schema,
		Aopts:  o.Aopts,
		Popts:  append(o.Popts, parquet.WithBatchSize(size)),
	}
}

// ToWriter creates a new ParquetWriter that writes to the given io.Writer.
func (o ParquetWriteOpts) ToWriter(wtr io.Writer) (ParquetWriter, error) {
	fwtr, err := pqarrow.NewFileWriter(
		o.Schema,
		wtr,
		parquet.NewWriterProperties(o.Popts...),
		pqarrow.NewArrowWriterProperties(o.Aopts...),
	)
	return ParquetWriter{FileWriter: fwtr}, err
}

// IterToWriter writes all RecordBatches from an iterator to the given
// io.Writer in Parquet format.
func (o ParquetWriteOpts) IterToWriter(
	biter iter.Seq2[arrow.RecordBatch, error],
	w io.Writer,
) error {
	pwtr, e := o.ToWriter(w)
	if nil != e {
		return e
	}
	return errors.Join(pwtr.WriteAll(biter), pwtr.Close())
}

// IterToStdout writes all RecordBatches from an iterator to standard output in
// Parquet format.
func (o ParquetWriteOpts) IterToStdout(
	biter iter.Seq2[arrow.RecordBatch, error],
) error {
	var bw *bufio.Writer = bufio.NewWriter(os.Stdout)
	return errors.Join(o.IterToWriter(biter, bw), bw.Flush())
}

// StdinToJsonsToIterToParquetToStdout reads JSONs from stdin, converts them to
// Arrow RecordBatches, and writes them to stdout in Parquet format.
func (o ParquetWriteOpts) StdinToJsonsToIterToParquetToStdout(
	opts ArrayReadOptions,
) error {
	var ibat iter.Seq2[arrow.RecordBatch, error] = opts.JsonsToIter(
		bufio.NewReader(os.Stdin),
	)
	return o.IterToStdout(ibat)
}

// ParquetWriter is a wrapper around pqarrow.FileWriter to provide convenience
// methods.
type ParquetWriter struct{ *pqarrow.FileWriter }

// Close closes the underlying Parquet file writer.
func (w ParquetWriter) Close() error { return w.FileWriter.Close() }

// Write writes a single RecordBatch to the Parquet file.
func (w ParquetWriter) Write(b arrow.RecordBatch) error {
	return w.FileWriter.Write(b)
}

// WriteBuffered writes a single RecordBatch to the Parquet file writer's buffer.
func (w ParquetWriter) WriteBuffered(b arrow.RecordBatch) error {
	return w.FileWriter.WriteBuffered(b)
}

// WriteAll writes all RecordBatches from an iterator to the Parquet file.
func (w ParquetWriter) WriteAll(b iter.Seq2[arrow.RecordBatch, error]) error {
	for bat, e := range b {
		if nil != e {
			return e
		}

		we := w.Write(bat)
		if nil != we {
			return we
		}
	}
	return nil
}
