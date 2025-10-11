package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	aa "github.com/apache/arrow-go/v18/arrow/avro"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	ha "github.com/hamba/avro/v2"
	j2p "github.com/takanoriyanagitani/go-jsons2arrow2parquet"
)

var (
	// ErrUnknownCompression is returned when an unsupported compression codec is
	// specified.
	ErrUnknownCompression = errors.New("unknown compression")

	// ErrUnknownParquetVersion is returned when an unsupported Parquet version is
	// specified.
	ErrUnknownParquetVersion = errors.New("unknown parquet version")

	// ErrAvscFlagRequired is returned when the mandatory -avsc flag is not provided.
	ErrAvscFlagRequired = errors.New("-avsc flag is required")
)

// getCompression parses the string representation of a compression codec and
// returns the corresponding compress.Compression type.
func getCompression(codec string) (compress.Compression, error) {
	codec = strings.ToUpper(strings.TrimSpace(codec))
	var c compress.Compression
	err := (&c).UnmarshalText([]byte(codec))
	if err != nil {
		return compress.Compression(0), fmt.Errorf("%w: %s", ErrUnknownCompression, codec)
	}
	return c, nil
}

// getParquetVersion parses the string representation of a Parquet version and
// returns the corresponding parquet.Version type.
func getParquetVersion(version string) (parquet.Version, error) {
	version = strings.ToLower(strings.TrimSpace(version))
	switch version {
	case "1.0":
		return parquet.V1_0, nil
	case "2.4":
		return parquet.V2_4, nil
	case "2.6":
		return parquet.V2_6, nil
	default:
		return parquet.Version(0), fmt.Errorf("%w: %s", ErrUnknownParquetVersion, version)
	}
}

// getSchemaFromAvsc reads an Avro schema file (.avsc), parses it, and converts
// it to an Arrow schema.
func getSchemaFromAvsc(path string) (*arrow.Schema, error) {
	schemaBytes, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("unable to read avro schema file: %w", err)
	}

	haSchema, err := ha.Parse(string(schemaBytes))
	if err != nil {
		return nil, fmt.Errorf("unable to parse avro schema: %w", err)
	}

	arrowSchema, err := aa.ArrowSchemaFromAvro(haSchema)
	if err != nil {
		return nil, fmt.Errorf("unable to convert avro to arrow schema: %w", err)
	}

	return arrowSchema, nil
}

// run is the main logic for the CLI application.
func run() error {
	var compressionStr string
	var parquetVersionStr string
	var statsEnabled bool
	var avscFile string
	var chunkSize int

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -avsc <schema.avsc> [options]\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Converts a stream of JSON objects from stdin to Parquet format on stdout.")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		flag.PrintDefaults()
	}

	flag.StringVar(&compressionStr, "compression", "snappy", "Compression codec (none, snappy, gzip, brotli, zstd)")
	flag.StringVar(&parquetVersionStr, "parquet-version", "2.6", "Parquet version (1.0, 2.4, 2.6)")
	flag.BoolVar(&statsEnabled, "stats", true, "Enable writing statistics")
	flag.StringVar(&avscFile, "avsc", "", "Path to Avro schema file (.avsc) (required)")
	flag.IntVar(&chunkSize, "chunk-size", 1024, "Chunk size for Arrow RecordBatches") //nolint:mnd
	flag.Parse()

	if avscFile == "" {
		flag.Usage()
		return ErrAvscFlagRequired
	}

	comp, err := getCompression(compressionStr)
	if err != nil {
		return err
	}

	pver, err := getParquetVersion(parquetVersionStr)
	if err != nil {
		return err
	}

	schema, err := getSchemaFromAvsc(avscFile)
	if err != nil {
		return err
	}

	stdinReader := bufio.NewReader(os.Stdin)

	readOpts := j2p.ArrayReadOptions{
		Schema: schema,
		Opts:   nil,
	}.WithChunkSize(chunkSize)

	writeOpts := j2p.ParquetWriteOpts{
		Schema: schema,
		Aopts:  nil,
		Popts:  nil,
	}.WithCompression(comp).WithVersion(pver).WithStats(statsEnabled)

	iter := readOpts.JsonsToIter(stdinReader)
	return writeOpts.IterToStdout(iter)
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}
