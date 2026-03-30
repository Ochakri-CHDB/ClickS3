package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

const (
	chKeyLen  = 32
	alphabet  = "abcdefghijklmnopqrstuvwxyz"
)

// GenerateKey produces a ClickHouse-style S3 key:
// {prefix}{32 random lowercase chars}
// This matches SharedMergeTree naming to avoid hash-ring hotspots on MinIO.
func GenerateKey(prefix string) string {
	b := make([]byte, chKeyLen)
	for i := range b {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		b[i] = alphabet[idx.Int64()]
	}
	return prefix + string(b)
}

// PartType represents ClickHouse data part storage format
type PartType int

const (
	CompactPart PartType = iota
	WidePart
)

// PartFile represents a single S3 object within a ClickHouse data part
type PartFile struct {
	Key      string
	Size     int64
	FileName string // logical name (e.g. "data.bin", "primary.idx")
}

// GenerateCompactPart generates the set of S3 objects for a ClickHouse Compact part.
// A compact part stores all columns in a single data.bin file.
func GenerateCompactPart(prefix string, dataSize int64) []PartFile {
	files := []PartFile{
		{Key: GenerateKey(prefix), Size: dataSize, FileName: "data.bin"},
		{Key: GenerateKey(prefix), Size: clamp(dataSize/100, 14*1024, 500*1024), FileName: "data.mrk3"},
		{Key: GenerateKey(prefix), Size: randBetween(16*1024, 256*1024), FileName: "primary.idx"},
		{Key: GenerateKey(prefix), Size: 288, FileName: "columns.txt"},
		{Key: GenerateKey(prefix), Size: 96, FileName: "checksums.txt"},
		{Key: GenerateKey(prefix), Size: 4, FileName: "count.txt"},
		{Key: GenerateKey(prefix), Size: 5, FileName: "default_compression_codec.txt"},
		{Key: GenerateKey(prefix), Size: 4, FileName: "partition.dat"},
		{Key: GenerateKey(prefix), Size: randBetween(8, 32), FileName: "minmax_col.idx"},
	}
	return files
}

// GenerateWidePart generates the set of S3 objects for a ClickHouse Wide part.
// A wide part stores each column in its own .bin and .mrk3 file.
func GenerateWidePart(prefix string, numColumns int, totalDataSize int64) []PartFile {
	colDataSize := totalDataSize / int64(numColumns)
	markSize := clamp(colDataSize/100, 14*1024, 500*1024)

	var files []PartFile
	for i := 0; i < numColumns; i++ {
		files = append(files,
			PartFile{Key: GenerateKey(prefix), Size: colDataSize, FileName: fmt.Sprintf("col%d.bin", i)},
			PartFile{Key: GenerateKey(prefix), Size: markSize, FileName: fmt.Sprintf("col%d.mrk3", i)},
		)
	}

	// Metadata files
	files = append(files,
		PartFile{Key: GenerateKey(prefix), Size: randBetween(16*1024, 256*1024), FileName: "primary.idx"},
		PartFile{Key: GenerateKey(prefix), Size: 288, FileName: "columns.txt"},
		PartFile{Key: GenerateKey(prefix), Size: 96, FileName: "checksums.txt"},
		PartFile{Key: GenerateKey(prefix), Size: 4, FileName: "count.txt"},
		PartFile{Key: GenerateKey(prefix), Size: 5, FileName: "default_compression_codec.txt"},
		PartFile{Key: GenerateKey(prefix), Size: 4, FileName: "partition.dat"},
		PartFile{Key: GenerateKey(prefix), Size: randBetween(8, 32), FileName: "minmax_col.idx"},
	)
	return files
}

func randBetween(min, max int64) int64 {
	if min >= max {
		return min
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(max-min))
	return min + n.Int64()
}

func clamp(val, min, max int64) int64 {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}
