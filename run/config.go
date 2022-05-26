package run

import (
	"errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Config struct {
	PdAddr          string
	DataSize        int
	Thread          int
	RecordSize      int
	BatchSize       int
	RecordCount     int
	RecordPerThread int
}

func NewConfig(pdAddr string, DataSizeGB float64, thread, recordSize, batchSize int) (*Config, error) {
	DataSizeByte := int(DataSizeGB * 1e9)
	recordCount := DataSizeByte / recordSize
	recordPerThread := recordCount / thread

	if DataSizeByte < 100000000 {
		return nil, errors.New("data size too small, please make sure it's greater than 100MB")
	}

	log.Info("[config] Generating new run config",
		zap.String("pd-address", pdAddr),
		zap.Int("data-size-byte", DataSizeByte),
		zap.Int("thread-count", thread),
		zap.Int("record-size-byte", recordSize),
		zap.Int("record-per-batch", batchSize),
		zap.Int("total-record", recordCount),
		zap.Int("record-per-thread", recordPerThread),
	)
	return &Config{
		PdAddr:          pdAddr,
		DataSize:        DataSizeByte,
		Thread:          thread,
		RecordSize:      recordSize,
		BatchSize:       batchSize,
		RecordCount:     recordCount,
		RecordPerThread: recordPerThread,
	}, nil
}
