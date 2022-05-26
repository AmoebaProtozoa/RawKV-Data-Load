package run

import (
	"context"
	"math/rand"
	"strconv"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/zap"
)

const keyPrefix = "TestEntry"

type run struct {
	cfg *Config
	ctx context.Context
	cli *rawkv.Client
}

func NewRun(ctx context.Context, cfg *Config) (*run, error) {
	cli, err := rawkv.NewClientV2(ctx, []string{cfg.PdAddr}, config.Security{})
	if err != nil {
		return nil, err
	}
	return &run{
		ctx: ctx,
		cli: cli,
		cfg: cfg,
	}, nil
}

func (r *run) CleanUp() {
	err := r.cli.Close()
	if err != nil {
		log.Error("[Run] Error during clean up", zap.Error(err))
	}
}

// batchPutOne insert 1 batch of records into TiKV
func (r *run) batchPutOne(startIndex int) error {
	batchSize := r.cfg.BatchSize
	recordSize := r.cfg.RecordSize
	keys := make([][]byte, 0, batchSize)
	vals := make([][]byte, 0, batchSize)
	for i := startIndex; i < startIndex+batchSize; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		val := make([]byte, recordSize)
		rand.Read(val)
		keys = append(keys, key)
		vals = append(vals, val)
	}
	return r.cli.BatchPut(r.ctx, keys, vals)
}

// putOne insert single record into TiKV
func (r *run) putOne(index int) error {
	recordSize := r.cfg.RecordSize
	key := []byte(keyPrefix + strconv.Itoa(index))
	val := make([]byte, recordSize)
	rand.Read(val)
	return r.cli.Put(r.ctx, key, val)
}

func (r *run) deleteOne(index int) error {
	key := []byte(keyPrefix + strconv.Itoa(index))
	return r.cli.Delete(r.ctx, key)
}

// BatchPut insert one thread's of data into TiKV
func (r *run) BatchPut(threadIndex int) error {
	startIndex := threadIndex * r.cfg.RecordPerThread
	endIndex := startIndex + r.cfg.RecordPerThread
	for i := startIndex; i < endIndex; i += r.cfg.BatchSize {
		err := r.batchPutOne(i)
		if err != nil {
			return err
		}
	}
	log.Info("[Run] Thread finished batch put",
		zap.Int("Thread Index", threadIndex),
		zap.Int("Start-Key Index", startIndex),
		zap.Int("End-Key Index", endIndex),
	)
	return nil
}

func (r *run) Delete(threadIndex int) error {
	startIndex := threadIndex * r.cfg.RecordPerThread
	endIndex := startIndex + r.cfg.RecordPerThread
	for i := startIndex; i < endIndex; i++ {
		err := r.deleteOne(i)
		if err != nil {
			return err
		}
	}
	log.Info("[Run] Thread finished deletion",
		zap.Int("Thread Index", threadIndex),
		zap.Int("Start-Key Index", startIndex),
		zap.Int("End-Key Index", endIndex),
	)
	return nil
}
