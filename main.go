package main

import (
	"context"
	"flag"
	"os"
	"sync"

	"github.com/AmoebaProtozoa/RawKV-Data-Load/run"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// flag names
const (
	nmPDAddr       = "pd"
	nmDataSize     = "size"
	nmThread       = "thread"
	nmRecordSize   = "record-size"
	nmBatchSize    = "batch-size"
	nmVersionCount = "version-count"
)

var (
	pdAddr       = flag.String(nmPDAddr, "127.0.0.1:2379", "PD Address with port")
	dataSize     = flag.Float64(nmDataSize, 1, "size of data to write per load in GB")
	thread       = flag.Int(nmThread, 40, "number of thread executing load and delete")
	recordSize   = flag.Int(nmRecordSize, 400, "size of a single record in Bytes")
	batchSize    = flag.Int(nmBatchSize, 100, "number of record per batch in batch load")
	versionCount = flag.Int(nmVersionCount, 1, "total number of versions to write for each key")
)

func main() {
	help := flag.Bool("help", false, "show usage")
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(0)
	}
	flag.Parse()

	cfg, err := run.NewConfig(*pdAddr, *dataSize, *thread, *recordSize, *batchSize)
	if err != nil {
		log.Error("[config] failed to create config", zap.Error(err))
		os.Exit(1)
	}
	for i := 0; i < *versionCount; i++ {
		runBatchLoad(cfg)
	}
	runDelete(cfg)
}

func runBatchLoad(cfg *run.Config) {
	var wg = sync.WaitGroup{}

	for i := 0; i < cfg.Thread; i++ {
		wg.Add(1)
		go func(threadIndex int) {
			defer wg.Done()
			runner, err := run.NewRun(context.Background(), cfg)
			if err != nil {
				log.Error("[Workload] Failed to create new run", zap.Error(err))
				return
			}
			defer runner.CleanUp()
			err = runner.BatchPut(threadIndex)
			if err != nil {
				log.Error("[Workload] Error during batch load", zap.Error(err))
			}
		}(i)
	}

	wg.Wait()
	return
}

func runDelete(cfg *run.Config) {
	var wg = sync.WaitGroup{}

	for i := 0; i < cfg.Thread; i++ {
		wg.Add(1)
		go func(threadIndex int) {
			defer wg.Done()
			runner, err := run.NewRun(context.Background(), cfg)
			if err != nil {
				log.Error("[Workload] Failed to create new run", zap.Error(err))
				return
			}
			defer runner.CleanUp()
			err = runner.Delete(threadIndex)
			if err != nil {
				log.Error("[Workload] Error during delete", zap.Error(err))
			}
		}(i)
	}

	wg.Wait()
	return
}
