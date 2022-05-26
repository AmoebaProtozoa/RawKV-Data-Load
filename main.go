package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

var (
	pdAddr         = "127.0.0.1:2379"
	targetSizeGB   = 4   // total size of record in GB
	thread         = 40  // number of thread executing load and delete
	bytePerRecord  = 400 // size per record
	recordPerBatch = 100 // number of record per batch if using batchLoad
	wg             = sync.WaitGroup{}
)

func main() {
	totalRecord := targetSizeGB * 1000000000 / bytePerRecord
	recordPerThread := totalRecord / thread

	// Load Data
	for i := 0; i < thread; i++ {
		wg.Add(1)
		//go runLoad(recordPerThread, i*recordPerThread)
		go runBatchLoad(recordPerThread, i*recordPerThread)
	}
	wg.Wait()

	// Delete Data
	fmt.Println("LoadComplete")
	for i := 0; i < thread; i++ {
		wg.Add(1)
		go runDelete(recordPerThread, i*recordPerThread)
	}
	wg.Wait()
	fmt.Println("DeletionComplete")
}

func connect(pdAddr string) (*rawkv.Client, error) {
	return rawkv.NewClientV2(context.Background(), []string{pdAddr}, config.Security{})
}

func close(cli *rawkv.Client) {
	err := cli.Close()
	if err != nil {
		panic(err)
	}
}

func put(cli *rawkv.Client, key, val []byte) error {
	return cli.Put(context.Background(), key, val)
}

func batchPut(cli *rawkv.Client, keys, vals [][]byte) error {
	return cli.BatchPut(context.Background(), keys, vals)
}

func get(cli *rawkv.Client, key []byte) ([]byte, error) {
	return cli.Get(context.Background(), key)
}

func delete(cli *rawkv.Client, key []byte) error {
	return cli.Delete(context.TODO(), key)
}

func runBatchLoad(recordCount int, startIndex int) {
	defer wg.Done()
	cli, err := connect(pdAddr)
	if err != nil {
		panic(err)
	}
	defer close(cli)

	for i := startIndex; i < startIndex+recordCount; i += recordPerBatch {
		keys := make([][]byte, recordPerBatch)
		vals := make([][]byte, recordPerBatch)
		for j := i; j < i+recordPerBatch; j++ {
			key := []byte("GCTestEntry" + strconv.Itoa(i))
			val := make([]byte, bytePerRecord)
			rand.Read(val)
			keys = append(keys, key)
			vals = append(vals, val)
		}
		err = batchPut(cli, keys, vals)
		if err != nil {
			panic(err)
		}
		fmt.Printf("putted batch entry %d to %d\n", i, startIndex+recordCount)
	}
}

func runLoad(recordCount int, startIndex int) {
	defer wg.Done()
	cli, err := connect(pdAddr)
	if err != nil {
		panic(err)
	}
	defer close(cli)

	for i := startIndex; i < startIndex+recordCount; i++ {
		key := []byte("GCTestEntry" + strconv.Itoa(i))
		value := make([]byte, bytePerRecord)
		rand.Read(value)
		err = put(cli, key, value)
		if err != nil {
			panic(err)
		}
		fmt.Printf("putting entry %d\n", i)
	}
}

func runDelete(recordCount int, startIndex int) {
	defer wg.Done()
	cli, err := connect(pdAddr)
	if err != nil {
		panic(err)
	}
	defer close(cli)

	for i := startIndex; i < startIndex+recordCount; i++ {
		key := []byte("GCTestEntry" + strconv.Itoa(i))
		err = delete(cli, key)
		if err != nil {
			panic(err)
		}
		fmt.Printf("deleted entry %d\n", i)
	}
}
