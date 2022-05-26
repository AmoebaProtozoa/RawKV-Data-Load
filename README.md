# RawKV-Data-Load
A small tool used to insert/delete APIv2 RawKV data into TiKV

## Usage:
```text
-help
show usage

-pd string
PD Address with port (default "127.0.0.1:2379")

-size float
size of data to write per load in GB (default 1)
please make sure its greater than 0.1 (100MB)

-record-size int
size of a single record in Bytes (default 400)

-thread int
number of thread executing load and delete (default 40)

-batch-size int
number of record per batch in batch load (default 100)

-version-count int
total number of versions to write for each key (default 1)
```
