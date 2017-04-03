package main

import (
	"encoding/binary"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/akmistry/go-nbd/client"
	"github.com/tecbot/gorocksdb"
)

type RocksBlockDevice struct {
	db *gorocksdb.DB

	iopsLimit *client.Limiter
	bwLimit   *client.Limiter
	size      uint64
}

const (
	blockSize = 4096
	diskSize  = 64 * 1024 * 1024 * 1024

	megaByte = 1024 * 1024
)

var (
	defaultReadOptions  = gorocksdb.NewDefaultReadOptions()
	defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()

	dev           = flag.String("device", "/dev/nbd0", "Path to /dev/nbdX device.")
	maxIops       = flag.Int("max-iops", 100, "Maximum IO/s")
	maxThroughput = flag.Int("max-throughput", 10, "Maximum throughput, in MB/s")
)

func (d *RocksBlockDevice) Readonly() bool {
	return false
}

func (d *RocksBlockDevice) Size() uint64 {
	return d.size
}

func (d *RocksBlockDevice) BlockSize() uint32 {
	return blockSize
}

func (d *RocksBlockDevice) offsetToKey(off int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(off))
	return buf
}

func (d *RocksBlockDevice) limit(size int) {
	iopsDelay := d.iopsLimit.Add(1)
	bwDelay := d.bwLimit.Add(uint64(size))
	if bwDelay > iopsDelay {
		time.Sleep(bwDelay)
	} else {
		time.Sleep(iopsDelay)
	}
}

func (d *RocksBlockDevice) ReadAt(p []byte, off int64) (n int, err error) {
	d.limit(len(p))

	for len(p) > 0 {
		blockOff := int(off % blockSize)
		blockLen := len(p)
		if (blockLen + blockOff) > blockSize {
			blockLen = blockSize - blockOff
		}
		block := off - int64(blockOff)

		key := d.offsetToKey(block)
		buf, _ := d.db.GetBytes(defaultReadOptions, key)
		if buf != nil {
			copy(p, buf[blockOff:])
		} else {
			for i, _ := range p[:blockLen] {
				p[i] = 0
			}
		}

		off += int64(blockLen)
		n += blockLen
		p = p[blockLen:]
	}
	return n, nil
}

func (d *RocksBlockDevice) WriteAt(p []byte, off int64) (n int, err error) {
	d.limit(len(p))

	if off%int64(d.BlockSize()) != 0 {
		log.Panicln("Invalid offset", off, "length", len(p))
	} else if uint32(len(p))%d.BlockSize() != 0 {
		log.Panicln("Invalid write length", len(p))
	}

	for len(p) > 0 {
		key := d.offsetToKey(off)
		err := d.db.Put(defaultWriteOptions, key, p[:int(d.BlockSize())])
		if err != nil {
			return n, err
		}

		off += int64(d.BlockSize())
		n += int(d.BlockSize())
		p = p[int(d.BlockSize()):]
	}
	return n, nil
}

func (d *RocksBlockDevice) Close() error {
	d.db.Close()
	return nil
}

func (d *RocksBlockDevice) Flush() error {
	return d.db.Flush(gorocksdb.NewDefaultFlushOptions())
}

func (d *RocksBlockDevice) Trim(off uint64, length uint32) error {
	for length > 0 {
		blockOff := int(off % blockSize)
		if blockOff == 0 && length >= blockSize {
			key := d.offsetToKey(int64(off))
			err := d.db.Delete(defaultWriteOptions, key)
			if err != nil {
				return err
			}
		}
		blockLen := blockSize - blockOff
		off += uint64(blockLen)
		length -= uint32(blockLen)
	}
	return nil
}

func NewRocksBlockDevice(name string, size uint64) *RocksBlockDevice {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(gorocksdb.NoCompression)
	opts.SetAllowMmapWrites(false)
	opts.IncreaseParallelism(runtime.NumCPU())

	filter := gorocksdb.NewBloomFilter(10)
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetFilterPolicy(filter)
	bbto.SetBlockSize(128 * 1024)
	cache := gorocksdb.NewLRUCache(64 * 1024 * 1024)
	bbto.SetBlockCache(cache)
	opts.SetBlockBasedTableFactory(bbto)

	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		log.Panicln(err)
	}
	iops := client.NewLimiter(uint64(*maxIops), time.Second, 0.1)
	bw := client.NewLimiter(uint64(*maxThroughput*megaByte), time.Second, 0.1)
	return &RocksBlockDevice{db: db, iopsLimit: iops, bwLimit: bw, size: size}
}

func main() {
	flag.Parse()

	rocksDev := NewRocksBlockDevice("/pub/amistry/rocks.block", diskSize)
	nbd, err := client.NewServer(*dev, rocksDev)
	if err != nil {
		log.Panicln(err)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		nbd.Disconnect()
	}()

	err = nbd.Run()
	if err != nil {
		log.Println("Shutdown error:", err)
	}
	rocksDev.db.Close()
}
