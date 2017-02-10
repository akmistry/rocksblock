package main

import (
	"encoding/binary"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/akmistry/go-nbd/client"
	"github.com/tecbot/gorocksdb"
)

type RocksBlockDevice struct {
	db   *gorocksdb.DB
	size uint64
}

const (
	blockSize = 512
)

var (
	defaultReadOptions  = gorocksdb.NewDefaultReadOptions()
	defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()

	dev = flag.String("device", "/dev/nbd0", "Path to /deb/nbdX device.")
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

func (d *RocksBlockDevice) ReadAt(p []byte, off int64) (n int, err error) {
	if off%int64(d.BlockSize()) != 0 {
		log.Panicln("Invalid offset", off, "length", len(p))
	} else if uint32(len(p))%d.BlockSize() != 0 {
		log.Panicln("Invalid read length", len(p))
	}

	for len(p) > 0 {
		key := d.offsetToKey(off)
		buf, _ := d.db.GetBytes(defaultReadOptions, key)
		if buf != nil {
			copy(p, buf)
		} else {
			for i, _ := range p[:int(d.BlockSize())] {
				p[i] = 0
			}
		}

		off += int64(d.BlockSize())
		n += int(d.BlockSize())
		p = p[int(d.BlockSize()):]
	}
	return n, nil
}

func (d *RocksBlockDevice) WriteAt(p []byte, off int64) (n int, err error) {
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

func NewRocksBlockDevice(name string, size uint64) *RocksBlockDevice {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(gorocksdb.NoCompression)
	opts.SetAllowMmapWrites(false)
	opts.SetUseFsync(true)

	filter := gorocksdb.NewBloomFilter(10)
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetFilterPolicy(filter)
	opts.SetBlockBasedTableFactory(bbto)

	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		log.Panicln(err)
	}
	return &RocksBlockDevice{db: db, size: size}
}

func main() {
	flag.Parse()

	rocksDev := NewRocksBlockDevice("/pub/amistry/rocks.block", 1024*1024*1024*16)
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

	log.Println(nbd.Run())
}
