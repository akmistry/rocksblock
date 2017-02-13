Experiment in building a block device using a KV-store.

Sectors are stored in rocksdb using the 64-bit byte offset encoded
as a big-endian string. A big-endian encoding ensures consecutive sectors
are stored consecutively in the KV-store (assuming the store orders data
lexicographically), preserving spatial locality.

Unaligned/partial reads are supported, but writes MUST be aligned to the
block size. Unaligned/partial writes would require a read-modify-write cycle.

This is an experiment and NOT intended for production use.
