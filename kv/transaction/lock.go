package mvcc

type Lock struct {
	Primary []byte
	StartTS uint64
	TTL     uint64
}
