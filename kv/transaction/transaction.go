package mvcc

type Modify struct {
	Kind WriteKind
}

type MvccTxn struct {
	StartTs int64
}
