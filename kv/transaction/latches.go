package mvcc

import "sync"

type Latches struct {
	latchMap   map[string]*sync.WaitGroup
	latchGuard sync.Mutex
}

func NewLatches() *Latches {
	l := &Latches{}
	l.latchMap = make(map[string]*sync.WaitGroup)
	return l
}

func (l *Latches) AcquireLatches(keys [][]byte) *sync.WaitGroup {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()
	for _, key := range keys {
		if latchWg, ok := l.latchMap[string(key)]; ok {
			return latchWg
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	for _, key := range keys {
		l.latchMap[string(key)] = wg
	}
	return nil
}

func (l *Latches) ReleaseLatches(keys [][]byte) {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()
	first := true
	for _, key := range keys {
		if first {
			wg := l.latchMap[string(key)]
			wg.Done()
			first = false
		}
		delete(l.latchMap, string(key))
	}
}

func (l *Latches) WaitForLatches(keys [][]byte) {
	for {
		wg := l.AcquireLatches(keys)
		if wg == nil {
			return
		}
		wg.Wait()
	}
}
