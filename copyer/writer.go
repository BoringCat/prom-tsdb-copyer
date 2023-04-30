package copyer

import (
	"context"
	"fmt"
	"path"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

type Queue struct {
	q    []any
	cond *sync.Cond
}

func (q *Queue) Push(val any) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.q = append(q.q, val)
	q.cond.Broadcast()
}

func (q *Queue) Pull() any {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if len(q.q) == 0 {
		q.cond.Wait()
	}
	resp := q.q[0]
	q.q = q.q[1:]
	return resp
}

func (q *Queue) PullNoWait() any {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if len(q.q) == 0 {
		return nil
	}
	resp := q.q[0]
	q.q = q.q[1:]
	return resp
}

type getSeries struct {
	Get   func() storage.SeriesSet
	Close func() error
}

type Writer struct {
	w     *tsdb.BlockWriter
	count *atomic.Uint64
	wg    *sync.WaitGroup
	q     *Queue
	l     *sync.Mutex

	al          labels.Labels
	commitCount uint64
	gc          bool
}

func NewWriter(logger log.Logger, dir string, blockSize int64, al labels.Labels, commitCount uint64, gc bool) (*Writer, error) {
	br, err := tsdb.NewBlockWriter(logger, dir, blockSize)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		w: br, count: &atomic.Uint64{}, wg: &sync.WaitGroup{},
		q: &Queue{q: []any{}, cond: sync.NewCond(&sync.Mutex{})},
		l: &sync.Mutex{}, al: al, commitCount: commitCount, gc: gc,
	}
	return w, nil
}

func (w *Writer) AppendFn(fn *getSeries) {
	w.q.Push(fn)
	w.wg.Add(1)
}

func (w *Writer) WriteJob() {
	w.l.Lock()
	defer w.l.Unlock()
	for bufFn := w.q.Pull(); bufFn != nil; bufFn = w.q.PullNoWait() {
		var appendCount uint64
		var err error
		bufs := bufFn.(*getSeries)
		selecter := bufs.Get()
		defer bufs.Close()
		writer := w.w.Appender(context.TODO())
		for selecter.Next() {
			series := selecter.At()
			var ref storage.SeriesRef = 0
			iter := series.Iterator()
			labels := series.Labels()
			if len(w.al) > 0 {
				labels = append(labels, w.al...)
			}
			for iter.Next() {
				appendCount++
				timeMs, value := iter.At()
				if ref, err = writer.Append(ref, labels, timeMs, value); err != nil {
					panic(err)
				}
			}
			if appendCount >= w.commitCount {
				if err = writer.Commit(); err != nil {
					panic(err)
				}
				w.count.Add(appendCount)
				appendCount = 0
				writer = w.w.Appender(context.TODO())
			}
			if err = iter.Err(); err != nil {
				panic(err)
			}
		}
		if err := selecter.Err(); err != nil {
			panic(err)
		}
		if warnings := selecter.Warnings(); len(warnings) > 0 {
			fmt.Println("warnings:", warnings)
		}
		if err = writer.Commit(); err != nil {
			panic(err)
		}
		w.count.Add(appendCount)
		if w.gc {
			runtime.GC()
		}
		w.wg.Done()
	}
}

func (w *Writer) FlushJob(wg *sync.WaitGroup, mint, maxt int64, tenant string, ch chan<- *TenantResult) func() {
	return func() {
		w.wg.Wait()
		defer wg.Done()
		if count := w.count.Load(); count > 0 {
			ulid, err := w.w.Flush(context.TODO())
			if err != nil {
				panic(errors.Wrap(err, "FlushBlock."+tenant))
			}
			ch <- &TenantResult{Mint: mint, Maxt: maxt, Tenant: tenant, SamCount: count, Ulid: ulid.String()}
		}
		if err := w.w.Close(); err != nil {
			panic(errors.Wrap(err, "CloseBlock."+tenant))
		}
	}
}

type MultiTenantWriter struct {
	w     map[string]*Writer
	sync  sync.Mutex
	count atomic.Int64

	newFn func(string) (*Writer, error)
}

func NewMultiTenantWriter(logger log.Logger, dir string, blockSize int64, al labels.Labels, commitCount uint64, gc bool) *MultiTenantWriter {
	return &MultiTenantWriter{
		w: map[string]*Writer{},
		newFn: func(tenant string) (*Writer, error) {
			return NewWriter(logger, path.Join(dir, tenant), blockSize, al, commitCount, gc)
		},
	}
}

func (mw *MultiTenantWriter) Get(tenant string) (*Writer, error) {
	mw.sync.Lock()
	defer mw.sync.Unlock()
	if w, ok := mw.w[tenant]; ok {
		return w, nil
	}
	if w, err := mw.newFn(tenant); err != nil {
		return nil, err
	} else {
		mw.count.Add(1)
		mw.w[tenant] = w
		return w, nil
	}
}

func (mw *MultiTenantWriter) Range(fn func(string, *Writer) bool) {
	mw.sync.Lock()
	defer mw.sync.Unlock()
	for tenant, w := range mw.w {
		if !fn(tenant, w) {
			break
		}
	}
}

func (mw *MultiTenantWriter) Count() int {
	return int(mw.count.Load())
}
