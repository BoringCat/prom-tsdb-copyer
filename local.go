package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"sync"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

type localCopyer struct {
	dbPool sync.Pool
}

func MustNewLocalCopyer(originDir string) *localCopyer {
	if _, err := os.Stat(path.Join(originDir, "wal")); os.IsNotExist(err) {
		noErr(os.Mkdir(path.Join(originDir, "wal"), 0x755))
	}
	return &localCopyer{
		dbPool: sync.Pool{
			New: func() any {
				odb, err := tsdb.OpenDBReadOnly(originDir, nil)
				noErr(err)
				return odb
			},
		},
	}
}

func (c *localCopyer) copyPerTime(opt *copyOpt) (count uint64, err error) {
	// 必须每个协程打开一个TSDB，否则并发会出现 block is closing
	odb := c.dbPool.Get().(*tsdb.DBReadOnly)
	defer c.dbPool.Put(odb)
	var querier storage.Querier
	if querier, err = odb.Querier(context.Background(), opt.mint, opt.maxt); err != nil {
		return
	}
	selecter := querier.Select(false, nil, labelMatchers...)
	for selecter.Next() {
		series := selecter.At()
		labels := series.Labels()
		if len(labelAppends) > 0 {
			labels = append(labels, labelAppends...)
		}
		iter := series.Iterator()
		var ref storage.SeriesRef = 0
		writer := opt.db.Appender(context.Background())
		for iter.Next() {
			count++
			timeMs, value := iter.At()
			if ref, err = writer.Append(ref, labels, timeMs, value); err != nil {
				return
			}
		}
		writer.Commit()
		if err = iter.Err(); err != nil {
			return
		}
	}
	if err = selecter.Err(); err != nil {
		return
	}
	if warnings := selecter.Warnings(); len(warnings) > 0 {
		fmt.Println("warnings:", warnings)
	}
	if err = querier.Close(); err != nil {
		return
	}
	fmt.Println(time.UnixMilli(opt.mint).Format(time.RFC3339Nano), "到", time.UnixMilli(opt.maxt).Format(time.RFC3339Nano), "的数据完成，共", count, "条")
	return
}

func (c *localCopyer) multiThreadCopyer(wg *sync.WaitGroup, req <-chan *copyOpt, resp chan<- *copyResp) {
	defer wg.Done()
	for o := range req {
		c, err := c.copyPerTime(o)
		o.db.Fin()
		resp <- &copyResp{c, err}
	}
}

func (c *localCopyer) getDbTimes() (mint int64, maxt int64, err error) {
	mint = math.MaxInt64
	maxt = math.MinInt64
	db := c.dbPool.Get().(*tsdb.DBReadOnly)
	defer c.dbPool.Put(db)
	bs, err := db.Blocks()
	if err != nil {
		return
	}
	for _, b := range bs {
		m := b.Meta()
		mint = int64Min(mint, m.MinTime)
		maxt = int64Max(maxt, m.MaxTime)
	}
	return
}

func (c *localCopyer) clean() error {
	c.dbPool.New = func() any { return nil }
	for {
		switch db := c.dbPool.Get().(type) {
		case *tsdb.DBReadOnly:
			if err := db.Close(); err != nil {
				return err
			}
		case nil:
			return nil
		}
	}
}
