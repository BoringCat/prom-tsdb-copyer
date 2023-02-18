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
	originDir, targetDir string
}

func (c *localCopyer) copyPerTime(opt *copyOpt) (count uint64, err error) {
	tmpdir, err := os.MkdirTemp(c.targetDir, fmt.Sprint("tsdb-copy-tmp-", opt.mint))
	if err != nil {
		return
	}
	defer func() {
		if err = os.RemoveAll(tmpdir); err != nil {
			return
		}
	}()
	if _, err := os.Stat(path.Join(c.originDir, "wal")); os.IsNotExist(err) {
		if err := os.Mkdir(path.Join(c.originDir, "wal"), 0x755); err != nil {
			return 0, err
		}
	}
	// 必须每个协程打开一个TSDB，否则并发会出现 block is closing
	odb, err := tsdb.OpenDBReadOnly(c.originDir, nil)
	if err != nil {
		return
	}
	defer odb.Close()
	mintStr := time.UnixMilli(opt.mint).Format(time.RFC3339Nano)
	maxtStr := time.UnixMilli(opt.maxt).Format(time.RFC3339Nano)
	ndb, err := tsdb.Open(tmpdir, nil, nil, tsdb.DefaultOptions(), nil)
	if err != nil {
		return
	}
	defer ndb.Close()
	ndb.DisableCompactions()
	fmt.Println("开始从", c.originDir, "拷贝", mintStr, "到", maxtStr, "的数据到", c.targetDir)
	var querier storage.Querier
	for startTimeMs := opt.mint; startTimeMs < opt.maxt; startTimeMs += timeSplit {
		var splitCount uint64 = 0
		endTimeMs := int64Min(opt.maxt, startTimeMs+timeSplit)
		if querier, err = odb.Querier(context.Background(), startTimeMs, endTimeMs); err != nil {
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
			writer := ndb.Appender(context.Background())
			for iter.Next() {
				splitCount++
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
		count += splitCount
		fmt.Println(time.UnixMilli(startTimeMs).Format(time.RFC3339Nano), "到", time.UnixMilli(endTimeMs).Format(time.RFC3339Nano), "的数据完成，共", splitCount, "条")
	}
	if count == 0 {
		fmt.Println(mintStr, "到", maxtStr, "共查询到", count, "条数据")
		return
	}
	fmt.Println(mintStr, "到", maxtStr, "共查询到", count, "条数据，开始生成快照")
	if err = ndb.Snapshot(c.targetDir, true); err != nil {
		return
	}
	return
}

func (c *localCopyer) multiThreadCopyer(wg *sync.WaitGroup, req <-chan *copyOpt, resp chan<- *copyResp) {
	defer wg.Done()
	for o := range req {
		c, err := c.copyPerTime(o)
		resp <- &copyResp{c, err}
	}
}

func (c *localCopyer) getDbTimes() (mint int64, maxt int64, err error) {
	mint = math.MaxInt64
	maxt = math.MinInt64
	db, err := tsdb.OpenDBReadOnly(c.originDir, nil)
	if err != nil {
		return
	}
	defer db.Close()
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
