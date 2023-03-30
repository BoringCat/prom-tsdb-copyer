package local

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"sync"

	"github.com/boringcat/prom-tsdb-copyer/copyer"
	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

type localCopyer struct {
	dbPool sync.Pool
}

func NewLocalCopyer(originDir string) (*localCopyer, error) {
	if _, err := os.Stat(path.Join(originDir, "wal")); os.IsNotExist(err) {
		if err := os.Mkdir(path.Join(originDir, "wal"), 0x755); err != nil {
			return nil, err
		}
	}
	return &localCopyer{
		dbPool: sync.Pool{
			New: func() any {
				if odb, err := tsdb.OpenDBReadOnly(originDir, nil); err != nil {
					panic(err)
				} else {
					return odb
				}
			},
		},
	}, nil
}

func MustNewLocalCopyer(originDir string) *localCopyer {
	if c, err := NewLocalCopyer(originDir); err != nil {
		panic(err)
	} else {
		return c
	}
}

func (c *localCopyer) CopyPerTime(opt *copyer.CopyOpt) (uid string, samCount, serCount uint64, err error) {
	// 必须每个协程打开一个TSDB，否则并发会出现 block is closing
	odb := c.dbPool.Get().(*tsdb.DBReadOnly)
	defer c.dbPool.Put(odb)
	var querier storage.Querier
	ndb, err := copyer.NewDB(opt)
	if err != nil {
		return
	}
	uniqSeries := map[string]struct{}{}
	for r := range opt.Range(opt.TimeSplit) {
		startTimeMs, endTimeMs := r[0], r[1]
		if querier, err = odb.Querier(context.Background(), startTimeMs, endTimeMs); err != nil {
			return
		}
		selecter := querier.Select(false, nil, opt.LabelMatchers...)
		for selecter.Next() {
			series := selecter.At()
			labels := series.Labels()
			if len(opt.LabelAppends) > 0 {
				labels = append(labels, opt.LabelAppends...)
			}
			iter := series.Iterator()
			var ref storage.SeriesRef = 0
			var hasSample bool
			writer := ndb.Appender(context.Background())
			for iter.Next() {
				hasSample = true
				samCount++
				timeMs, value := iter.At()
				if ref, err = writer.Append(ref, labels, timeMs, value); err != nil {
					return
				}
			}
			if hasSample {
				uniqSeries[labels.String()] = struct{}{}
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
	}
	serCount = uint64(len(uniqSeries))
	mintStr, maxtStr := opt.ShowTime()
	fmt.Println(mintStr, "到", maxtStr, "的数据完成，有", serCount, "个序列，共", samCount, "条")
	if samCount == 0 {
		err = ndb.Clean()
	} else {
		uid, err = ndb.Fin()
	}
	return
}

func (c *localCopyer) MultiThreadCopyer(wg *sync.WaitGroup, req <-chan *copyer.CopyOpt, resp chan<- *copyer.CopyResp) {
	defer wg.Done()
	for o := range req {
		resp <- copyer.NewCopyResp(c.CopyPerTime(o))
	}
}

func (c *localCopyer) GetDbTimes() (mint int64, maxt int64, err error) {
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
		mint = utils.Int64Min(mint, m.MinTime)
		maxt = utils.Int64Max(maxt, m.MaxTime)
	}
	return
}

func (c *localCopyer) Clean() error {
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
