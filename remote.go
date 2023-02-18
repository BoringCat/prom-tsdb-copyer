package main

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
)

func MustNewRemoteCopyer(uri, targetDir string) *remoteCopyer {
	u, err := url.Parse(uri)
	noErr(err)
	c, err := remote.NewReadClient("remote", &remote.ClientConfig{
		URL:     &config.URL{URL: u},
		Timeout: model.Duration(time.Hour),
	})
	noErr(err)
	return &remoteCopyer{c, targetDir}
}

type remoteCopyer struct {
	c         remote.ReadClient
	targetDir string
}

func (c *remoteCopyer) copyPerTime(opt *copyOpt) (count uint64, err error) {
	tmpdir, err := os.MkdirTemp(c.targetDir, fmt.Sprint("tsdb-copy-tmp-", opt.mint))
	if err != nil {
		return
	}
	defer func() {
		if err = os.RemoveAll(tmpdir); err != nil {
			return
		}
	}()
	mintStr := time.UnixMilli(opt.mint).Format(time.RFC3339Nano)
	maxtStr := time.UnixMilli(opt.maxt).Format(time.RFC3339Nano)
	ndb, err := tsdb.Open(tmpdir, nil, nil, tsdb.DefaultOptions(), nil)
	if err != nil {
		return
	}
	defer ndb.Close()
	ndb.DisableCompactions()
	fmt.Println("开始拷贝", mintStr, "到", maxtStr, "的数据到", c.targetDir)
	lm := make([]*prompb.LabelMatcher, len(labelMatchers))
	for idx, m := range labelMatchers {
		lm[idx] = &prompb.LabelMatcher{Name: m.Name, Value: m.Value, Type: prompb.LabelMatcher_Type(m.Type)}
	}
	var result *prompb.QueryResult
	for startTimeMs := opt.mint; startTimeMs < opt.maxt; startTimeMs += timeSplit {
		var splitCount uint64 = 0
		endTimeMs := int64Min(opt.maxt, startTimeMs+timeSplit)
		result, err = c.c.Read(context.TODO(), &prompb.Query{
			StartTimestampMs: startTimeMs,
			EndTimestampMs:   endTimeMs,
			Matchers:         lm,
		})
		if err != nil {
			return
		}
		for _, series := range result.GetTimeseries() {
			qlabels := series.GetLabels()
			lbs := make(labels.Labels, len(qlabels))
			for idx, ql := range qlabels {
				lbs[idx] = labels.Label{Name: ql.GetName(), Value: ql.GetValue()}
			}
			if len(labelAppends) > 0 {
				lbs = append(lbs, labelAppends...)
			}
			var ref storage.SeriesRef = 0
			writer := ndb.Appender(context.Background())
			for _, sample := range series.GetSamples() {
				splitCount++
				if ref, err = writer.Append(ref, lbs, sample.GetTimestamp(), sample.GetValue()); err != nil {
					return
				}
			}
			if err = writer.Commit(); err != nil {
				return
			}
		}
		fmt.Println(time.UnixMilli(startTimeMs).Format(time.RFC3339Nano), "到", time.UnixMilli(endTimeMs).Format(time.RFC3339Nano), "的数据完成，共", splitCount, "条")
		count += splitCount
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

func (c *remoteCopyer) multiThreadCopyer(wg *sync.WaitGroup, req <-chan *copyOpt, resp chan<- *copyResp) {
	defer wg.Done()
	for o := range req {
		c, err := c.copyPerTime(o)
		resp <- &copyResp{c, err}
	}
}

func (c *remoteCopyer) getDbTimes() (int64, int64, error) {
	return math.MinInt64, math.MaxInt64, nil
}
