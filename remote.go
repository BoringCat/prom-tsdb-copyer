package main

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

func MustNewRemoteCopyer(uri, targetDir string) *remoteCopyer {
	u, err := url.Parse(uri)
	noErr(err)
	c, err := remote.NewReadClient("remote", &remote.ClientConfig{
		URL:     &config.URL{URL: u},
		Timeout: model.Duration(time.Hour),
	})
	noErr(err)
	lm := make([]*prompb.LabelMatcher, len(args.labelMatchers))
	for idx, m := range args.labelMatchers {
		lm[idx] = &prompb.LabelMatcher{Name: m.Name, Value: m.Value, Type: prompb.LabelMatcher_Type(m.Type)}
	}
	return &remoteCopyer{c, lm}
}

type remoteCopyer struct {
	c  remote.ReadClient
	lm []*prompb.LabelMatcher
}

func (c *remoteCopyer) copyPerTime(opt *copyOpt) (count uint64, err error) {
	var result *prompb.QueryResult
	ndb := makeNewDB(opt.mint, opt.maxt, args.targetDir)
	for startTimeMs := opt.mint; startTimeMs < opt.maxt; startTimeMs += args.timeSplit {
		endTimeMs := int64Min(opt.maxt, startTimeMs+args.timeSplit)
		result, err = c.c.Read(context.TODO(), &prompb.Query{
			StartTimestampMs: startTimeMs,
			EndTimestampMs:   endTimeMs,
			Matchers:         c.lm,
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
			if len(args.labelAppends) > 0 {
				lbs = append(lbs, args.labelAppends...)
			}
			var ref storage.SeriesRef = 0
			writer := ndb.Appender(context.Background())
			for _, sample := range series.GetSamples() {
				count++
				if ref, err = writer.Append(ref, lbs, sample.GetTimestamp(), sample.GetValue()); err != nil {
					return
				}
			}
			if err = writer.Commit(); err != nil {
				return
			}
		}
	}
	fmt.Println(time.UnixMilli(opt.mint).Format(time.RFC3339Nano), "到", time.UnixMilli(opt.maxt).Format(time.RFC3339Nano), "的数据完成，共", count, "条")
	if count == 0 {
		ndb.Clean()
	} else {
		ndb.Fin()
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

func (c *remoteCopyer) clean() error {
	return nil
}
