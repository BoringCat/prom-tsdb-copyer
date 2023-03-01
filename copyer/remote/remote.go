package remote

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/boringcat/prom-tsdb-copyer/copyer"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

func NewRemoteCopyer(uri, targetDir string, labelMatchers []*labels.Matcher) (*remoteCopyer, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	c, err := remote.NewReadClient("remote", &remote.ClientConfig{
		URL:     &config.URL{URL: u},
		Timeout: model.Duration(time.Hour),
	})
	if err != nil {
		return nil, err
	}
	lm := make([]*prompb.LabelMatcher, len(labelMatchers))
	for idx, m := range labelMatchers {
		lm[idx] = &prompb.LabelMatcher{Name: m.Name, Value: m.Value, Type: prompb.LabelMatcher_Type(m.Type)}
	}
	return &remoteCopyer{c, lm}, nil
}

func MustNewRemoteCopyer(uri, targetDir string, labelMatchers []*labels.Matcher) *remoteCopyer {
	if c, err := NewRemoteCopyer(uri, targetDir, labelMatchers); err != nil {
		panic(err)
	} else {
		return c
	}
}

type remoteCopyer struct {
	c  remote.ReadClient
	lm []*prompb.LabelMatcher
}

func (c *remoteCopyer) CopyPerTime(opt *copyer.CopyOpt) (uid string, samCount, serCount uint64, err error) {
	var result *prompb.QueryResult
	ndb, err := copyer.NewDB(opt)
	if err != nil {
		return
	}
	for r := range opt.Range(opt.TimeSplit) {
		startTimeMs, endTimeMs := r[0], r[1]
		result, err = c.c.Read(context.TODO(), &prompb.Query{
			StartTimestampMs: startTimeMs,
			EndTimestampMs:   endTimeMs,
			Matchers:         c.lm,
		})
		if err != nil {
			return
		}
		for _, series := range result.GetTimeseries() {
			serCount++
			qlabels := series.GetLabels()
			lbs := make(labels.Labels, len(qlabels))
			for idx, ql := range qlabels {
				lbs[idx] = labels.Label{Name: ql.GetName(), Value: ql.GetValue()}
			}
			if len(opt.LabelAppends) > 0 {
				lbs = append(lbs, opt.LabelAppends...)
			}
			var ref storage.SeriesRef = 0
			writer := ndb.Appender(context.Background())
			for _, sample := range series.GetSamples() {
				samCount++
				if ref, err = writer.Append(ref, lbs, sample.GetTimestamp(), sample.GetValue()); err != nil {
					return
				}
			}
			if err = writer.Commit(); err != nil {
				return
			}
		}
	}
	mintStr, maxtStr := opt.ShowTime()
	fmt.Println(mintStr, "到", maxtStr, "的数据完成，有", serCount, "个序列，共", samCount, "条")
	if samCount == 0 {
		err = ndb.Clean()
	} else {
		uid, err = ndb.Fin()
	}
	return
}

func (c *remoteCopyer) MultiThreadCopyer(wg *sync.WaitGroup, req <-chan *copyer.CopyOpt, resp chan<- *copyer.CopyResp) {
	defer wg.Done()
	for o := range req {
		resp <- copyer.NewCopyResp(c.CopyPerTime(o))
	}
}

func (c *remoteCopyer) GetDbTimes() (int64, int64, error) {
	return math.MinInt64, math.MaxInt64, nil
}

func (c *remoteCopyer) Clean() error {
	return nil
}
