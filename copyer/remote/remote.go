package remote

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/boringcat/prom-tsdb-copyer/copyer"
	"github.com/go-kit/log"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
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
	return &remoteCopyer{c: c, lm: lm, logger: log.NewNopLogger()}, nil
}

func MustNewRemoteCopyer(uri, targetDir string, labelMatchers []*labels.Matcher) *remoteCopyer {
	if c, err := NewRemoteCopyer(uri, targetDir, labelMatchers); err != nil {
		panic(err)
	} else {
		return c
	}
}

type writeBuffer struct {
	s *prompb.TimeSeries
	l labels.Labels
}

type remoteCopyer struct {
	c      remote.ReadClient
	lm     []*prompb.LabelMatcher
	logger log.Logger
}

func (c *remoteCopyer) getQueryBuffer(result *prompb.QueryResult, opt *copyer.CopyOpt) map[string][]*writeBuffer {
	buffer := map[string][]*writeBuffer{}
	for _, series := range result.GetTimeseries() {
		qlabels := series.GetLabels()
		lbs := make(labels.Labels, len(qlabels))
		for idx, ql := range qlabels {
			lbs[idx] = labels.Label{Name: ql.GetName(), Value: ql.GetValue()}
		}
		tenant := lbs.Get(opt.TenantLabel)
		if len(tenant) == 0 {
			tenant = opt.DefaultTenant
		}
		if len(opt.LabelAppends) > 0 {
			lbs = append(lbs, opt.LabelAppends...)
		}
		if v, ok := buffer[tenant]; ok {
			buffer[tenant] = append(v, &writeBuffer{series, lbs})
		} else {
			buffer[tenant] = []*writeBuffer{{series, lbs}}
		}
	}
	return buffer
}

func (c *remoteCopyer) writerABuffer(w storage.Appendable, bufs []*writeBuffer, commitCount uint64) (samCount uint64, err error) {
	var appendCount uint64
	writer := w.Appender(context.TODO())
	for _, buf := range bufs {
		var ref storage.SeriesRef = 0
		for _, sample := range buf.s.GetSamples() {
			appendCount++
			if ref, err = writer.Append(ref, buf.l, sample.GetTimestamp(), sample.GetValue()); err != nil {
				return
			}
		}
		if appendCount >= commitCount {
			if err = writer.Commit(); err != nil {
				return
			}
			samCount += appendCount
			appendCount = 0
			writer = w.Appender(context.TODO())
		}
	}
	if err = writer.Commit(); err != nil {
		return
	}
	samCount += appendCount
	return
}

func (c *remoteCopyer) blockWrite(
	dstDir string, bws map[string]*tsdb.BlockWriter,
	buf map[string][]*writeBuffer, commitCount uint64,
) (nbws map[string]*tsdb.BlockWriter, samCount uint64, err error) {
	nbws = bws
	for tenant, bufs := range buf {
		var appendCount uint64
		bw, ok := nbws[tenant]
		if !ok {
			if bw, err = tsdb.NewBlockWriter(c.logger, path.Join(dstDir, tenant), tsdb.DefaultBlockDuration); err != nil {
				return
			}
			nbws[tenant] = bw
		}
		if appendCount, err = c.writerABuffer(bw, bufs, commitCount); err != nil {
			return
		}
		samCount += appendCount
	}
	return
}

func (c *remoteCopyer) CopyPerBlock(opt *copyer.CopyOpt) (uids []*copyer.TenantUlid, samCount uint64, err error) {
	var result *prompb.QueryResult
	bws := map[string]*tsdb.BlockWriter{}
	for r := range opt.Range() {
		var appendCount uint64
		startMs, endMs := r[0], r[1]
		if result, err = c.c.Read(context.TODO(), &prompb.Query{
			StartTimestampMs: startMs,
			EndTimestampMs:   endMs,
			Matchers:         c.lm,
		}); err != nil {
			return
		}
		buffer := c.getQueryBuffer(result, opt)
		if len(buffer) == 0 {
			continue
		}
		if bws, appendCount, err = c.blockWrite(opt.TargetDir, bws, buffer, opt.CommitCount); err != nil {
			return
		}
		samCount += appendCount
	}
	uids = []*copyer.TenantUlid{}
	for tenant, bw := range bws {
		ulid, ferr := bw.Flush(context.TODO())
		if ferr != nil {
			err = ferr
			return
		}
		uids = append(uids, &copyer.TenantUlid{Tenant: tenant, Ulid: ulid.String()})
		if err = bw.Close(); err != nil {
			return
		}
	}
	mintStr, maxtStr := opt.ShowTime()
	fmt.Println(mintStr, "到", maxtStr, "的数据完成，有", samCount, "个指标")
	return
}

func (c *remoteCopyer) MultiThreadCopyer(wg *sync.WaitGroup, req <-chan *copyer.CopyOpt, resp chan<- *copyer.CopyResp) {
	defer wg.Done()
	for o := range req {
		resp <- copyer.NewCopyResp(c.CopyPerBlock(o))
	}
}

func (c *remoteCopyer) GetDbTimes() (int64, int64, error) {
	return math.MinInt64, math.MaxInt64, nil
}

func (c *remoteCopyer) Clean() error {
	return nil
}
