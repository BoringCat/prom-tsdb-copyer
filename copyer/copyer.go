package copyer

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/boringcat/prom-tsdb-copyer/copyer/local"
	copy_remote "github.com/boringcat/prom-tsdb-copyer/copyer/remote"
	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/go-kit/log"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
)

type TenantResult struct {
	Mint, Maxt int64
	Ulid       string
	Tenant     string
	SamCount   uint64
}

func (r *TenantResult) Show() string {
	return fmt.Sprint(
		"租户 ", r.Tenant, " ",
		time.UnixMilli(r.Mint).Format(time.RFC3339Nano), " 到 ",
		time.UnixMilli(r.Maxt).Format(time.RFC3339Nano), " 的数据完成，有 ",
		r.SamCount, " 个指标",
	)
}

func (r *TenantResult) New(tenant string) *TenantResult {
	return &TenantResult{Mint: r.Mint, Maxt: r.Maxt, Tenant: tenant}
}

type TenantResults []*TenantResult

func (t TenantResults) ToMap() map[string][]string {
	resp := map[string][]string{}
	for _, tu := range t {
		if v, ok := resp[tu.Tenant]; ok {
			resp[tu.Tenant] = append(v, tu.Ulid)
		} else {
			resp[tu.Tenant] = []string{tu.Ulid}
		}
	}
	return resp
}

type CopyOpt struct {
	ManualGC      bool
	WriteThread   int
	Mint          int64
	Cent          int64
	Maxt          int64
	BlockSplit    int64
	TimeSplit     int64
	CommitCount   uint64
	TargetDir     string
	TenantLabel   string
	DefaultTenant string
	LabelAppends  labels.Labels
	LabelMatchers []*labels.Matcher
	JobPool       *ants.Pool
	FlushPool     *ants.Pool
	FlushGroup    *sync.WaitGroup
}

func (opt *CopyOpt) ShowTime() (string, string) {
	return time.UnixMilli(opt.Mint).Format(time.RFC3339Nano), time.UnixMilli(opt.Maxt).Format(time.RFC3339Nano)
}

func (opt *CopyOpt) Range() chan [2]int64 {
	ch := make(chan [2]int64)
	go func() {
		for mint := opt.Mint; mint < opt.Maxt; mint += opt.TimeSplit {
			ch <- [2]int64{mint, utils.Int64Min(mint+opt.TimeSplit, opt.Maxt)}
		}
		close(ch)
	}()
	return ch
}

func (opt *CopyOpt) BlockRange(split int64) chan [2]int64 {
	ch := make(chan [2]int64)
	if split == 0 {
		split = tsdb.DefaultBlockDuration
	}
	go func() {
		for mint := opt.Mint; mint < opt.Maxt; mint += split {
			ch <- [2]int64{mint, utils.Int64Min(mint+split, opt.Maxt)}
		}
		close(ch)
	}()
	return ch
}

func (opt *CopyOpt) Copy(mint, maxt int64) *CopyOpt {
	return &CopyOpt{
		Mint:          mint,
		Maxt:          maxt,
		ManualGC:      opt.ManualGC,
		WriteThread:   opt.WriteThread,
		TargetDir:     opt.TargetDir,
		TenantLabel:   opt.TenantLabel,
		DefaultTenant: opt.DefaultTenant,
		CommitCount:   opt.CommitCount,
		BlockSplit:    opt.BlockSplit,
		TimeSplit:     opt.TimeSplit,
		LabelAppends:  opt.LabelAppends,
		LabelMatchers: opt.LabelMatchers,
		JobPool:       opt.JobPool,
		FlushPool:     opt.FlushPool,
		FlushGroup:    opt.FlushGroup,
	}
}

type getTimeable interface {
	GetTimes() (mint, maxt int64)
}

type Copyer struct {
	logger log.Logger
	client storage.Queryable
}

func NewLocalCopyer(originDir string) (*Copyer, error) {
	db, err := tsdb.OpenDBReadOnly(originDir, nil)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	blockReaders, err := db.Blocks()
	if err != nil {
		return nil, err
	}
	bs := make(local.Blocks, len(blockReaders))
	for idx, br := range blockReaders {
		md := br.Meta()
		bs[idx] = local.NewBlock(path.Join(originDir, md.ULID.String()), md.MinTime, md.MaxTime)
	}
	return &Copyer{client: bs, logger: log.NewNopLogger()}, nil
}

func NewRemoteCopyer(uri string, lbApi *url.URL) (*Copyer, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	rc, err := remote.NewReadClient("remote", &remote.ClientConfig{
		URL:     &config.URL{URL: u},
		Timeout: model.Duration(time.Hour),
	})
	if err != nil {
		return nil, err
	}
	var client storage.Queryable
	client = remote.NewSampleAndChunkQueryableClient(rc, nil, nil, true, nil)
	if lbApi != nil {
		client = copy_remote.NewLabelableQueryable(client, lbApi.String())
	}
	return &Copyer{
		client: client,
		logger: log.NewNopLogger(),
	}, nil
}

func MustNewCopyer(c *Copyer, err error) *Copyer {
	if err != nil {
		panic(err)
	}
	return c
}

func (c *Copyer) getQueryBufferJob(q storage.Querier, opt *CopyOpt, tenant *labels.Matcher) *getSeries {
	var mls []*labels.Matcher
	if tenant == nil {
		mls = opt.LabelMatchers
	} else {
		mls = append([]*labels.Matcher{tenant}, opt.LabelMatchers...)
	}
	return &getSeries{
		Get:   func() storage.SeriesSet { return q.Select(false, nil, mls...) },
		Close: q.Close,
	}
}

func (c *Copyer) copyTenant(w *Writer, q storage.Querier, opt *CopyOpt, tenant string) error {
	m, merr := labels.NewMatcher(labels.MatchEqual, opt.TenantLabel, tenant)
	if merr != nil {
		panic(errors.Wrap(merr, "labels.NewMatcher."+tenant))
	}
	w.AppendFn(c.getQueryBufferJob(q, opt, m))
	return nil
}

func (c *Copyer) multiTenantCopy(opt *CopyOpt, ch chan<- *TenantResult) (err error) {
	wr := NewMultiTenantWriter(c.logger, opt.TargetDir, tsdb.DefaultBlockDuration, opt.LabelAppends, opt.CommitCount, opt.ManualGC)
	for r := range opt.Range() {
		querier, qerr := c.client.Querier(context.TODO(), r[0], r[1])
		if qerr != nil {
			err = errors.Wrap(qerr, "client.Querier")
			return
		}
		tenants, _, terr := querier.LabelValues(opt.TenantLabel)
		if terr != nil {
			err = errors.Wrap(terr, "querier.LabelValues")
			return
		}
		for _, tenant := range tenants {
			w, werr := wr.Get(tenant)
			if werr != nil {
				err = errors.Wrap(werr, "getTenantWriter."+tenant)
				return
			}
			if cerr := c.copyTenant(w, querier, opt, tenant); cerr != nil {
				err = errors.Wrap(cerr, "copyTenant."+tenant)
				return
			}
		}
		w, werr := wr.Get(opt.DefaultTenant)
		if werr != nil {
			err = errors.Wrap(werr, "getDefaultTenantWriter")
			return
		}
		if cerr := c.copyTenant(w, querier, opt, ""); cerr != nil {
			err = errors.Wrap(cerr, "copyDefaultTenant")
			return
		}
	}
	opt.FlushGroup.Add(wr.Count())
	wr.Range(func(tenant string, w *Writer) bool {
		if serr := opt.FlushPool.Submit(w.FlushJob(opt.FlushGroup, opt.Mint, opt.Maxt, tenant, ch)); err != nil {
			err = errors.Wrap(serr, "FlushJob."+tenant)
			return false
		}
		if jerr := opt.JobPool.Submit(w.WriteJob); err != nil {
			err = errors.Wrap(jerr, "Submit.WriteLocalJob."+tenant)
			return false
		}
		return true
	})
	return
}

func (c *Copyer) singalTenantCopy(opt *CopyOpt, ch chan<- *TenantResult) (err error) {
	w, err := NewWriter(c.logger, opt.TargetDir, tsdb.DefaultBlockDuration, opt.LabelAppends, opt.CommitCount, opt.ManualGC)
	if err != nil {
		return err
	}
	for r := range opt.Range() {
		querier, qerr := c.client.Querier(context.TODO(), r[0], r[1])
		if qerr != nil {
			err = errors.Wrap(qerr, "client.Querier")
			return
		}
		w.AppendFn(c.getQueryBufferJob(querier, opt, nil))
	}
	opt.FlushGroup.Add(1)
	if err = opt.FlushPool.Submit(w.FlushJob(opt.FlushGroup, opt.Mint, opt.Maxt, "", ch)); err != nil {
		return
	}
	err = opt.JobPool.Submit(w.WriteJob)
	return
}

func (c *Copyer) CopyPerBlock(opt *CopyOpt, ch chan<- *TenantResult) (err error) {
	mint, maxt := opt.ShowTime()
	defer fmt.Println(mint, "到", maxt, "的任务已发布完成")
	if len(opt.TenantLabel) == 0 {
		return c.singalTenantCopy(opt, ch)
	} else {
		return c.multiTenantCopy(opt, ch)
	}
}

func (c *Copyer) GetDbTimes() (int64, int64) {
	if v, ok := c.client.(getTimeable); ok {
		return v.GetTimes()
	}
	return math.MinInt64, math.MaxInt64
}
