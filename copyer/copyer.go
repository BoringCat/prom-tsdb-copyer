package copyer

import (
	"sync"
	"time"

	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

type TenantUlid struct {
	Ulid   string
	Tenant string
}
type TenantUlids []*TenantUlid

func (t TenantUlids) ToMap() map[string][]string {
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

type Copyer interface {
	CopyPerBlock(opt *CopyOpt) (uids []*TenantUlid, samCount uint64, err error)
	MultiThreadCopyer(wg *sync.WaitGroup, req <-chan *CopyOpt, resp chan<- *CopyResp)
	GetDbTimes() (mint, maxt int64, err error)
	Clean() error
}

type CopyOpt struct {
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
		TargetDir:     opt.TargetDir,
		TenantLabel:   opt.TenantLabel,
		DefaultTenant: opt.DefaultTenant,
		CommitCount:   opt.CommitCount,
		BlockSplit:    opt.BlockSplit,
		TimeSplit:     opt.TimeSplit,
		LabelAppends:  opt.LabelAppends,
		LabelMatchers: opt.LabelMatchers,
	}
}

func NewCopyResp(uids []*TenantUlid, samCount uint64, err error) *CopyResp {
	return &CopyResp{uids, samCount, err}
}

type CopyResp struct {
	Uids     []*TenantUlid
	SamCount uint64
	Err      error
}
