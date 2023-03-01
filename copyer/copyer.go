package copyer

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type Copyer interface {
	CopyPerTime(opt *CopyOpt) (uid string, samCount, serCount uint64, err error)
	MultiThreadCopyer(wg *sync.WaitGroup, req <-chan *CopyOpt, resp chan<- *CopyResp)
	GetDbTimes() (mint, maxt int64, err error)
	Clean() error
}

func NewCopyOpt(mint, maxt, blockSplit, timeSplit int64, targetDir string, allowOutOfOrder bool, labelAppends labels.Labels, labelMatchers []*labels.Matcher) *CopyOpt {
	return &CopyOpt{mint, maxt, targetDir, allowOutOfOrder, blockSplit, timeSplit, labelAppends, labelMatchers}
}

type CopyOpt struct {
	Mint            int64
	Maxt            int64
	TargetDir       string
	AllowOutOfOrder bool
	BlockSplit      int64
	TimeSplit       int64
	LabelAppends    labels.Labels
	LabelMatchers   []*labels.Matcher
}

func (opt *CopyOpt) ShowTime() (string, string) {
	return time.UnixMilli(opt.Mint).Format(time.RFC3339Nano), time.UnixMilli(opt.Maxt).Format(time.RFC3339Nano)
}

func (opt *CopyOpt) Range(split int64) chan [2]int64 {
	ch := make(chan [2]int64)
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
		Mint:            mint,
		Maxt:            maxt,
		TargetDir:       opt.TargetDir,
		AllowOutOfOrder: opt.AllowOutOfOrder,
		BlockSplit:      opt.BlockSplit,
		TimeSplit:       opt.TimeSplit,
		LabelAppends:    opt.LabelAppends,
		LabelMatchers:   opt.LabelMatchers,
	}
}

func NewCopyResp(uid string, samCount, serCount uint64, err error) *CopyResp {
	return &CopyResp{uid, samCount, serCount, err}
}

type CopyResp struct {
	Uid      string
	SamCount uint64
	SerCount uint64
	Err      error
}

type CopyDB struct {
	*tsdb.DB
	mint, maxt        int64
	tmpdir, targetDir string
	allowOutOfOrder   bool
}

func (c *CopyDB) Fin() (string, error) {
	fmt.Println(
		time.UnixMilli(c.mint).Format(time.RFC3339Nano),
		"到", time.UnixMilli(c.maxt).Format(time.RFC3339Nano),
		"的分块完成，开始生成快照")
	if c.allowOutOfOrder {
		if err := c.DB.CompactOOOHead(); err != nil {
			return "", err
		}
	}
	if err := c.DB.Compact(); err != nil {
		return "", err
	}
	stmpdir, err := os.MkdirTemp(c.targetDir, fmt.Sprint("tsdb-snapshot-tmp-", c.mint))
	if err != nil {
		return "", err
	}
	if err := c.DB.Snapshot(stmpdir, true); err != nil {
		return "", err
	}
	uid, err := compact(c.targetDir, stmpdir)
	if err != nil {
		return "", err
	}
	if err := os.RemoveAll(stmpdir); err != nil {
		return uid, err
	}
	return uid, c.Clean()
}
func (c *CopyDB) Clean() error {
	if err := c.DB.CleanTombstones(); err != nil {
		return err
	}
	if err := c.DB.Close(); err != nil {
		return err
	}
	return os.RemoveAll(c.tmpdir)
}

func NewDB(copt *CopyOpt) (*CopyDB, error) {
	tmpdir, err := os.MkdirTemp(copt.TargetDir, fmt.Sprint("tsdb-copy-tmp-", copt.Mint))
	if err != nil {
		return nil, err
	}
	opt := tsdb.DefaultOptions()
	opt.WALCompression = true
	if copt.AllowOutOfOrder {
		opt.OutOfOrderTimeWindow = copt.BlockSplit
	}
	db, err := tsdb.Open(tmpdir, nil, nil, opt, nil)
	if err != nil {
		return nil, err
	}
	db.DisableCompactions()
	return &CopyDB{
		DB:              db,
		mint:            copt.Mint,
		maxt:            copt.Maxt,
		tmpdir:          tmpdir,
		targetDir:       copt.TargetDir,
		allowOutOfOrder: copt.AllowOutOfOrder,
	}, nil
}

func MustNewDB(opt *CopyOpt) *CopyDB {
	if db, err := NewDB(opt); err != nil {
		panic(err)
	} else {
		return db
	}
}

func compact(targetDir, snapshotDir string) (string, error) {
	dirs := []string{}
	rdirs, err := os.ReadDir(snapshotDir)
	if err != nil {
		return "", err
	}
	for _, d := range rdirs {
		dirs = append(dirs, path.Join(snapshotDir, d.Name()))
	}
	compacter, err := tsdb.NewLeveledCompactor(
		context.Background(), nil, nil,
		tsdb.ExponentialBlockRanges(tsdb.DefaultBlockDuration, 3, 5),
		chunkenc.NewPool(), nil,
	)
	if err != nil {
		return "", err
	}
	uid, err := compacter.Compact(targetDir, dirs, nil)
	if err != nil {
		return "", err
	}
	return uid.String(), nil
}
