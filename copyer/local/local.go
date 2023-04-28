package local

import (
	"context"
	"fmt"
	"math"
	"path"
	"sync"

	"github.com/boringcat/prom-tsdb-copyer/copyer"
	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type block struct {
	dir        string
	mint, maxt int64

	pb *tsdb.Block
}

func (b *block) Close() error { return b.pb.Close() }

func (b *block) Has(mint, maxt int64) bool {
	return (mint > b.mint && maxt < b.mint) || (mint < b.maxt && maxt > b.mint) || b.mint >= mint && b.maxt <= maxt
}

func (b *block) GetQueryer(mint, maxt int64) (storage.Querier, error) {
	var err error
	if b.pb == nil {
		if b.pb, err = tsdb.OpenBlock(nil, b.dir, chunkenc.NewPool()); err != nil {
			return nil, err
		}
	}
	return tsdb.NewBlockQuerier(b.pb, mint, maxt)
}

type blocks []*block

func (b blocks) GetBlocks(mint, maxt int64) []*block {
	resp := blocks{}
	for _, block := range b {
		if block.Has(mint, maxt) {
			resp = append(resp, block)
		}
	}
	return resp
}

func (b blocks) GetQueryer(mint, maxt int64) (storage.Querier, error) {
	bs := b.GetBlocks(mint, maxt)
	switch len(bs) {
	case 0:
		return nil, nil
	case 1:
		return bs[0].GetQueryer(mint, maxt)
	}
	bqs := make([]storage.Querier, len(bs))
	for idx, block := range bs {
		if q, err := block.GetQueryer(mint, maxt); err != nil {
			return nil, err
		} else {
			bqs[idx] = q
		}
	}
	return storage.NewMergeQuerier(bqs, nil, storage.ChainedSeriesMerge), nil
}

func (b blocks) GetTimes() (mint, maxt int64) {
	maxt = int64(math.MinInt64)
	mint = int64(math.MaxInt64)
	for _, block := range b {
		mint = utils.Int64Min(mint, block.mint)
		maxt = utils.Int64Max(maxt, block.maxt)
	}
	return
}

type localCopyer struct {
	bs     blocks
	logger log.Logger
}

type writeBuffer struct {
	s storage.Series
	l labels.Labels
}

func NewLocalCopyer(originDir string) (*localCopyer, error) {
	db, err := tsdb.OpenDBReadOnly(originDir, nil)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	blockReaders, err := db.Blocks()
	if err != nil {
		return nil, err
	}
	bs := make(blocks, len(blockReaders))
	for idx, br := range blockReaders {
		md := br.Meta()
		bs[idx] = &block{
			dir:  path.Join(originDir, md.ULID.String()),
			mint: md.MinTime, maxt: md.MaxTime,
		}
	}
	return &localCopyer{bs: bs, logger: log.NewNopLogger()}, nil
}

func MustNewLocalCopyer(originDir string) *localCopyer {
	if c, err := NewLocalCopyer(originDir); err != nil {
		panic(err)
	} else {
		return c
	}
}

func (c *localCopyer) blockFirstWrite(dstDir string, buf map[string][]*writeBuffer, commitCount uint64) (
	bws map[string]*tsdb.BlockWriter, samCount uint64, err error,
) {
	bws = map[string]*tsdb.BlockWriter{}
	for tenant, bufs := range buf {
		var appendCount uint64
		if bws[tenant], err = tsdb.NewBlockWriter(c.logger, path.Join(dstDir, tenant), tsdb.DefaultBlockDuration); err != nil {
			return
		}
		if appendCount, err = c.writerABuffer(bws[tenant], bufs, commitCount); err != nil {
			return
		} else if appendCount == 0 {
			if err = bws[tenant].Close(); err != nil {
				return
			}
			delete(bws, tenant)
		}
		samCount += appendCount
	}
	return
}

func (c *localCopyer) blockWrite(
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

func (c *localCopyer) getQueryBuffer(q storage.Querier, opt *copyer.CopyOpt) (map[string][]*writeBuffer, error) {
	buffer := map[string][]*writeBuffer{}
	selecter := q.Select(false, nil, opt.LabelMatchers...)
	for selecter.Next() {
		series := selecter.At()
		labels := series.Labels()
		tenant := labels.Get(opt.TenantLabel)
		if len(tenant) == 0 {
			tenant = opt.DefaultTenant
		}
		if len(opt.LabelAppends) > 0 {
			labels = append(labels, opt.LabelAppends...)
		}
		if v, ok := buffer[tenant]; ok {
			buffer[tenant] = append(v, &writeBuffer{series, labels})
		} else {
			buffer[tenant] = []*writeBuffer{{series, labels}}
		}
	}
	if err := selecter.Err(); err != nil {
		return nil, err
	}
	if warnings := selecter.Warnings(); len(warnings) > 0 {
		fmt.Println("warnings:", warnings)
	}
	return buffer, nil
}

func (c *localCopyer) writerABuffer(w storage.Appendable, bufs []*writeBuffer, commitCount uint64) (samCount uint64, err error) {
	var appendCount uint64
	writer := w.Appender(context.TODO())
	for _, buf := range bufs {
		var ref storage.SeriesRef = 0
		iter := buf.s.Iterator()
		for iter.Next() {
			appendCount++
			timeMs, value := iter.At()
			if ref, err = writer.Append(ref, buf.l, timeMs, value); err != nil {
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
		if err = iter.Err(); err != nil {
			return
		}
	}
	if err = writer.Commit(); err != nil {
		return
	}
	samCount += appendCount
	return
}

func (c *localCopyer) CopyPerBlock(opt *copyer.CopyOpt) (uids []*copyer.TenantUlid, samCount uint64, err error) {
	var querier storage.Querier
	bws := map[string]*tsdb.BlockWriter{}
	for r := range opt.Range() {
		var appendCount uint64
		var buffer map[string][]*writeBuffer
		startMs, endMs := r[0], r[1]
		if querier, err = c.bs.GetQueryer(startMs, endMs); err != nil {
			return
		}
		if buffer, err = c.getQueryBuffer(querier, opt); err != nil {
			return
		}
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

func (c *localCopyer) MultiThreadCopyer(wg *sync.WaitGroup, req <-chan *copyer.CopyOpt, resp chan<- *copyer.CopyResp) {
	defer wg.Done()
	for o := range req {
		resp <- copyer.NewCopyResp(c.CopyPerBlock(o))
	}
}

func (c *localCopyer) GetDbTimes() (mint int64, maxt int64, err error) {
	mint, maxt = c.bs.GetTimes()
	return
}

func (c *localCopyer) Clean() error {
	for _, b := range c.bs {
		if err := b.Close(); err != nil {
			return err
		}
	}
	return nil
}
