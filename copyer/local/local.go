package local

import (
	"context"
	"math"

	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type Block struct {
	dir        string
	mint, maxt int64

	pb *tsdb.Block
}

func NewBlock(dir string, mint, maxt int64) *Block {
	return &Block{dir: dir, mint: mint, maxt: maxt}
}

func NewBlockByBlock(pb *tsdb.Block) *Block {
	return &Block{dir: pb.Dir(), mint: pb.MinTime(), maxt: pb.MaxTime(), pb: pb}
}

func (b *Block) Close() error { return b.pb.Close() }

func (b *Block) Has(mint, maxt int64) bool {
	return (mint > b.mint && maxt < b.mint) || (mint < b.maxt && maxt > b.mint) || b.mint >= mint && b.maxt <= maxt
}

func (b *Block) Querier(_ context.Context, mint, maxt int64) (storage.Querier, error) {
	var err error
	if b.pb == nil {
		if b.pb, err = tsdb.OpenBlock(nil, b.dir, chunkenc.NewPool()); err != nil {
			return nil, err
		}
	}
	return tsdb.NewBlockQuerier(b.pb, mint, maxt)
}

type Blocks []*Block

func (b Blocks) GetBlocks(mint, maxt int64) []*Block {
	resp := Blocks{}
	for _, block := range b {
		if block.Has(mint, maxt) {
			resp = append(resp, block)
		}
	}
	return resp
}

func (b Blocks) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	bs := b.GetBlocks(mint, maxt)
	switch len(bs) {
	case 0:
		return nil, nil
	case 1:
		return bs[0].Querier(ctx, mint, maxt)
	}
	bqs := make([]storage.Querier, len(bs))
	for idx, block := range bs {
		if q, err := block.Querier(ctx, mint, maxt); err != nil {
			return nil, err
		} else {
			bqs[idx] = q
		}
	}
	return storage.NewMergeQuerier(bqs, nil, storage.ChainedSeriesMerge), nil
}

func (b Blocks) GetTimes() (mint, maxt int64) {
	maxt = int64(math.MinInt64)
	mint = int64(math.MaxInt64)
	for _, block := range b {
		mint = utils.Int64Min(mint, block.mint)
		maxt = utils.Int64Max(maxt, block.maxt)
	}
	return
}
