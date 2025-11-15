package main

import (
	"context"
	"fmt"
	"os"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var (
	compactor *tsdb.LeveledCompactor
)

func initCompactor() {
	var err error
	compactor, err = tsdb.NewLeveledCompactor(context.Background(), reg, logger.With("logger", "compactor"), []int64{0}, chunkenc.NewPool(), nil)
	if err != nil {
		panic(err.Error())
	}
}

func doCompact(blockIds []ulid.ULID) (ulid.ULID, error) {
	dirs := make([]string, len(blockIds))
	for idx, bid := range blockIds {
		dirs[idx] = fmt.Sprintf("%s/%v", args.targetDir, bid)
	}
	bid, err := compactor.Compact(args.targetDir, dirs, nil)
	if err != nil {
		return ulid.Zero, errors.Wrap(err, "压缩块失败")
	}
	for _, dir := range dirs {
		os.RemoveAll(dir)
	}
	return bid[0], nil
}
