package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type copyDB struct {
	taskCount int
	*tsdb.DB
	mint, maxt        int64
	tmpdir, targetDir string
}

func (c *copyDB) Fin() {
	fmt.Println(
		time.UnixMilli(c.mint).Format(time.RFC3339Nano),
		"到", time.UnixMilli(c.maxt).Format(time.RFC3339Nano),
		"的分块完成，开始生成快照")
	if args.allowOutOfOrder {
		noErr(c.DB.CompactOOOHead())
	}
	noErr(c.DB.Compact())
	stmpdir, err := os.MkdirTemp(c.targetDir, fmt.Sprint("tsdb-snapshot-tmp-", c.mint))
	noErr(err)
	noErr(c.DB.Snapshot(stmpdir, true))
	noErr(compact(stmpdir))
	noErr(os.RemoveAll(stmpdir))
	c.Clean()
}
func (c *copyDB) Clean() {
	noErr(c.DB.CleanTombstones())
	noErr(c.DB.Close())
	noErr(os.RemoveAll(c.tmpdir))
}

func makeNewDB(mint, maxt int64, targetDir string) *copyDB {
	tmpdir, err := os.MkdirTemp(targetDir, fmt.Sprint("tsdb-copy-tmp-", mint))
	noErr(err)
	opt := tsdb.DefaultOptions()
	opt.WALCompression = true
	if args.allowOutOfOrder {
		opt.OutOfOrderTimeWindow = args.blockSplit
	}
	db, err := tsdb.Open(tmpdir, nil, nil, opt, nil)
	noErr(err)
	db.DisableCompactions()
	return &copyDB{
		DB:        db,
		mint:      mint,
		maxt:      maxt,
		tmpdir:    tmpdir,
		targetDir: targetDir,
	}
}

func compact(snapshotDir string) error {
	dirs := []string{}
	rdirs, err := os.ReadDir(snapshotDir)
	if err != nil {
		return err
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
		return err
	}
	uid, err := compacter.Compact(args.targetDir, dirs, nil)
	if err != nil {
		return err
	}
	fmt.Println("数据压缩完成, 生成块", uid)
	for _, d := range dirs {
		if err := os.RemoveAll(d); err != nil {
			return err
		}
		fmt.Println("数据压缩完成, 删除块", path.Base(d))
	}
	return nil
}
