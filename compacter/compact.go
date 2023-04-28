package compacter

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var (
	logger log.Logger
)

func SetLogger(l log.Logger) {
	logger = l
}

type dirMeta struct {
	dir  string
	meta *tsdb.BlockMeta
}

func Compact(dstDir, srcDir string, clean bool, blockSplit int64) ([]string, error) {
	dirs := []*dirMeta{}
	rdirs, err := os.ReadDir(srcDir)
	if err != nil {
		return nil, err
	}
	for _, d := range rdirs {
		dir := path.Join(srcDir, d.Name())
		metaFile := path.Join(dir, "meta.json")
		fd, err := os.Open(metaFile)
		if err != nil {
			logger.Log("level", "info", "msg", err)
			continue
		}
		var m tsdb.BlockMeta
		if err := json.NewDecoder(fd).Decode(&m); err != nil {
			return nil, err
		}
		dirs = append(dirs, &dirMeta{dir, &m})
	}
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].meta.MinTime < dirs[j].meta.MinTime })
	jobs := [][]string{}
	if blockSplit == 0 {
		jobs = append(jobs, make([]string, len(dirs)))
		for idx, d := range dirs {
			jobs[0][idx] = d.dir
		}
	} else {
		var hourSplited int64 = 0
		tjobs := []string{}
		for _, d := range dirs {
			tjobs = append(tjobs, d.dir)
			hourSplited += int64(math.Round(float64(d.meta.MaxTime-d.meta.MinTime)/float64(time.Hour.Milliseconds()))) * time.Hour.Milliseconds()
			if hourSplited >= blockSplit {
				jobs = append(jobs, tjobs)
				tjobs = []string{}
				hourSplited = 0
			}
		}
		if len(tjobs) > 0 {
			jobs = append(jobs, tjobs)
		}
	}
	ulids := []string{}
	for _, j := range jobs {
		ulid, err := compact(dstDir, j, clean)
		if err != nil {
			return ulids, err
		}
		ulids = append(ulids, ulid)
	}
	return ulids, nil
}

func compact(dst string, src []string, clean bool) (string, error) {
	switch len(src) {
	case 0:
		return "", nil
	case 1:
		name := path.Base(src[0])
		dstPath := path.Join(dst, name)
		if src[0] == dstPath {
			return name, os.Remove(path.Join(dstPath, "tombstones"))
		} else if err := os.Rename(src[0], dstPath); err != nil {
			return "", err
		}
		return name, os.Remove(path.Join(dstPath, "tombstones"))
	}
	compacter, err := tsdb.NewLeveledCompactor(context.Background(), nil, logger, []int64{0}, chunkenc.NewPool(), nil)
	if err != nil {
		return "", err
	}
	uid, err := compacter.Compact(dst, src, nil)
	if err != nil {
		return "", err
	}
	if clean {
		for _, dir := range src {
			os.RemoveAll(dir)
		}
	}
	return uid.String(), os.Remove(path.Join(dst, uid.String(), "tombstones"))
}
