package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/tsdb"
)

var (
	version, commit, buildDate string
)

func int64Min(s1, s2 int64) int64 {
	if s1 > s2 {
		return s2
	}
	return s1
}
func int64Max(s1, s2 int64) int64 {
	if s1 >= s2 {
		return s1
	}
	return s2
}

func multiThreadMain(cp copyer) uint64 {
	var copyCount uint64 = 0
	var workgroup sync.WaitGroup
	var recvgroup sync.WaitGroup
	dbmint, dbmaxt, err := cp.getDbTimes()
	noErr(err)
	qmint := int64Max(dbmint, args.startTime)
	qmaxt := int64Min(dbmaxt, args.endTime)
	req := make(chan *copyOpt)
	resp := make(chan *copyResp)
	workgroup.Add(args.multiThread)
	for i := 0; i < args.multiThread; i++ {
		go cp.multiThreadCopyer(&workgroup, req, resp)
	}
	recvgroup.Add(1)
	go func() {
		for r := range resp {
			noErr(r.err)
			copyCount += r.count
		}
		recvgroup.Done()
	}()
	for mint := qmint; mint < qmaxt; mint += args.blockSplit {
		maxt := int64Min(mint+args.blockSplit, qmaxt)
		fmt.Println("发布拷贝", time.UnixMilli(mint).Format(time.RFC3339Nano), "到", time.UnixMilli(maxt).Format(time.RFC3339Nano), "数据的任务")
		req <- &copyOpt{mint, maxt}
	}
	close(req)
	workgroup.Wait()
	close(resp)
	recvgroup.Wait()
	return copyCount
}

func singleMain(cp copyer) uint64 {
	var copyCount uint64 = 0
	dbmint, dbmaxt, err := cp.getDbTimes()
	noErr(err)
	qmint := int64Max(dbmint, args.startTime)
	qmaxt := int64Min(dbmaxt, args.endTime)
	mintStr := time.UnixMilli(qmint).Format(time.RFC3339Nano)
	maxtStr := time.UnixMilli(qmaxt).Format(time.RFC3339Nano)
	fmt.Println("开始从", args.source, "拷贝", mintStr, "到", maxtStr, "的数据到", args.targetDir)
	for mint := qmint; mint < qmaxt; mint += args.blockSplit {
		db := makeNewDB(qmint, qmaxt, args.targetDir)
		maxt := int64Min(mint+args.blockSplit, qmaxt)
		c, err := cp.copyPerTime(&copyOpt{mint, maxt})
		noErr(err)
		copyCount += c
		if copyCount == 0 {
			fmt.Println(mintStr, "到", maxtStr, "共查询到", c, "条数据")
			db.Clean()
			continue
		}
		db.Fin()
	}
	return copyCount
}

func verify(targetDir string) uint64 {
	db, err := tsdb.OpenDBReadOnly(targetDir, nil)
	noErr(err)
	defer db.Close()
	bs, err := db.Blocks()
	noErr(err)
	var count uint64 = 0
	for _, b := range bs {
		metadata := b.Meta()
		fmt.Println("块", metadata.ULID, "有", metadata.Stats.NumSamples, "条指标")
		count += metadata.Stats.NumSamples
	}
	return count
}

func appendThanosMetadata(targetDir string) {
	if len(args.appendLabels) == 0 {
		panic("Thanos元数据必须添加label")
	}
	blockDirs, err := os.ReadDir(targetDir)
	noErr(err)
	for _, blockDir := range blockDirs {
		if !blockDir.IsDir() {
			continue
		}
		blockPath := path.Join(targetDir, blockDir.Name())
		metaPath := path.Join(blockPath, "meta.json")
		chunksPath := path.Join(blockPath, "chunks")
		fd, err := os.Open(metaPath)
		if os.IsNotExist(err) {
			continue
		}
		metadata := map[string]any{}
		noErr(json.NewDecoder(fd).Decode(&metadata))
		fd.Close()
		chunks, err := os.ReadDir(chunksPath)
		noErr(err)
		segment_files := []string{}
		files := []map[string]any{}
		for _, chunk := range chunks {
			info, err := chunk.Info()
			noErr(err)
			segment_files = append(segment_files, info.Name())
			files = append(files, map[string]any{"rel_path": fmt.Sprint("chunks/", info.Name()), "size_bytes": info.Size()})
		}
		indexInfo, err := os.Stat(path.Join(blockPath, "index"))
		noErr(err)
		files = append(files, map[string]any{"rel_path": "index", "size_bytes": indexInfo.Size()})
		files = append(files, map[string]any{"rel_path": "meta.json"})
		metadata["thanos"] = map[string]any{
			"labels":        args.appendLabels,
			"downsample":    map[string]int{"resolution": 0},
			"source":        "compactor",
			"segment_files": segment_files,
			"files":         files,
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "\t")
		noErr(enc.Encode(metadata))
		noErr(os.WriteFile(metaPath, buf.Bytes(), 0x644))
	}
}

func main() {
	parseArgs()
	var copyCount uint64 = 0
	var existsCount uint64 = 0
	var cp copyer
	if strings.HasPrefix(args.source, "http://") || strings.HasPrefix(args.source, "https://") {
		cp = MustNewRemoteCopyer(args.source, args.targetDir)
	} else {
		cp = MustNewLocalCopyer(args.source)
	}
	if args.multiThread > 0 {
		copyCount = multiThreadMain(cp)
	} else {
		copyCount = singleMain(cp)
	}
	if args.verifyCount {
		existsCount = verify(args.targetDir)
		if existsCount != copyCount {
			panic(fmt.Errorf("复制结果与原始 append 数不一致！ %d != %d", copyCount, existsCount))
		}
		fmt.Println("校验完成")
	}
	if args.appendThanosMetadata {
		appendThanosMetadata(args.targetDir)
	}
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}
