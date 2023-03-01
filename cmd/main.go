package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/boringcat/prom-tsdb-copyer/copyer"
	"github.com/boringcat/prom-tsdb-copyer/copyer/local"
	"github.com/boringcat/prom-tsdb-copyer/copyer/remote"
	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/prometheus/prometheus/tsdb"
)

var (
	version, commit, buildDate string
)

type uidChecker []string

func (c uidChecker) Has(uid string) bool {
	for _, id := range c {
		if uid == id {
			return true
		}
	}
	return false
}

func multiThreadMain(cp copyer.Copyer) (samCount, serCount uint64, uids []string) {
	uids = []string{}
	var workgroup sync.WaitGroup
	var recvgroup sync.WaitGroup
	dbmint, dbmaxt, err := cp.GetDbTimes()
	noErr(err)
	qmint := utils.Int64Max(dbmint, args.startTime)
	qmaxt := utils.Int64Min(dbmaxt, args.endTime)
	baseOpt := copyer.NewCopyOpt(qmint, qmaxt, args.blockSplit, args.timeSplit, args.targetDir, args.allowOutOfOrder, args.labelAppends, args.labelMatchers)
	req := make(chan *copyer.CopyOpt)
	resp := make(chan *copyer.CopyResp)
	workgroup.Add(args.multiThread)
	for i := 0; i < args.multiThread; i++ {
		go cp.MultiThreadCopyer(&workgroup, req, resp)
	}
	recvgroup.Add(1)
	go func() {
		for r := range resp {
			noErr(r.Err)
			uids = append(uids, r.Uid)
			samCount += r.SamCount
			serCount += r.SerCount
		}
		recvgroup.Done()
	}()
	for r := range baseOpt.Range(args.blockSplit) {
		opt := baseOpt.Copy(r[0], r[1])
		mintStr, maxtStr := opt.ShowTime()
		fmt.Println("发布拷贝", mintStr, "到", maxtStr, "数据的任务")
		req <- opt
	}
	close(req)
	workgroup.Wait()
	close(resp)
	recvgroup.Wait()
	return
}

func singleMain(cp copyer.Copyer) (samCount, serCount uint64, uids []string) {
	uids = []string{}
	dbmint, dbmaxt, err := cp.GetDbTimes()
	noErr(err)
	qmint := utils.Int64Max(dbmint, args.startTime)
	qmaxt := utils.Int64Min(dbmaxt, args.endTime)
	baseOpt := copyer.NewCopyOpt(qmint, qmaxt, args.blockSplit, args.timeSplit, args.targetDir, args.allowOutOfOrder, args.labelAppends, args.labelMatchers)
	mintStr, maxtStr := baseOpt.ShowTime()
	fmt.Println("开始从", args.source, "拷贝", mintStr, "到", maxtStr, "的数据到", args.targetDir)
	for r := range baseOpt.Range(args.blockSplit) {
		opt := baseOpt.Copy(r[0], r[1])
		uid, sac, sec, err := cp.CopyPerTime(opt)
		noErr(err)
		uids = append(uids, uid)
		samCount += sac
		serCount += sec
		if sac == 0 {
			fmt.Println(mintStr, "到", maxtStr, "共查询到", sac, "条数据")
			continue
		}
	}
	return
}

func verify(targetDir string, uids uidChecker) (samCount, serCount uint64) {
	db, err := tsdb.OpenDBReadOnly(targetDir, nil)
	noErr(err)
	defer db.Close()
	bs, err := db.Blocks()
	noErr(err)
	for _, b := range bs {
		metadata := b.Meta()
		if !uids.Has(metadata.ULID.String()) {
			continue
		}
		fmt.Println("块", metadata.ULID, "有", metadata.Stats.NumSamples, "条指标", metadata.Stats.NumSeries, "个序列")
		samCount += metadata.Stats.NumSamples
		serCount += metadata.Stats.NumSeries
	}
	return
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
	var origSamCount, origSerCount uint64
	var newSamCount, newSerCount uint64
	var uids []string
	var cp copyer.Copyer
	if strings.HasPrefix(args.source, "http://") || strings.HasPrefix(args.source, "https://") {
		cp = remote.MustNewRemoteCopyer(args.source, args.targetDir, args.labelMatchers)
	} else {
		cp = local.MustNewLocalCopyer(args.source)
	}
	if args.multiThread > 0 {
		origSamCount, origSerCount, uids = multiThreadMain(cp)
	} else {
		origSamCount, origSerCount, uids = singleMain(cp)
	}
	if args.verifyCount {
		newSamCount, newSerCount = verify(args.targetDir, uids)
		if origSamCount != newSamCount {
			panic(fmt.Errorf("复制结果与原始指标数不一致！ %d != %d", origSamCount, newSamCount))
		}
		if origSerCount != newSerCount {
			panic(fmt.Errorf("复制结果与原始序列数不一致！ %d != %d", origSerCount, newSerCount))
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
