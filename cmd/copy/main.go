package compact

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/boringcat/prom-tsdb-copyer/compacter"
	"github.com/boringcat/prom-tsdb-copyer/copyer"
	"github.com/boringcat/prom-tsdb-copyer/copyer/local"
	"github.com/boringcat/prom-tsdb-copyer/copyer/remote"
	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/go-kit/log"
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

func compact(dstDir string, tUlids copyer.TenantUlids) (uids map[string]uidChecker) {
	uids = map[string]uidChecker{}
	if args.blockSplit == tsdb.DefaultBlockDuration {
		for k, v := range tUlids.ToMap() {
			uids[k] = v
		}
	} else {
		for tenant := range tUlids.ToMap() {
			td := path.Join(dstDir, tenant)
			cuids, err := compacter.Compact(td, td, true, args.blockSplit)
			noErr(err)
			uids[tenant] = cuids
		}
	}
	return
}

func multiThreadMain(cp copyer.Copyer) (samCount uint64, uids map[string]uidChecker) {
	var workgroup sync.WaitGroup
	var recvgroup sync.WaitGroup
	dbmint, dbmaxt, err := cp.GetDbTimes()
	noErr(err)
	qmint := utils.Int64Max(dbmint, args.startTime)
	qmaxt := utils.Int64Min(dbmaxt, args.endTime)
	baseOpt := copyer.CopyOpt{
		Mint: qmint, Maxt: qmaxt,
		BlockSplit: args.blockSplit, TimeSplit: args.timeSplit, CommitCount: args.commitCount,
		TargetDir: args.targetDir, LabelAppends: args.labelAppends,
		LabelMatchers: args.labelMatchers, TenantLabel: args.tenantLabel, DefaultTenant: args.defaultTenant,
	}
	req := make(chan *copyer.CopyOpt)
	resp := make(chan *copyer.CopyResp)
	workgroup.Add(args.multiThread)
	for i := 0; i < args.multiThread; i++ {
		go cp.MultiThreadCopyer(&workgroup, req, resp)
	}
	recvgroup.Add(1)
	tUlids := copyer.TenantUlids{}
	go func() {
		for r := range resp {
			noErr(r.Err)
			tUlids = append(tUlids, r.Uids...)
			samCount += r.SamCount
		}
		recvgroup.Done()
	}()
	for r := range baseOpt.BlockRange(0) {
		opt := baseOpt.Copy(r[0], r[1])
		mintStr, maxtStr := opt.ShowTime()
		req <- opt
		fmt.Println("发布拷贝", mintStr, "到", maxtStr, "数据的任务")
	}
	close(req)
	workgroup.Wait()
	close(resp)
	recvgroup.Wait()
	uids = compact(baseOpt.TargetDir, tUlids)
	return
}

func singleMain(cp copyer.Copyer) (samCount uint64, uids map[string]uidChecker) {
	dbmint, dbmaxt, err := cp.GetDbTimes()
	noErr(err)
	qmint := utils.Int64Max(dbmint, args.startTime)
	qmaxt := utils.Int64Min(dbmaxt, args.endTime)
	baseOpt := copyer.CopyOpt{
		Mint: qmint, Maxt: qmaxt,
		BlockSplit: args.blockSplit, TimeSplit: args.timeSplit, CommitCount: args.commitCount,
		TargetDir: args.targetDir, LabelAppends: args.labelAppends,
		LabelMatchers: args.labelMatchers, TenantLabel: args.tenantLabel, DefaultTenant: args.defaultTenant,
	}
	mintStr, maxtStr := baseOpt.ShowTime()
	fmt.Println("开始从", args.source, "拷贝", mintStr, "到", maxtStr, "的数据到", args.targetDir)
	tUlids := copyer.TenantUlids{}
	for r := range baseOpt.BlockRange(0) {
		opt := baseOpt.Copy(r[0], r[1])
		uid, sac, err := cp.CopyPerBlock(opt)
		noErr(err)
		tUlids = append(tUlids, uid...)
		samCount += sac
		if sac == 0 {
			fmt.Println(mintStr, "到", maxtStr, "共查询到", sac, "条数据")
			continue
		}
	}
	uids = compact(baseOpt.TargetDir, tUlids)
	return
}

func verify(targetDir string, uidmap map[string]uidChecker) (samCount uint64) {
	for tenant, uids := range uidmap {
		td := path.Join(targetDir, tenant)
		db, err := tsdb.OpenDBReadOnly(td, nil)
		noErr(err)
		defer db.Close()
		bs, err := db.Blocks()
		noErr(err)
		for _, b := range bs {
			metadata := b.Meta()
			if !uids.Has(metadata.ULID.String()) {
				continue
			}
			fmt.Println(tenant, "块", metadata.ULID, "有", metadata.Stats.NumSamples, "条指标", metadata.Stats.NumSeries, "个序列")
			samCount += metadata.Stats.NumSamples
		}
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

func Main() {
	noErr(args.ParseArgs())
	os.Setenv("TMPDIR", args.targetDir)
	compacter.SetLogger(log.WithPrefix(log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout)), "ts", log.DefaultTimestamp))
	var origSamCount, newSamCount uint64
	var uids map[string]uidChecker
	var cp copyer.Copyer
	if strings.HasPrefix(args.source, "http://") || strings.HasPrefix(args.source, "https://") {
		cp = remote.MustNewRemoteCopyer(args.source, args.targetDir, args.labelMatchers)
	} else {
		cp = local.MustNewLocalCopyer(args.source)
	}
	if args.multiThread > 0 {
		origSamCount, uids = multiThreadMain(cp)
	} else {
		origSamCount, uids = singleMain(cp)
	}
	if args.verifyCount {
		newSamCount = verify(args.targetDir, uids)
		if origSamCount != newSamCount {
			panic(fmt.Errorf("复制结果与原始指标数不一致！ %d != %d", origSamCount, newSamCount))
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
