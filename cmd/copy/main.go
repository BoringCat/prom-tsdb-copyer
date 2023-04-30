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
	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/panjf2000/ants/v2"
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

func compactFn(dstDir, tenant string, ulids []string, wg *sync.WaitGroup, m *sync.Map) func() {
	td := path.Join(dstDir, tenant)
	return func() {
		defer wg.Done()
		cuids, err := compacter.CompactByUlids(td, td, ulids, true, args.blockSplit)
		noErr(err)
		m.Store(tenant, cuids)
	}
}

func compact(p *ants.Pool, dstDir string, tUlids copyer.TenantResults) (uids map[string]uidChecker) {
	uids = map[string]uidChecker{}
	if args.blockSplit == tsdb.DefaultBlockDuration {
		for k, v := range tUlids.ToMap() {
			uids[k] = v
		}
	} else {
		var wg sync.WaitGroup
		var m sync.Map
		for tenant, ulids := range tUlids.ToMap() {
			wg.Add(1)
			p.Submit(compactFn(dstDir, tenant, ulids, &wg, &m))
		}
		wg.Wait()
		m.Range(func(key, value any) bool {
			uids[key.(string)] = value.([]string)
			return true
		})
	}
	return
}

func poolMain(cp *copyer.Copyer) (samCount uint64, uids map[string]uidChecker) {
	var wg sync.WaitGroup
	var rg sync.WaitGroup
	// Appender Pool
	wp, err := ants.NewPool(args.writeThread, ants.WithPreAlloc(args.writeThread > 0))
	noErr(err)
	// Flush Pool
	fp, err := ants.NewPool(0)
	noErr(err)
	dbmint, dbmaxt := cp.GetDbTimes()
	noErr(err)
	qmint := utils.Int64Max(dbmint, args.startTime)
	qmaxt := utils.Int64Min(dbmaxt, args.endTime)
	baseOpt := copyer.CopyOpt{
		Mint: qmint, Maxt: qmaxt, ManualGC: args.manualGC, JobPool: wp, FlushPool: fp, FlushGroup: &wg,
		BlockSplit: args.blockSplit, TimeSplit: args.timeSplit, CommitCount: args.commitCount,
		TargetDir: args.targetDir, LabelAppends: args.labelAppends, WriteThread: args.writeThread,
		LabelMatchers: args.labelMatchers, TenantLabel: args.tenantLabel, DefaultTenant: args.defaultTenant,
	}
	mintStr, maxtStr := baseOpt.ShowTime()
	fmt.Println("开始从", args.source, "拷贝", mintStr, "到", maxtStr, "的数据到", args.targetDir)

	ch := make(chan *copyer.TenantResult, args.writeThread)
	tUlids := copyer.TenantResults{}
	rg.Add(1)
	go func() {
		defer rg.Done()
		for r := range ch {
			tUlids = append(tUlids, r)
			fmt.Println(r.Show())
			samCount += r.SamCount
		}
	}()
	for r := range baseOpt.BlockRange(0) {
		opt := baseOpt.Copy(r[0], r[1])
		noErr(cp.CopyPerBlock(opt, ch))
		if args.waitEachBlock {
			wg.Wait()
		}
	}
	wg.Wait()
	close(ch)
	rg.Wait()
	fp.Release()
	uids = compact(wp, baseOpt.TargetDir, tUlids)
	wp.Release()
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
	var newSamCount uint64
	var cp *copyer.Copyer
	if strings.HasPrefix(args.source, "http://") || strings.HasPrefix(args.source, "https://") {
		if len(args.tenantLabel) > 0 && (args.labelApi == nil || len(args.labelApi.String()) == 0) {
			panic("当使用RemoteRead，并且需要区分租户时，LabelApi地址不能为空")
		}
		cp = copyer.MustNewCopyer(copyer.NewRemoteCopyer(args.source, args.labelApi))
	} else {
		cp = copyer.MustNewCopyer(copyer.NewLocalCopyer(args.source))
	}
	origSamCount, uids := poolMain(cp)
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
