package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	timelayout = "2006-01-02 15:04:05.999"
)

var (
	originDir      string
	targetDir      string
	startTimeStr   string
	endTimeStr     string
	queryTimeSplit time.Duration
	blockTimeSplit time.Duration
	verifyCount    bool
	multiThread    int
	matchLabels    []string
	app            = kingpin.New("prom-tsdb-copyer", "普罗米修斯TSDB复制器")

	startTime     int64
	endTime       int64
	timeSplit     int64 = 3600000
	blockSplit    int64 = 86400000
	labelMatchers       = []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "", ".*")}
	appendLabels        = map[string]string{}
	labelAppends  labels.Labels

	version, commit, buildDate string
)

func parseArgs() {
	app.Arg("from", "源TSDB文件夹/Remote Read 地址").Required().StringVar(&originDir)
	app.Arg("toDir", "目标TSDB文件夹").Required().ExistingDirVar(&targetDir)
	app.Flag("start-time", "数据开始时间").Short('S').Required().StringVar(&startTimeStr)
	app.Flag("end-time", "数据结束时间").Short('E').Required().StringVar(&endTimeStr)
	app.Flag("query-split", "切分查询的时长").Short('Q').Default("1h").DurationVar(&queryTimeSplit)
	app.Flag("block-split", "切分新块的时长").Short('B').Default("24h").DurationVar(&blockTimeSplit)
	app.Flag("verify", "是否验证数据量").BoolVar(&verifyCount)
	app.Flag("multi-thread", "并行多少个复制(0=GOMAXPROCS)").Short('T').Default("-1").IntVar(&multiThread)
	app.Flag("label-query", "查询label（k=v）").Short('l').StringsVar(&matchLabels)
	app.Flag("label-append", "增加label（k=v）").Short('L').StringMapVar(&appendLabels)
	app.Version(fmt.Sprintf("版本: %s-%s\n构建日期: %s", version, commit, buildDate))

	kingpin.MustParse(app.Parse(os.Args[1:]))

	st, err := time.ParseInLocation(timelayout, startTimeStr, time.Local)
	noErr(err)
	startTime = st.UnixMilli()
	et, err := time.ParseInLocation(timelayout, endTimeStr, time.Local)
	noErr(err)
	endTime = et.UnixMilli()

	if multiThread == 0 {
		multiThread = runtime.GOMAXPROCS(0)
	}
	if queryTimeSplit > time.Hour {
		queryTimeSplit = time.Hour
	}
	timeSplit = int64(queryTimeSplit / time.Millisecond)
	blockSplit = int64(blockTimeSplit / time.Millisecond)
	if mls := len(matchLabels); mls > 0 {
		fmt.Print("使用查询语句: {")
		labelMatchers = make([]*labels.Matcher, mls)
		for idx, matchStr := range matchLabels {
			labelMatchers[idx] = labels.MustNewMatcher(mustParseMatchKV(matchStr))
			fmt.Print(labelMatchers[idx].String())
			fmt.Print(",")
		}
		fmt.Print("}\n")
	}
	if len(appendLabels) > 0 {
		labelAppends = labels.FromMap(appendLabels)
		fmt.Println("将增加label:", labelAppends.String())
	}
}

func mustParseMatchKV(s string) (mt labels.MatchType, name string, val string) {
	var sep string
	if strings.Contains(s, "=") {
		mt = labels.MatchEqual
		sep = "="
	} else if strings.Contains(s, "!=") {
		mt = labels.MatchNotEqual
		sep = "!="
	} else if strings.Contains(s, "=~") {
		mt = labels.MatchRegexp
		sep = "=~"
	} else if strings.Contains(s, "!~") {
		mt = labels.MatchNotRegexp
		sep = "!~"
	}
	if len(sep) == 0 {
		panic(fmt.Errorf("非法的label表达式: %s", s))
	}
	fields := strings.Split(s, sep)
	if len(fields) < 2 {
		panic(fmt.Errorf("非法的label表达式: %s", s))
	}
	name = fields[0]
	val = strings.Join(fields[1:], sep)
	return
}

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

type copyer interface {
	copyPerTime(opt *copyOpt) (count uint64, err error)
	multiThreadCopyer(wg *sync.WaitGroup, req <-chan *copyOpt, resp chan<- *copyResp)
	getDbTimes() (mint int64, maxt int64, err error)
}

type copyOpt struct {
	mint, maxt int64
}

type copyResp struct {
	count uint64
	err   error
}

func multiThreadMain(cp copyer) uint64 {
	var copyCount uint64 = 0
	var workgroup sync.WaitGroup
	var recvgroup sync.WaitGroup
	dbmint, dbmaxt, err := cp.getDbTimes()
	noErr(err)
	qmint := int64Max(dbmint, startTime)
	qmaxt := int64Min(dbmaxt, endTime)
	req := make(chan *copyOpt)
	resp := make(chan *copyResp)
	for i := 0; i < multiThread; i++ {
		workgroup.Add(1)
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
	for mint := qmint; mint < qmaxt; mint += blockSplit {
		maxt := int64Min(mint+blockSplit, qmaxt)
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
	qmint := int64Max(dbmint, startTime)
	qmaxt := int64Min(dbmaxt, endTime)
	for mint := qmint; mint < qmaxt; mint += blockSplit {
		maxt := int64Min(mint+blockSplit, qmaxt)
		c, err := cp.copyPerTime(&copyOpt{mint, maxt})
		noErr(err)
		copyCount += c
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

func main() {
	parseArgs()
	var copyCount uint64 = 0
	var existsCount uint64 = 0
	var cp copyer
	if strings.HasPrefix(originDir, "http://") || strings.HasPrefix(originDir, "https://") {
		cp = MustNewRemoteCopyer(originDir, targetDir)
	} else {
		cp = &localCopyer{originDir, targetDir}
	}
	if multiThread > 0 {
		copyCount = multiThreadMain(cp)
	} else {
		copyCount = singleMain(cp)
	}
	if verifyCount {
		existsCount = verify(targetDir)
		if existsCount != copyCount {
			panic(fmt.Errorf("复制结果与原始 append 数不一致！ %d != %d", copyCount, existsCount))
		}
		fmt.Println("校验完成")
	}
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}
