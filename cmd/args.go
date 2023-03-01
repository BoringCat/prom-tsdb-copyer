package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	timeZlayout = "2006-01-02 15:04:05.999Z07:00"
	timelayout  = "2006-01-02 15:04:05.999"
)

type cmdArgs struct {
	// 数据源，可以是TSDB目录或者是RemoteRead地址
	source string
	// 目标目录
	targetDir string
	// 查询数据的起始时间 2006-01-02 15:04:05.999Z
	startTimeStr string
	// 查询数据的结束时间 格式 2006-01-02 15:04:05.999Z
	endTimeStr string
	// 查询数据分段间隔
	queryTimeSplit time.Duration
	// TSDB数据分块间隔
	blockTimeSplit time.Duration
	// 是否验证序列、样本数
	verifyCount bool
	// 是否开启乱序写入支持
	allowOutOfOrder      bool
	appendThanosMetadata bool
	// 并发线程数
	multiThread int
	// 查询语句的label
	matchLabels []string
	// 追加到序列的Label
	// 注意：不会检查label是否存在
	appendLabels map[string]string

	// 起始时间（毫秒），由 startTimeStr 解析得来
	startTime int64
	// 结束时间（毫秒），由 startTimeStr 解析得来
	endTime int64
	// 数据分段间隔（毫秒），由 queryTimeSplit 解析得来（默认3600000）
	timeSplit int64 // = 3600000
	// TSDB分块间隔（毫秒），由 blockTimeSplit 解析得来（默认7200000）
	blockSplit int64 // = 86400000
	// 查询条件
	labelMatchers []*labels.Matcher // = []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "", ".*")}
	// 追加的Labe，由 appendLabels 解析得来
	labelAppends labels.Labels
}

func tryParseTimeWithZone(timeStr string) (time.Time, error) {
	if t, err := time.ParseInLocation(timeZlayout, timeStr, time.Local); err == nil {
		return t, nil
	}
	return time.ParseInLocation(timelayout, timeStr, time.Local)
}

func (c *cmdArgs) ParseArgs() (err error) {
	if t, err := tryParseTimeWithZone(c.startTimeStr); err != nil {
		return err
	} else {
		c.startTime = t.UnixMilli()
	}
	if t, err := tryParseTimeWithZone(c.endTimeStr); err != nil {
		return err
	} else {
		c.endTime = t.UnixMilli()
	}
	if c.multiThread == 0 {
		c.multiThread = runtime.GOMAXPROCS(0)
	}
	c.timeSplit = int64(c.queryTimeSplit / time.Millisecond)
	c.blockSplit = int64(c.blockTimeSplit / time.Millisecond)
	if mls := len(c.matchLabels); mls > 0 {
		fmt.Print("使用查询语句: {")
		c.labelMatchers = make([]*labels.Matcher, mls)
		for idx, matchStr := range c.matchLabels {
			c.labelMatchers[idx] = labels.MustNewMatcher(mustParseMatchKV(matchStr))
			fmt.Print(c.labelMatchers[idx].String())
			fmt.Print(",")
		}
		fmt.Print("}\n")
	}
	if len(c.appendLabels) > 0 {
		c.labelAppends = labels.FromMap(c.appendLabels)
		fmt.Println("将增加label:", c.labelAppends.String())
	} else if args.appendThanosMetadata {
		noErr(fmt.Errorf("追加Thanos的元数据时必须添加额外的Label"))
	}
	return nil
}

func mustParseMatchKV(s string) (mt labels.MatchType, name string, val string) {
	var sep string
	if strings.Contains(s, "!=") {
		mt = labels.MatchNotEqual
		sep = "!="
	} else if strings.Contains(s, "=~") {
		mt = labels.MatchRegexp
		sep = "=~"
	} else if strings.Contains(s, "!~") {
		mt = labels.MatchNotRegexp
		sep = "!~"
	} else if strings.Contains(s, "=") {
		mt = labels.MatchEqual
		sep = "="
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

var (
	args = cmdArgs{
		matchLabels:   []string{},
		appendLabels:  map[string]string{},
		labelMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "", ".*")},
		timeSplit:     int64(time.Hour / time.Millisecond),
		blockSplit:    int64(24 * time.Hour / time.Millisecond),
	}
)

func parseArgs() {
	app := kingpin.New("prom-tsdb-copyer", "普罗米修斯TSDB复制器")
	app.HelpFlag.Short('h')
	app.Arg("from", "源TSDB文件夹/Remote Read 地址").Required().StringVar(&args.source)
	app.Arg("toDir", "目标TSDB文件夹").Required().ExistingDirVar(&args.targetDir)
	app.Flag("start-time", "数据开始时间").Short('S').Required().StringVar(&args.startTimeStr)
	app.Flag("end-time", "数据结束时间").Short('E').Required().StringVar(&args.endTimeStr)
	app.Flag("query-split", "切分查询的时长").Short('Q').Default("1h").DurationVar(&args.queryTimeSplit)
	app.Flag("block-split", "切分新块的时长").Short('B').Default("24h").DurationVar(&args.blockTimeSplit)
	app.Flag("verify", "是否验证数据量").BoolVar(&args.verifyCount)
	app.Flag("allow-out-of-order", "是否允许乱序写入").BoolVar(&args.allowOutOfOrder)
	app.Flag("thanos-metadata", "是否追加Thanos的元数据").BoolVar(&args.appendThanosMetadata)
	app.Flag("multi-thread", "并行多少个复制(0=GOMAXPROCS)").Short('T').Default("-1").IntVar(&args.multiThread)
	app.Flag("label-query", "查询label（k=v）").Short('l').StringsVar(&args.matchLabels)
	app.Flag("label-append", "增加label（k=v）").Short('L').StringMapVar(&args.appendLabels)
	app.Version(fmt.Sprintf("版本: %s-%s\n构建日期: %s", version, commit, buildDate)).VersionFlag.Short('V')

	kingpin.MustParse(app.Parse(os.Args[1:]))
	noErr(args.ParseArgs())
}
