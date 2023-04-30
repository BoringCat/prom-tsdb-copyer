package compact

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
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
	// 分组提交指标写入的数量（默认10240）
	commitCount uint64 // = 10240
	// 是否验证序列、样本数
	verifyCount bool
	// 是否开启乱序写入支持
	appendThanosMetadata bool
	// 并发线程数
	writeThread int
	// 查询语句的label
	matchLabels []string
	// 追加到序列的Label
	// 注意：不会检查label是否存在
	appendLabels map[string]string
	// 每个Block写入后执行一次GC
	manualGC bool
	// 在每个分块任务发布后等待
	waitEachBlock bool

	// 分租户label，用于区分租户
	tenantLabel string
	// 默认租户
	defaultTenant string

	// 源是RemoteRead地址时，用于获取Tenant标签值的地址
	labelApi *url.URL

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
	// 追加的Label，由 appendLabels 解析得来
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
	if c.writeThread < 0 {
		c.writeThread = 1
	}
	c.timeSplit = int64(c.queryTimeSplit / time.Millisecond)
	if c.timeSplit > tsdb.DefaultBlockDuration/2 {
		c.timeSplit = tsdb.DefaultBlockDuration / 2
	} else if !utils.Edivisible(tsdb.DefaultBlockDuration, c.timeSplit) {
		recommend := time.Duration(tsdb.DefaultBlockDuration/(tsdb.DefaultBlockDuration/c.timeSplit)) * time.Millisecond
		panic(fmt.Errorf("timeSplit 必须能被 tsdb.DefaultBlockDuration 整除，推荐 %s ", recommend))
	}
	c.blockSplit = int64(c.blockTimeSplit / time.Millisecond)
	if c.blockSplit <= 0 {
	} else if c.blockSplit < tsdb.DefaultBlockDuration {
		c.blockSplit = tsdb.DefaultBlockDuration
	} else if !utils.Edivisible(c.blockSplit, tsdb.DefaultBlockDuration) {
		recommend := (time.Duration(c.blockSplit/tsdb.DefaultBlockDuration) + 1) * time.Hour * 2
		panic(fmt.Errorf("blockSplit 必须能整除 tsdb.DefaultBlockDuration，推荐 %s ", recommend))
	}
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
		if args.appendThanosMetadata {
			fmt.Println("将在Meta中增加label:", c.labelAppends.String())
		} else {
			fmt.Println("将增加label:", c.labelAppends.String())
			c.labelAppends = labels.FromMap(c.appendLabels)
		}
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

func ParseArgs(app utils.KingPin) {
	app.Arg("from", "源TSDB文件夹/Remote Read 地址").Required().StringVar(&args.source)
	app.Arg("toDir", "目标TSDB文件夹").Required().ExistingDirVar(&args.targetDir)
	app.Flag("label-api", "远程Label API地址").Envar("COPYER_LABEL_API").URLVar(&args.labelApi)
	app.Flag("start-time", "数据开始时间").Short('S').Envar("COPYER_START_TIME").Required().StringVar(&args.startTimeStr)
	app.Flag("end-time", "数据结束时间").Short('E').Envar("COPYER_END_TIME").Required().StringVar(&args.endTimeStr)
	app.Flag("query-split", "切分查询的时长").Short('Q').Envar("COPYER_QUERY_SPLIT").Default("1h").DurationVar(&args.queryTimeSplit)
	app.Flag("block-split", "切分新块的时长").Short('B').Envar("COPYER_BLOCK_SPLIT").Default("24h").DurationVar(&args.blockTimeSplit)
	app.Flag("verify", "是否验证数据量").Envar("COPYER_VERIFY").BoolVar(&args.verifyCount)
	app.Flag("thanos-metadata", "是否追加Thanos的元数据").Envar("COPYER_THANOS_METADATA").BoolVar(&args.appendThanosMetadata)
	app.Flag("write-thread", "每个读取并行多少个写入（注意内存使用）(0=不限制)").Envar("COPYER_MULTI_THREAD").Short('T').Default("1").IntVar(&args.writeThread)
	app.Flag("label-query", "查询label（k=v）").Short('l').Envar("COPYER_LABEL_QUERY").StringsVar(&args.matchLabels)
	app.Flag("label-append", "增加label（k=v）").Short('L').Envar("COPYER_LABEL_APPEND").StringMapVar(&args.appendLabels)
	app.Flag("commit-count", "分组提交指标写入的数量").Envar("COPYER_COMMIT_COUNT").Default("10240").Uint64Var(&args.commitCount)
	app.Flag("tenant", "分租户用的Label").Envar("COPYER_TENANT_KEY").StringVar(&args.tenantLabel)
	app.Flag("default-tenant", "默认租户值").Envar("COPYER_DEFAULT_TENANT").StringVar(&args.defaultTenant)
	app.Flag("manual-gc", "Block写入后手动GC（牺牲时间减低内存消耗）").Short('G').Envar("COPYER_MANUAL_GC").BoolVar(&args.manualGC)
	app.Flag("wait", "在每个分块任务发布后等待").Short('W').Envar("COPYER_WAIT").BoolVar(&args.waitEachBlock)
}
