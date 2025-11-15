package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	timeZlayout = "2006-01-02 15:04:05.999Z07:00"
	timelayout  = "2006-01-02 15:04:05.999"
)

type commandFlags struct {
	// 源目录
	sourceDir string
	// 目标目录
	targetDir string
	// 查询数据的起始时间 2006-01-02 15:04:05.999Z
	fromTimeStr string
	// 查询数据的结束时间 格式 2006-01-02 15:04:05.999Z
	toTimeStr string
	// TSDB数据查询间隔
	queryDuration time.Duration
	// TSDB数据分块间隔
	blockDuration time.Duration
	// 并发线程数
	writeThread int
	// 查询语句的label
	matchLabels []string
	// 追加到序列的Label
	// 注意：不会检查label是否存在
	appendLabels map[string]string
	// 写入完成后手动执行GC
	gcAfterFlush bool
	gcPreSeries  uint64
	logDebug     bool
	showMetrics  bool

	// 查询数据的起始时间
	from time.Time
	// 查询数据的结束时间
	to time.Time
	// 查询条件
	labelMatchers []*labels.Matcher
}

var (
	args   commandFlags
	logger *slog.Logger
	reg    *prometheus.Registry

	version, buildDate, commit, goVersion, gitBranch string
)

func getVersionStr() string {
	return fmt.Sprintf(
		"%s, version %s (branch: %s, revision: %s)\n  build date:\t%s\n  go version:\t%s\n  platform:\t%s/%s",
		"prom-tsdb-copyer", version, gitBranch, commit, buildDate, goVersion, runtime.GOOS, runtime.GOARCH,
	)
}

func tryParseTimeWithZone(timeStr string) (time.Time, error) {
	if t, err := time.ParseInLocation(timeZlayout, timeStr, time.Local); err == nil {
		return t, nil
	}
	return time.ParseInLocation(timelayout, timeStr, time.Local)
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
	} else {
		s = fmt.Sprint("__name__=", s)
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

func parseArgs() string {
	app := kingpin.New("prom-tsdb-copyer", "普罗米修斯TSDB复制器")
	app.HelpFlag.Short('h')
	app.Version(getVersionStr()).VersionFlag.Short('v')
	app.Arg("source", "源TSDB文件夹").Required().StringVar(&args.sourceDir)
	app.Arg("target", "目标TSDB文件夹").Required().StringVar(&args.targetDir)
	app.Flag("from", "数据开始时间").Envar("COPYER_FROM_TIME").StringVar(&args.fromTimeStr)
	app.Flag("to", "数据结束时间").Envar("COPYER_TO_TIME").StringVar(&args.toTimeStr)
	app.Flag("query-duration", "切分查询的时长（增加这个值会加大内存使用）").Short('S').Envar("COPYER_QUERY_DURATION").Default("2h").DurationVar(&args.queryDuration)
	app.Flag("block-duration", "切分新块的时长").Short('B').Envar("COPYER_BLOCK_DURATION").Default("24h").DurationVar(&args.blockDuration)
	app.Flag("thread", "每个新块并行多少个查询（内存使用翻倍）(0=不限制)").Envar("COPYER_THREAD").Short('T').Default("1").IntVar(&args.writeThread)
	app.Flag("label-query", "查询label（k=v）").Short('l').Envar("COPYER_LABEL_QUERY").StringsVar(&args.matchLabels)
	app.Flag("label-append", "增加label（k=v）").Short('L').Envar("COPYER_LABEL_APPEND").StringMapVar(&args.appendLabels)
	app.Flag("gc-pre-series", "写入多少序列后GC（0=关闭）").Envar("COPYER_GC_PRE_SERIES").Uint64Var(&args.gcPreSeries)
	app.Flag("debug", "输出Debug日志到终端").Short('d').Envar("COPYER_DEBUG").BoolVar(&args.logDebug)
	app.Flag("show-metrics", "输出监控指标到终端").Envar("COPYER_SHOW_METRICS").BoolVar(&args.showMetrics)
	app.Flag("gc-after-flush", "写入完成后手动GC").Envar("COPYER_GC_AFTER_FLUSH").BoolVar(&args.gcAfterFlush)

	command := kingpin.MustParse(app.Parse(os.Args[1:]))
	if args.logDebug {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	if len(args.fromTimeStr) > 0 {
		args.from = Must(tryParseTimeWithZone(args.fromTimeStr))
	}
	if len(args.toTimeStr) > 0 {
		args.to = Must(tryParseTimeWithZone(args.toTimeStr))
	}

	if mls := len(args.matchLabels); mls > 0 {
		fmt.Print("使用查询语句: {")
		args.labelMatchers = make([]*labels.Matcher, mls)
		for idx, matchStr := range args.matchLabels {
			args.labelMatchers[idx] = labels.MustNewMatcher(mustParseMatchKV(matchStr))
			fmt.Print(args.labelMatchers[idx].String())
			fmt.Print(",")
		}
		fmt.Print("}\n")
	} else {
		fmt.Print("使用查询语句: {")
		args.labelMatchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "__name__", "")}
		fmt.Print(args.labelMatchers[0].String())
		fmt.Print("}\n")
	}
	if len(args.appendLabels) > 0 {
		fmt.Println("将增加label:", labels.FromMap(args.appendLabels).String())
	}
	os.MkdirAll(args.targetDir, 0o755)
	os.Setenv("TMPDIR", args.targetDir)
	return command
}

func initMertics() {
	reg = prometheus.NewRegistry()
	reg.MustRegister(metricCopySeries, metricDbFlushTime)
}

func doFakeHttpRequest(handler http.Handler) {
	s := httptest.NewServer(handler)
	defer s.Close()
	resp, err := s.Client().Get(s.URL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	io.CopyBuffer(os.Stdout, resp.Body, make([]byte, 65535))
}

func main() {
	parseArgs()
	debug.SetGCPercent(75)
	initMertics()
	initCompactor()
	doCopy()
	if args.showMetrics {
		h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
		doFakeHttpRequest(h)
	}
}
