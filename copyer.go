package main

import (
	"context"
	"iter"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var (
	ErrUnsupportValueType = errors.New("不支持的数据格式")
	metricCopySeries      = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "prom_tsdb",
		Subsystem: "copyer",
		Name:      "series_copy_seconds",
		Buckets:   []float64{0.001, 0.002, 0.004, 0.008, 0.016, 0.064, 0.256, 1.024},
	})
	metricDbFlushTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "prom_tsdb",
		Subsystem: "copyer",
		Name:      "db_flush_seconds",
		Buckets:   prometheus.DefBuckets,
	})
)

func getQueryRange(from, to time.Time, d time.Duration) iter.Seq2[time.Time, time.Time] {
	if d == 0 {
		return func(yield func(time.Time, time.Time) bool) {
			yield(from, to)
		}
	}
	// 按间隔取整时间
	stepFromTime := time.UnixMilli(d.Milliseconds() * (from.UnixMilli() / d.Milliseconds()))
	stepToTime := stepFromTime.Add(d).Add(-time.Millisecond)
	return func(yield func(time.Time, time.Time) bool) {
		for stepFromTime.Before(to) || stepFromTime.Equal(to) {
			// 返回符合时间区间的时间
			if !yield(timeMax(stepFromTime, from), timeMin(stepToTime, to)) {
				return
			}
			// 加一个间隔
			stepFromTime = stepFromTime.Add(d)
			stepToTime = stepToTime.Add(d)
		}
	}
}

func doOneTimeCopy(ctx context.Context, wg *sync.WaitGroup, p Pool, from, to time.Time, d time.Duration, ch chan<- ulid.ULID) {
	defer wg.Done()
	db, err := tsdb.OpenDBReadOnly(args.sourceDir, os.TempDir(), logger.With("logger", "source-tsdb"))
	if err != nil {
		logger.Error("打开源TSDB失败", "err", err, "source", args.sourceDir)
		ch <- ulid.Zero
		return
	}
	defer db.Close()
	logger := logger.With("from", from, "to", to)
	if !p.Get(ctx) {
		logger.Error("无法获取到并发额度")
		ch <- ulid.Zero
		return
	}
	defer p.Put()
	logger.Info("按时间片进行查询")
	reader, err := db.Querier(from.UnixMilli(), to.UnixMilli())
	if err != nil {
		logger.Error("查询源TSDB失败", "err", err)
		ch <- ulid.Zero
		return
	}
	defer reader.Close()
	writer, err := tsdb.NewBlockWriter(logger.With("logger", "target-tsdb"), args.targetDir, d.Milliseconds()*2)
	if err != nil {
		logger.Error("打开新TSDB块失败", "err", err, "target", args.targetDir)
		ch <- ulid.Zero
		return
	}
	defer writer.Close()
	var iter chunkenc.Iterator
	var count uint64 = 0
	seriesSet := reader.Select(ctx, false, nil, args.labelMatchers...)
	for seriesSet.Next() {
		count++
		seriesBegin := time.Now()
		series := seriesSet.At()
		var ref storage.SeriesRef = 0
		iter = series.Iterator(iter)
		lbs := series.Labels()
		if len(args.appendLabels) > 0 {
			builder := labels.NewBuilder(lbs)
			for k, v := range args.appendLabels {
				builder.Set(k, v)
			}
			lbs = builder.Labels()
		}
		appender := writer.Appender(ctx)
		hasWrite := false
		for {
			valueType := iter.Next()
			if valueType == chunkenc.ValNone {
				break
			}
			switch valueType {
			case chunkenc.ValFloat:
				t, v := iter.At()
				ref, err = appender.Append(ref, lbs, t, v)
			case chunkenc.ValFloatHistogram:
				t, v := iter.AtFloatHistogram(nil)
				ref, err = appender.AppendHistogram(ref, lbs, t, nil, v)
			case chunkenc.ValHistogram:
				t, v := iter.AtHistogram(nil)
				ref, err = appender.AppendHistogram(ref, lbs, t, v, nil)
			default:
				logger.Error("不支持的数据格式", "valueType", valueType, "lbs", lbs.String())
				ch <- ulid.Zero
				return
			}

			if err != nil {
				logger.Error("写入新TSDB失败", "err", err, "lbs", lbs.String())
				ch <- ulid.Zero
				return
			}
			hasWrite = true
		}

		if err := iter.Err(); err != nil {
			logger.Error("读取源TSDB序列失败", "err", err, "lbs", lbs.String())
			ch <- ulid.Zero
			return
		}
		if !hasWrite {
			logger.Warn("没有数据写入", "lbs", lbs.String())
			continue
		}
		if err := appender.Commit(); err != nil {
			logger.Error("提交新TSDB失败", "err", err, "lbs", lbs.String())
			ch <- ulid.Zero
			return
		}
		metricCopySeries.Observe(time.Since(seriesBegin).Seconds())
		if args.gcPreSeries > 0 && count%args.gcPreSeries == 0 {
			logger.Debug("runtime.GC")
			runtime.GC()
		}
	}
	flushBegin := time.Now()
	bid, err := writer.Flush(ctx)
	if err != nil {
		logger.Error("保存新TSDB块失败", "err", err)
		ch <- ulid.Zero
		return
	}
	metricDbFlushTime.Observe(time.Since(flushBegin).Seconds())
	if args.gcAfterFlush {
		logger.Debug("runtime.GC")
		runtime.GC()
	}
	logger.Info("时间区间复制完成", "from", from, "to", to, "blockId", bid)
	ch <- bid
}

func doOneBlockCopy(ctx context.Context, p Pool, from, to time.Time) ([]ulid.ULID, error) {
	var wg sync.WaitGroup
	ch := make(chan ulid.ULID, args.writeThread)
	bids := []ulid.ULID{}
	for from, to := range getQueryRange(from, to, args.queryDuration) {
		wg.Add(1)
		go doOneTimeCopy(ctx, &wg, p, from, to, args.queryDuration, ch)
	}
	go func(wg *sync.WaitGroup, ch chan ulid.ULID) {
		wg.Wait()
		close(ch)
	}(&wg, ch)
	for bid := range ch {
		if !bid.IsZero() {
			bids = append(bids, bid)
		} else {
			logger.Warn("获取到空blockId")
		}
	}
	return bids, nil
	// bid, err := doCompact(bids)
	// if err != nil {
	// 	logger.Error("压缩TSDB块失败", "err", err, "target", args.targetDir)
	// 	return err
	// }
	// logger.Info("时间块复制并压缩完成", "from", from, "to", to, "blockId", bid)
	// return nil
}

func getTimeRangeFromTSDB() (from time.Time, to time.Time, err error) {
	db, err := tsdb.OpenDBReadOnly(args.sourceDir, "", logger.With("logger", "source-tsdb"))
	if err != nil {
		err = errors.Wrap(err, "打开源TSDB失败")
		return
	}
	defer db.Close()
	blocks, err := db.Blocks()
	if err != nil {
		err = errors.Wrap(err, "读取源TSDB块失败")
		return
	}
	from, to = time.UnixMilli(blocks[0].Meta().MinTime), time.UnixMilli(blocks[0].Meta().MaxTime)
	for _, block := range blocks {
		meta := block.Meta()
		from = timeMin(from, time.UnixMilli(meta.MinTime))
		to = timeMax(to, time.UnixMilli(meta.MaxTime))
	}
	logger.Debug("从源TSDB获取到时间区间", "from", from, "to", to)
	return
}

func doCopy() error {
	p := newJobPool(args.writeThread)
	realFrom, realTo := args.from, args.to
	if realFrom.IsZero() || realTo.IsZero() {
		var err error
		realFrom, realTo, err = getTimeRangeFromTSDB()
		if err != nil {
			return errors.Wrap(err, "从TSDB获取时间区间失败")
		}
	}
	compatorJobs := [][]ulid.ULID{}
	for from, to := range getQueryRange(realFrom, realTo, args.blockDuration) {
		logger.Debug("切分时间区间", "from", from, "to", to)
		if !from.Before(to) {
			break
		}
		bids, err := doOneBlockCopy(context.TODO(), p, from, to)
		if err != nil {
			return err
		}
		compatorJobs = append(compatorJobs, bids)
	}
	for _, bids := range compatorJobs {
		_, err := doCompact(bids)
		if err != nil {
			logger.Error("压缩TSDB块失败", "err", err, "bids", bids)
			return err
		}
	}
	return nil
}
