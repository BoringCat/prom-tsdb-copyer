package compact

import (
	"fmt"
	"time"

	"github.com/boringcat/prom-tsdb-copyer/utils"
	"github.com/prometheus/prometheus/tsdb"
)

type cmdArgs struct {
	// TSDB目录列表
	sourceDirs []string
	// TSDB数据分块间隔
	blockTimeSplit time.Duration
	// 并发线程数
	multiThread int

	// TSDB分块间隔（毫秒），由 blockTimeSplit 解析得来（默认7200000）
	blockSplit int64 // = 86400000
}

func (c *cmdArgs) ParseArgs() (err error) {
	c.blockSplit = int64(c.blockTimeSplit / time.Millisecond)
	if c.blockSplit <= 0 {
	} else if c.blockSplit < tsdb.DefaultBlockDuration {
		c.blockSplit = tsdb.DefaultBlockDuration
	} else {
		fnum := float64(c.blockSplit) / float64(tsdb.DefaultBlockDuration)
		inum := float64(c.blockSplit / tsdb.DefaultBlockDuration)
		if fnum != inum {
			panic(fmt.Sprintf("blockSplit 必须为 %dh 的倍数", tsdb.DefaultBlockDuration/int64(time.Hour/time.Millisecond)))
		}
	}
	return nil
}

var (
	args = cmdArgs{
		blockSplit: int64(24 * time.Hour / time.Millisecond),
	}
)

func ParseArgs(app utils.KingPin) {
	app.Arg("dirs", "源TSDB文件夹/Remote Read 地址").Required().ExistingDirsVar(&args.sourceDirs)
	app.Flag("block-split", "切分新块的时长").Short('B').Envar("COPYER_BLOCK_SPLIT").Default("24h").DurationVar(&args.blockTimeSplit)
	app.Flag("multi-thread", "并行多少个复制(0=GOMAXPROCS)").Short('T').Envar("COPYER_MUTIL_THREAD").Default("-1").IntVar(&args.multiThread)
}
