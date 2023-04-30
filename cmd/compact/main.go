package compact

import (
	"os"
	"sync"

	"github.com/boringcat/prom-tsdb-copyer/compacter"
	"github.com/go-kit/log"
	"github.com/panjf2000/ants/v2"
)

func compactJob(dst, src string, clean bool, blockSplit int64, wg *sync.WaitGroup) func() {
	return func() {
		defer wg.Done()
		_, err := compacter.Compact(dst, src, clean, blockSplit)
		noErr(err)
	}
}

func Main() {
	noErr(args.ParseArgs())
	compacter.SetLogger(log.With(log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout)), "ts", log.DefaultTimestamp))
	if args.multiThread < 0 {
		for _, src := range args.sourceDirs {
			_, err := compacter.Compact(src, src, true, args.blockSplit)
			noErr(err)
		}
	} else {
		var wg sync.WaitGroup
		wg.Add(len(args.sourceDirs))
		for _, src := range args.sourceDirs {
			ants.Submit(compactJob(src, src, true, args.blockSplit, &wg))
		}
		wg.Wait()
	}
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}
