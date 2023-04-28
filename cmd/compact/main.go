package compact

import (
	"os"
	"sync"

	"github.com/boringcat/prom-tsdb-copyer/compacter"
	"github.com/go-kit/log"
)

type compactReq struct {
	dstDir     string
	srcDir     string
	clean      bool
	blockSplit int64
}

func worker(wg *sync.WaitGroup, ch <-chan *compactReq) {
	defer wg.Done()
	for req := range ch {
		_, err := compacter.Compact(req.dstDir, req.srcDir, req.clean, req.blockSplit)
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
		ch := make(chan *compactReq)
		var wg sync.WaitGroup
		for i := 0; i < args.multiThread; i++ {
			wg.Add(1)
			go worker(&wg, ch)
		}
		for _, src := range args.sourceDirs {
			ch <- &compactReq{src, src, true, args.blockSplit}
		}
		wg.Wait()
	}
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}
