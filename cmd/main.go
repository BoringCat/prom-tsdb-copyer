package main

import (
	"fmt"
	"os"

	cmdCompact "github.com/boringcat/prom-tsdb-copyer/cmd/compact"
	cmdCopy "github.com/boringcat/prom-tsdb-copyer/cmd/copy"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version, commit, buildDate string
)

func main() {
	app := kingpin.New("prom-tsdb-copyer", "普罗米修斯TSDB复制器")
	app.HelpFlag.Short('h')
	app.Version(fmt.Sprintf("版本: %s-%s\n构建日期: %s", version, commit, buildDate)).VersionFlag.Short('V')
	copyCmd := app.Command("copy", "从本地或者远程复制TSDB到本地")
	compactCmd := app.Command("compact", "根据间隔压缩本地TSDB")
	cmdCopy.ParseArgs(copyCmd)
	cmdCompact.ParseArgs(compactCmd)
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))
	switch cmd {
	case copyCmd.FullCommand():
		cmdCopy.Main()
	case compactCmd.FullCommand():
		cmdCompact.Main()
	}
}
