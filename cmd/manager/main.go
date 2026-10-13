package main

import (
	"os"
)

func main() {
	if err := newRootCommand().Execute(); err != nil {
		// cobra 已经把错误打到 stderr，这里只需要决定退出码。
		os.Exit(1)
	}
}
