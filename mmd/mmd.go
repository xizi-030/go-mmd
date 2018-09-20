package mmd

import (
	"flag"
	logpkg "log"
	"os"
)

var log = logpkg.New(os.Stdout, "[mmd] ", logpkg.LstdFlags|logpkg.Lmicroseconds)
var mmdUrl = "localhost:9999"

func init() {
	flag.StringVar(&mmdUrl, "mmd", mmdUrl, "Sets default MMD Url")
}
