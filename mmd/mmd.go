package mmd

import (
	"flag"
	logpkg "log"
	"os"
	"strconv"
)

var log = logpkg.New(os.Stdout, "[mmd] ", logpkg.LstdFlags|logpkg.Lmicroseconds)
var mmdUrl = "localhost:9999"

func init() {
	flag.StringVar(&mmdUrl, "mmd", mmdUrl, "Sets default MMD Url")
	//env takes precedence
	mmdUrl = getEnv("MMD_URL_OVERRIDE", mmdUrl)
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		var result, err = strconv.ParseBool(value)
		if err != nil {
			panic("Invalid value for boolean env var " + key +
				" - must be one of: 1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False")
		}
		return result
	}
	return fallback
}
