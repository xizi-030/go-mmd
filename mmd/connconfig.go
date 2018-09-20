package mmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const reconnectInterval = time.Second * 10
const reconnectDelay = time.Second * 1

type ConnConfig struct {
	Url               string
	ReadSz            int
	WriteSz           int
	AppName           string
	AutoRetry         bool
	ReconnectInterval time.Duration
	ReconnectDelay    time.Duration
	OnConnect         OnConnection
}

func NewConnConfig(url string) *ConnConfig {
	return &ConnConfig{
		Url:               url,
		ReadSz:            64 * 1024,
		WriteSz:           64 * 1024,
		AppName:           fmt.Sprintf("Go:%s", filepath.Base(os.Args[0])),
		AutoRetry:         false,
		ReconnectInterval: reconnectInterval,
		ReconnectDelay:    reconnectDelay,
	}
}

func (c *ConnConfig) Connect() (*Conn, error) {
	return _create_connection(c)
}

func _create_connection(cfg *ConnConfig) (*Conn, error) {
	mmdc := &Conn{
		dispatch:    make(map[ChannelId]chan ChannelMsg, 1024),
		callTimeout: time.Second * 30,
		services:    make(map[string]ServiceFunc),
		config:      cfg,
	}

	err := mmdc.createSocketConnection(false)
	if err != nil {
		return nil, err
	}

	return mmdc, err
}
