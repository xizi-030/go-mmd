package mmd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
)

const DefaultRetryInterval = 5*time.Second
const LocalhostUrl = "localhost:9999"

type OnConnection func(*Conn) error

// Conn Connection and channel dispatch map
type Conn struct {
	socket       *net.TCPConn
	dispatch     map[ChannelId]chan ChannelMsg
	dispatchLock sync.RWMutex
	socketLock   sync.Mutex
	callTimeout  time.Duration
	services     map[string]ServiceFunc
	config       *ConnConfig
}

func (c *Conn) Subscribe(service string, body interface{}) (*Chan, error) {
	buff := NewBuffer(1024)
	cc := NewChannelCreate(SubChan, service, body)
	err := Encode(buff, cc)
	if err != nil {
		return nil, err
	}
	ch := make(chan ChannelMsg, 1)
	c.registerChannel(cc.ChannelId, ch)
	err = c.writeOnSocket(buff.Flip().Bytes())
	if err != nil {
		return nil, err
	}
	return &Chan{Ch: ch, con: c, Id: cc.ChannelId}, nil
}

func (c *Conn) Call(service string, body interface{}) (interface{}, error) {
	return c.CallAuthenticated(service, AuthToken(NO_AUTH_TOKEN), body)
}

func (c *Conn) CallAuthenticated(service string, token AuthToken, body interface{}) (interface{}, error) {
	buff := NewBuffer(1024)
	cc := NewChannelCreate(CallChan, service, body)
	cc.AuthToken = token
	err := Encode(buff, cc)
	if err != nil {
		return nil, err
	}
	ch := make(chan ChannelMsg, 1)
	c.registerChannel(cc.ChannelId, ch)
	defer c.unregisterChannel(cc.ChannelId)
	err = c.writeOnSocket(buff.Flip().Bytes())
	if err != nil {
		return nil, err
	}
	select {
	case ret, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("Call Error: channel closed while waiting for return message")
		}

		e, ok := ret.Body.(MMDError)
		if ok {
			return nil, fmt.Errorf("MMD Error: %d: %v", e.code, e.msg)
		}

		return ret.Body, nil
	case <-time.After(c.callTimeout):
		return nil, fmt.Errorf("Timeout waiting for: %s", service)
	}
}

func (c *Conn) SetDefaultCallTimeout(dur time.Duration) {
	c.callTimeout = dur
}

func (c *Conn) GetDefaultCallTimeout() time.Duration {
	return c.callTimeout
}

func (c Conn) String() string {
	return fmt.Sprintf("Conn{remote: %s, local: %s}", c.socket.RemoteAddr(), c.socket.LocalAddr())
}

func (c *Conn) RegisterLocalService(name string, fn ServiceFunc) error {
	return c.registerServiceUtil(name, fn, "registerLocal")
}

func (c *Conn) RegisterService(name string, fn ServiceFunc) error {
	return c.registerServiceUtil(name, fn, "register")
}

// default to local connection to call a service
func Call(service string, body interface{}) (interface{}, error) {
	lc, err := LocalConnect()
	if err != nil {
		return nil, err
	}
	return lc.Call(service, body)
}

// Creates a default URL connection (-mmd to override)
func Connect() (*Conn, error) {
	return ConnectTo(mmdUrl)
}

func LocalConnect() (*Conn, error) {
	return ConnectTo(LocalhostUrl)
}

func ConnectTo(url string) (*Conn, error) {
	return NewConnConfig(url).Connect()
}

func ConnectWithTags(url string, myTags []string, theirTags []string) (*Conn, error) {
	conn := NewConnConfig(url)
	conn.ExtraMyTags = myTags
	conn.ExtraTheirTags = theirTags
	return conn.Connect()
}

func ConnectWithRetry(url string, reconnectInterval time.Duration, onConnect OnConnection) (*Conn, error) {
	cfg := NewConnConfig(url)
	cfg.ReconnectInterval = reconnectInterval
	cfg.AutoRetry = true
	cfg.OnConnect = onConnect
	return cfg.Connect()
}

func ConnectWithTagsWithRetry(url string, myTags []string, theirTags []string, reconnectInterval time.Duration, onConnect OnConnection) (*Conn, error) {
	cfg := NewConnConfig(url)
	cfg.ExtraMyTags = myTags
	cfg.ExtraTheirTags = theirTags
	cfg.ReconnectInterval = reconnectInterval
	cfg.AutoRetry = true
	cfg.OnConnect = onConnect
	return cfg.Connect()
}

// internal to package --

func (c *Conn) startReader() {
	go reader(c)
}

func (c *Conn) cleanupReader() {
	log.Println("Cleaning up reader")
	defer c.dispatchLock.Unlock()
	c.socket.CloseRead()
	c.dispatchLock.Lock()
	for k, v := range c.dispatch {
		log.Println("Auto-closing channel", k)
		delete(c.dispatch, k)
		close(v)
	}
}

func (c *Conn) cleanupSocket() {
	c.socket.CloseWrite()
}

func (c *Conn) reconnect() {
	err := c.closeSocket()
	if err != nil {
		log.Panicln("Failed to close socket: ", err)
	}

	start := time.Now()
	err = c.createSocketConnection(true)
	elapsed := time.Since(start)

	log.Println("Socket reset. Connected to mmd after :", elapsed)
}

func (c *Conn) closeSocket() error {
	if c.socket != nil {
		err := c.socket.Close()
		return err
	}

	log.Println("Cannot close a nil socket")
	return nil
}

func (c *Conn) createSocketConnection(isRetryConnection bool) error {
	addr, err := net.ResolveTCPAddr("tcp", c.config.Url)
	if err != nil {
		return err
	}

	if isRetryConnection && c.config.ReconnectDelay > 0 {
		log.Printf("Sleeping for %.2f seconds before trying next connection\n", c.config.ReconnectDelay.Seconds())
		time.Sleep(c.config.ReconnectDelay)
	}

	for {
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil && c.config.AutoRetry {
			log.Printf("Failed to connect, will sleep for %.2f seconds before trying again : %v\n", c.config.ReconnectInterval.Seconds(), err)
			time.Sleep(c.config.ReconnectInterval)
			continue
		}

		if err == nil {
			conn.SetWriteBuffer(c.config.WriteSz)
			conn.SetReadBuffer(c.config.ReadSz)
			c.socket = conn

			return c.onSocketConnection()
		}

		return err
	}
}

func (c *Conn) onSocketConnection() error {
	c.startReader()
	err := c.handshake()
	if err != nil {
		return err
	}

	if len(c.config.ExtraTheirTags) > 0 {
		c.Call("$mmd", map[string]interface{}{"extraTheirTags": c.config.ExtraTheirTags})
	}

	if c.config.OnConnect != nil {
		return c.config.OnConnect(c)
	}

	return nil
}

func (c *Conn) onDisconnect() {
	c.cleanupReader()
	c.cleanupSocket()
	if c.config.AutoRetry {
		c.reconnect()
	}
}

func (c *Conn) handshake() error {
	handshake := []byte{1, 1}
	handshake = append(handshake, c.config.AppName...)
	return c.writeOnSocket(handshake)
}

func (c *Conn) registerServiceUtil(name string, fn ServiceFunc, registryAction string) error {
	c.services[name] = fn
	ok, err := c.Call("serviceregistry", map[string]interface{}{
		"action": registryAction,
		"name":   name,
		"tag":    c.config.ExtraMyTags,
	})
	if err == nil && ok != "ok" {
		err = fmt.Errorf("Unexpected return: %v", ok)
	}
	if err != nil {
		delete(c.services, name)
	}
	return err
}

func (c *Conn) registerChannel(cid ChannelId, ch chan ChannelMsg) {
	c.dispatchLock.Lock()
	c.dispatch[cid] = ch
	c.dispatchLock.Unlock()
}

func (c *Conn) unregisterChannel(cid ChannelId) chan ChannelMsg {
	c.dispatchLock.Lock()
	ret, ok := c.dispatch[cid]
	if ok {
		delete(c.dispatch, cid)
	}
	c.dispatchLock.Unlock()
	return ret
}

func (c *Conn) lookupChannel(cid ChannelId) chan ChannelMsg {
	c.dispatchLock.RLock()
	ret := c.dispatch[cid]
	c.dispatchLock.RUnlock()
	return ret
}

func (c *Conn) writeOnSocket(data []byte) error {
	c.socketLock.Lock()
	defer c.socketLock.Unlock()

	fsz := make([]byte, 4)
	binary.BigEndian.PutUint32(fsz, uint32(len(data)))

	_, err := c.socket.Write(fsz)
	if err != nil {
		return fmt.Errorf("Failed to write header: %s %s", fsz, err)
	} else {
		_, err = c.socket.Write(data)
		if err != nil {
			return fmt.Errorf("Failed to write data: %s", err)
		}
	}

	return nil
}

func reader(c *Conn) {
	fszb := make([]byte, 4)
	buff := make([]byte, 256)
	defer c.onDisconnect()
	for {
		num, err := io.ReadFull(c.socket, fszb)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println("Error reading frame size:", err)
			return
		}
		if num != 4 {
			log.Println("Short read for size:", num)
			return
		}
		fsz := int(binary.BigEndian.Uint32(fszb))
		if len(buff) < fsz {
			buff = make([]byte, fsz)
		}

		reads := 0
		offset := 0
		for offset < fsz {
			sz, err := c.socket.Read(buff[offset:fsz])
			if err != nil {
				log.Panic("Error reading message:", err)
				return
			}
			reads++
			offset += sz
		}
		m, err := Decode(Wrap(buff[:fsz]))
		if err != nil {
			log.Panic("Error decoding buffer:", err)
		} else {
			switch msg := m.(type) {
			case ChannelMsg:
				if msg.IsClose {
					ch := c.unregisterChannel(msg.Channel)
					if ch != nil {
						ch <- msg
						close(ch)
					} else {
						log.Println("Unknown channel:", msg.Channel, "discarding message")
					}
				} else {
					ch := c.lookupChannel(msg.Channel)
					if ch != nil {
						ch <- msg
					} else {
						log.Println("Unknown channel:", msg.Channel, "discarding message")
					}
				}
			case ChannelCreate:
				fn, ok := c.services[msg.Service]
				if !ok {
					log.Println("Unknown service:", msg.Service, "cannot process", msg)
				}
				ch := make(chan ChannelMsg, 1)
				c.registerChannel(msg.ChannelId, ch)
				fn(c, &Chan{Ch: ch, con: c, Id: msg.ChannelId}, &msg)
			default:
				log.Panic("Unknown message type:", reflect.TypeOf(msg), msg)
			}
		}
	}
}
