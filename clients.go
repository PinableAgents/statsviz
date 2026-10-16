package statsviz

import (
	"context"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/PinableAgents/statsviz/internal/plot"
)

type clients struct {
	cfg *plot.Config
	ctx context.Context

	mu sync.RWMutex
	m  map[*websocket.Conn]chan []byte
	wg sync.WaitGroup

	closed bool
}

func newClients(ctx context.Context, cfg *plot.Config) *clients {
	return &clients{
		m:   make(map[*websocket.Conn]chan []byte),
		cfg: cfg,
		ctx: ctx,
	}
}

type wsmsg struct {
	Event string `json:"event"`
	Data  any    `json:"data"`
}

func (c *clients) add(conn *websocket.Conn) {
	dbglog("adding client")
	ch := make(chan []byte)

	// 先登记再写配置，使 close 能拒绝或中断与 Server.Close 并发的连接。
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close()
		return
	}
	c.m[conn] = ch
	c.wg.Add(1)
	c.mu.Unlock()

	// Send config first.
	err := conn.WriteJSON(wsmsg{Event: "config", Data: c.cfg})
	if err != nil {
		dbglog("failed to send config: %v", err)
		c.remove(conn, ch)
		return
	}

	go func() {
		defer c.remove(conn, ch)

		for {
			select {
			case <-c.ctx.Done():
				return
			case msg := <-ch:
				if err := sendbuf(conn, msg); err != nil {
					dbglog("failed to send data: %v", err)
					return
				}
			}
		}
	}()
}

func (c *clients) remove(conn *websocket.Conn, ch chan []byte) {
	c.mu.Lock()
	if c.m[conn] == ch {
		delete(c.m, conn)
	}
	c.mu.Unlock()
	_ = conn.Close()
	c.wg.Done()
	dbglog("removed client")
}

func (c *clients) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.wg.Wait()
		return
	}
	c.closed = true
	connections := make([]*websocket.Conn, 0, len(c.m))
	for conn := range c.m {
		connections = append(connections, conn)
	}
	c.mu.Unlock()

	for _, conn := range connections {
		_ = conn.Close()
	}
	c.wg.Wait()
}

func sendbuf(conn *websocket.Conn, buf []byte) error {
	w, err := conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	_, err1 := w.Write(buf)
	err2 := w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (c *clients) broadcast(buf []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, ch := range c.m {
		select {
		case ch <- buf:
		default:
			// if a client is not keeping up, we
			// drop the message for that client.
			dbglog("dropping message to client")
		}
	}
}
