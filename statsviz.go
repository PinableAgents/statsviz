// Package statsviz allows visualizing Go [runtime/metrics] data in real time.
//
// Register Statsviz HTTP handlers with your server's [http.ServeMux]
// (preferred method):
//
//	mux := http.NewServeMux()
//	statsviz.Register(mux)
//
// Alternatively, you can register with [http.DefaultServeMux]
// though you shouldn't do that in production:
//
//	ss := statsviz.NewServer()
//	ss.Register(http.DefaultServeMux)
//
// By default, Statsviz is served at http://host:port/debug/statsviz/. This, and
// other settings, can be changed by passing some [Option] to [NewServer].
//
// If your application is not already running an HTTP server, you need to start
// one. Add "net/http" and "log" to your imports, and use the following code in
// your main function:
//
//	go func() {
//	    log.Println(http.ListenAndServe("localhost:8080", nil))
//	}()
//
// Then open your browser and visit http://localhost:8080/debug/statsviz/.
//
// # Advanced usage:
//
// If you want more control over Statsviz HTTP handlers, for examples if:
//   - you're using some HTTP framework
//   - you want to place Statsviz handler behind some middleware
//
// then use [NewServer] to obtain a [Server] instance. Both the [Server.Index] and
// [Server.Ws]() methods return [http.HandlerFunc].
//
//	srv, err := statsviz.NewServer(); // Create server or handle error
//	srv.Index()                       // UI (dashboard) http.HandlerFunc
//	srv.Ws()                          // Websocket http.HandlerFunc
package statsviz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/PinableAgents/statsviz/history"
	"github.com/PinableAgents/statsviz/internal/plot"
	"github.com/PinableAgents/statsviz/internal/static"
)

const (
	defaultRoot         = "/debug/statsviz"
	defaultSendInterval = time.Second
	historyTimeout      = 2 * time.Second
)

// RegisterDefault registers the Statsviz HTTP handlers on [http.DefaultServeMux].
//
// RegisterDefault should not be used in production.
func RegisterDefault(opts ...Option) error {
	return Register(http.DefaultServeMux, opts...)
}

// Register registers the Statsviz HTTP handlers on the provided mux.
//
// Register must be called once per application.
func Register(mux *http.ServeMux, opts ...Option) error {
	srv, err := NewServer(opts...)
	if err != nil {
		return err
	}
	srv.Register(mux)
	return nil
}

// Server is the core component of Statsviz. It collects and periodically
// updates metrics data and provides two essential HTTP handlers:
//   - the Index handler serves Statsviz user interface, allowing you to
//     visualize runtime metrics on your browser.
//   - The Ws handler establishes a WebSocket connection allowing the connected
//     browser to receive metrics updates from the server.
//
// The zero value is a valid Server, with default options.
//
// NOTE: Having more than one Server in the same program is not supported (and
// is not useful anyway).
type Server struct {
	cancel  context.CancelFunc // terminate goroutines
	clients *clients           // connected websocket clients

	interval  time.Duration // interval between consecutive metrics emission
	root      string        // HTTP path root
	plots     *plot.List    // plots shown on the user interface
	userPlots []plot.UserPlot

	historyStore history.Store

	initMu      sync.Mutex
	initialized bool
	closed      bool
	initErr     error
	wg          sync.WaitGroup
	closeOnce   sync.Once
	closeErr    error
}

// NewServer constructs a new Statsviz Server with the provided options, or the
// default settings.
//
// Note that once the server is created, its HTTP handlers needs to be registered
// with some HTTP server. You can either use the Register method or register yourself
// the Index and Ws handlers.
func NewServer(opts ...Option) (*Server, error) {
	s := &Server{}
	if err := s.initialize(opts...); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) initialize(opts ...Option) error {
	s.initMu.Lock()
	defer s.initMu.Unlock()
	if s.closed {
		return errors.New("statsviz: server is closed")
	}
	if s.initialized {
		return s.initErr
	}
	s.initialized = true
	s.interval = defaultSendInterval
	s.root = defaultRoot

	for _, opt := range opts {
		if err := opt(s); err != nil {
			s.initErr = err
			return s.initErr
		}
	}

	pl, err := plot.NewList(s.userPlots)
	if err != nil {
		s.initErr = err
		return s.initErr
	}
	s.plots = pl
	cfg := s.plots.Config()
	cfg.History = plot.HistoryConfig{IntervalMS: s.interval.Milliseconds()}

	if s.historyStore != nil {
		configJSON, err := json.Marshal(cfg)
		if err != nil {
			s.initErr = fmt.Errorf("encode history run config: %w", err)
			return s.initErr
		}
		startCtx, cancelStart := context.WithTimeout(context.Background(), historyTimeout)
		run, err := s.historyStore.StartRun(startCtx, history.RunConfig{
			StartedAtMS: time.Now().UnixMilli(),
			IntervalNS:  s.interval.Nanoseconds(),
			Config:      configJSON,
			Aggregation: s.plots.Aggregation(),
		})
		cancelStart()
		if err != nil {
			s.initErr = fmt.Errorf("start history run: %w", err)
			return s.initErr
		}
		if run.ID <= 0 {
			s.initErr = errors.New("start history run: store returned an invalid run ID")
			return s.initErr
		}
		s.plots.SetRunID(run.ID)
		cfg.History.Enabled = true
		cfg.History.RunID = run.ID
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.clients = newClients(ctx, cfg)

	// Collect metrics.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		tick := time.NewTicker(s.interval)
		defer tick.Stop()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				snapshot, err := s.plots.Collect()
				if err != nil {
					dbglog("failed to collect metrics: %v", err)
					continue
				}
				buf := bytes.Buffer{}
				if err := snapshot.WriteMetrics(&buf); err != nil {
					dbglog("failed to encode metrics: %v", err)
					continue
				}
				s.clients.broadcast(buf.Bytes())
				if s.historyStore != nil {
					err := s.historyStore.Enqueue(history.Sample{
						RunID:      snapshot.RunID,
						Seq:        snapshot.Seq,
						Timestamp:  snapshot.Timestamp,
						DurationNS: snapshot.DurationNS,
						Series:     snapshot.Series,
					})
					if err != nil {
						dbglog("failed to enqueue history sample: %v", err)
					}
				}
			}
		}
	}()

	return nil
}

func (s *Server) ensureInitialized() error {
	return s.initialize()
}

// Register registers the Statsviz HTTP handlers on the provided mux.
//
// Register must be called once per application.
func (s *Server) Register(mux *http.ServeMux) {
	_ = s.ensureInitialized()
	base := strings.TrimSuffix(s.root, "/")
	if base == "" {
		mux.Handle("/", s.Index())
		mux.HandleFunc("/ws", s.Ws())
		mux.HandleFunc("/history/", s.History())
		return
	}
	mux.Handle(base+"/", s.Index())
	mux.HandleFunc(base+"/ws", s.Ws())
	mux.HandleFunc(base+"/history/", s.History())
}

// Close releases all resources used by the Server.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.initMu.Lock()
		s.closed = true
		cancel := s.cancel
		clients := s.clients
		store := s.historyStore
		s.initMu.Unlock()
		if cancel != nil {
			cancel()
		}
		if clients != nil {
			clients.close()
		}
		s.wg.Wait()
		if store != nil {
			ctx, cancelFlush := context.WithTimeout(context.Background(), historyTimeout)
			defer cancelFlush()
			if err := store.Flush(ctx); err != nil {
				s.closeErr = fmt.Errorf("flush history: %w", err)
			}
		}
	})
	return s.closeErr
}

// Option is a configuration option for the Server.
type Option func(*Server) error

// SendFrequency changes the interval between successive acquisitions of metrics
// and their sending to the user interface. The default interval is one second.
func SendFrequency(intv time.Duration) Option {
	return func(s *Server) error {
		if intv <= 0 {
			return fmt.Errorf("frequency must be a positive integer")
		}
		s.interval = intv
		return nil
	}
}

// Root changes the root path of the Statsviz user interface.
// The default is "/debug/statsviz".
func Root(path string) Option {
	return func(s *Server) error {
		s.root = strings.TrimSuffix(path, "/")
		return nil
	}
}

// TimeseriesPlot adds a new time series plot to Statsviz. This options can
// be added multiple times.
func TimeseriesPlot(tsp TimeSeriesPlot) Option {
	return func(s *Server) error {
		s.userPlots = append(s.userPlots, plot.UserPlot{Scatter: tsp.timeseries})
		return nil
	}
}

// Index returns the index handler, which responds with the Statsviz user
// interface HTML page. By default, the handler is served at the path specified
// by the root. Use [WithRoot] to change the path.
func (s *Server) Index() http.HandlerFunc {
	if err := s.ensureInitialized(); err != nil {
		return func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "statsviz initialization failed", http.StatusServiceUnavailable)
		}
	}
	dist := http.FileServerFS(static.Assets())
	return http.StripPrefix(s.root+"/", dist).ServeHTTP
}

func parseBoolEnv(name string) bool {
	env := os.Getenv(name)
	val, err := strconv.ParseBool(env)
	if err != nil {
		if env != "" {
			fmt.Fprintf(os.Stderr, "statsviz: malformed %s %v\n", name, err)
		}
	}
	return val
}

var debug = false

func dbglog(format string, args ...any) {
	if debug {
		fmt.Fprintf(os.Stderr, "statsviz: "+format+"\n", args...)
	}
}

var wsUpgrader = sync.OnceValue(func() websocket.Upgrader {
	var checkOrigin func(r *http.Request) bool

	// Allow all origins for testing.
	if debug = parseBoolEnv("STATSVIZ_DEBUG"); debug {
		// passthrough
		checkOrigin = func(r *http.Request) bool { return true }
	}

	return websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 2048,
		CheckOrigin:     checkOrigin,
	}
})

// Ws returns the WebSocket handler used by Statsviz to send application
// metrics. The underlying net.Conn is used to upgrade the HTTP server
// connection to the WebSocket protocol.
func (s *Server) Ws() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.ensureInitialized(); err != nil {
			http.Error(w, "statsviz initialization failed", http.StatusServiceUnavailable)
			return
		}
		upgrader := wsUpgrader()
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			dbglog("failed to upgrade connection: %v", err)
			return
		}

		s.clients.add(ws)
	}
}
