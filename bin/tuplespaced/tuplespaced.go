package main

import (
	"fmt"
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/server"
	"github.com/alecthomas/tuplespace/store"
	"github.com/codegangsta/martini"
	"github.com/ogier/pflag"
	"net/http"
	"os"
	"runtime"
	"time"
)

var (
	bindFlag         = pflag.String("bind", "127.0.0.1:2619", "bind address")
	readTimeoutFlag  = pflag.Duration("read_timeout", 30*time.Second, "HTTP server read timeout")
	writeTimeoutFlag = pflag.Duration("write_timeout", 30*time.Second, "HTTP server write timeout")
	ncpuFlag         = pflag.Int("ncpu", runtime.NumCPU(), "number of cpus to use")
	logLevelFlag     = pflag.String("log-level", "info", "log level (finest, fine, debug, info, warning, error, critical)")
	storeFlag        = pflag.String("store", "leveldb", "set storage backend (memory, leveldb, gkvlite)")
	dbFlag           = pflag.String("db", "tuplespace.db", "path to database")

	logLevels = map[string]log.Level{
		"finest":   log.FINEST,
		"fine":     log.FINE,
		"debug":    log.DEBUG,
		"info":     log.INFO,
		"warning":  log.WARNING,
		"error":    log.ERROR,
		"critical": log.CRITICAL,
	}

	stores = map[string]func() (tuplespace.TupleStore, error){
		"memory":  func() (tuplespace.TupleStore, error) { return store.NewMemoryStore(), nil },
		"leveldb": func() (tuplespace.TupleStore, error) { return store.NewLevelDBStore(*dbFlag) },
	}
)

func fatalf(f string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+f, args...)
	os.Exit(1)
}

func Send(ts tuplespace.RawTupleSpace, r *http.Request, req tuplespace.SendRequest, resp server.ResponseSerializer) {
	err := ts.SendMany(req.Tuples, req.Timeout)

	if err != nil {
		resp.Error(http.StatusInternalServerError, err)
	} else {
		resp.Serialize(http.StatusOK, &tuplespace.SendResponse{})
	}
}

func Read(ts tuplespace.RawTupleSpace, resp server.ResponseSerializer, req tuplespace.ReadRequest, w http.ResponseWriter) {
	takeOrRead(false, ts, resp, req, w)
}

func Take(ts tuplespace.RawTupleSpace, resp server.ResponseSerializer, req tuplespace.ReadRequest, w http.ResponseWriter) {
	takeOrRead(true, ts, resp, req, w)
}

func takeOrRead(take bool, ts tuplespace.RawTupleSpace, resp server.ResponseSerializer, req tuplespace.ReadRequest, w http.ResponseWriter) {
	var tuples []tuplespace.Tuple
	var err error

	actions := 0
	if take {
		actions |= tuplespace.ActionTake
	}
	if !req.All {
		actions |= tuplespace.ActionOne
	}

	handle := ts.ReadOperation(req.Match, req.Timeout, actions)

	select {
	case <-w.(http.CloseNotifier).CloseNotify():
		err = tuplespace.CancelledReader
		handle.Cancel()
	case err = <-handle.Error():
	case tuples = <-handle.Get():
	}

	if err != nil {
		status := http.StatusInternalServerError
		if err == tuplespace.ReaderTimeout {
			status = http.StatusGatewayTimeout
		} else if err == tuplespace.CancelledReader {
			status = http.StatusRequestTimeout
		}
		resp.Error(status, err)
	} else {
		resp.Serialize(http.StatusOK, &tuplespace.ReadResponse{Tuples: tuples})
	}
}

func makeService(ts tuplespace.RawTupleSpace, debug bool) *martini.Martini {
	m := martini.New()
	m.Use(martini.Recovery())
	if debug {
		m.Use(martini.Logger())
	}
	m.Use(server.SerializationMiddleware())

	m.MapTo(ts, (*tuplespace.RawTupleSpace)(nil))

	r := martini.NewRouter()
	r.Post("/tuplespace/", server.DeserializerMiddleware(tuplespace.SendRequest{}), Send)
	r.Get("/tuplespace/", server.DeserializerMiddleware(tuplespace.ReadRequest{}), Read)
	r.Delete("/tuplespace/", server.DeserializerMiddleware(tuplespace.ReadRequest{}), Take)

	m.Action(r.Handle)
	return m
}

func main() {
	pflag.Usage = func() {
		fmt.Printf(`usage: tuplespaced [flags]

Run tuplespace server.

Flags:
`)
		pflag.PrintDefaults()
	}
	pflag.Parse()
	runtime.GOMAXPROCS(*ncpuFlag)

	log.AddFilter("stdout", logLevels[*logLevelFlag], log.NewConsoleLogWriter())
	debug := logLevels[*logLevelFlag] <= log.DEBUG

	log.Info("Starting server on http://%s/tuplespace/", *bindFlag)

	store, err := stores[*storeFlag]()
	if err != nil {
		fatalf("failed to initialise store %s: %s", *storeFlag, err.Error())
	}
	ts := tuplespace.NewTupleSpace(store)

	srv := &http.Server{
		Addr:         *bindFlag,
		Handler:      makeService(ts, debug),
		ReadTimeout:  *readTimeoutFlag,
		WriteTimeout: *writeTimeoutFlag,
	}
	err = srv.ListenAndServe()
	if err != nil {
		fatalf("error: %s\n", err)
	}
}
