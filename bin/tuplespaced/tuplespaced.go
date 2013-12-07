package main

import (
	"fmt"
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/store"
	"github.com/codegangsta/martini"
	"github.com/codegangsta/martini-contrib/binding"
	"github.com/codegangsta/martini-contrib/render"
	"github.com/ogier/pflag"
	"net/http"
	"os"
	"runtime"
	"time"
)

var (
	bindFlag         = pflag.String("bind", "127.0.0.1:2619", "bind address")
	profilerFlag     = pflag.Bool("profiler", false, "run server under profiler")
	readTimeoutFlag  = pflag.Duration("read_timeout", 30*time.Second, "HTTP server read timeout")
	writeTimeoutFlag = pflag.Duration("write_timeout", 30*time.Second, "HTTP server write timeout")
	ncpuFlag         = pflag.Int("ncpu", runtime.NumCPU(), "number of cpus to use")
	logLevelFlag     = pflag.String("log-level", "info", "log level (finest, fine, debug, info, warning, error, critical)")
	storeFlag        = pflag.String("store", "leveldb", "set storage backend (memory, leveldb)")
	dbFlag           = pflag.String("db", "tuplestore.db", "path to database")

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

func Send(ts tuplespace.RawTupleSpace, req tuplespace.SendRequest, resp render.Render, errors binding.Errors) {
	if errors.Count() > 0 {
		resp.JSON(http.StatusBadRequest, &tuplespace.ErrorResponse{Error: "invalid request structure"})
		return
	}

	err := ts.SendMany(req.Tuples, req.Timeout)

	if err != nil {
		resp.JSON(http.StatusInternalServerError, &tuplespace.ErrorResponse{Error: err.Error()})
	} else {
		resp.JSON(http.StatusCreated, &tuplespace.SendResponse{})
	}
}

func Read(ts tuplespace.RawTupleSpace, w http.ResponseWriter, req tuplespace.ReadRequest, resp render.Render, errors binding.Errors) {
	takeOrRead(false, ts, w, req, resp, errors)
}

func Take(ts tuplespace.RawTupleSpace, w http.ResponseWriter, req tuplespace.ReadRequest, resp render.Render, errors binding.Errors) {
	takeOrRead(true, ts, w, req, resp, errors)
}

func takeOrRead(take bool, ts tuplespace.RawTupleSpace, w http.ResponseWriter,
	req tuplespace.ReadRequest, resp render.Render, errors binding.Errors) {
	if errors.Count() > 0 {
		resp.JSON(http.StatusBadRequest, &tuplespace.ErrorResponse{Error: "invalid request structure"})
		return
	}

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
		resp.JSON(status, &tuplespace.ErrorResponse{Error: err.Error()})
	} else {
		resp.JSON(http.StatusOK, &tuplespace.ReadResponse{Tuples: tuples})
	}
}

func makeService(ts tuplespace.RawTupleSpace, debug bool) *martini.Martini {
	m := martini.New()
	m.Use(martini.Recovery())
	if debug {
		m.Use(martini.Logger())
	}
	m.Use(render.Renderer(render.Options{}))

	m.MapTo(ts, (*tuplespace.RawTupleSpace)(nil))

	r := martini.NewRouter()
	r.Post("/tuplespace/", binding.Json(tuplespace.SendRequest{}), Send)
	r.Get("/tuplespace/", binding.Json(tuplespace.ReadRequest{}), Read)
	r.Delete("/tuplespace/", binding.Json(tuplespace.ReadRequest{}), Take)

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

	if *profilerFlag {
		log.Warn("Running with profiler under http://localhost:6060/debug/pprof/")
		go func() { log.Error("%s", http.ListenAndServe("localhost:6060", nil)) }()
	}

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
