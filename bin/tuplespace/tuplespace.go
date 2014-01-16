package main

import (
	"encoding/json"
	"fmt"
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/client"
	"github.com/ogier/pflag"
	"os"
	"runtime"
	"time"
)

var (
	serverFlag  = pflag.String("server", "http://127.0.0.1:2619/tuplespace/", "tuplespace server address")
	timeoutFlag = pflag.Duration("timeout", time.Second*60, "tuplespace operation timeout")
	copiesFlag  = pflag.Int("copies", 1, "number of copies of the tuple to send")
	silentFlag  = pflag.Bool("silent", false, "don't display received tuples")
)

func fatalf(f string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+f+"\n", args...)
	os.Exit(1)
}

func parseTuple(arg string) (tuple tuplespace.Tuple) {
	err := json.Unmarshal([]byte(arg), &tuple)
	if err != nil {
		fatalf("invalid tuple (%s)", err.Error())
	}
	return
}

func main() {
	pflag.Usage = func() {
		fmt.Print(`usage: tuplespace <cmd> <args...>

Commands:
    send <tuple>
    read <tuple>
    take <tuple>
    readall <tuple>
    takeall <tuple>

Where <tuple> is in the form '[<value>|null, ...]'.

Flags:
`)
		pflag.PrintDefaults()
		fmt.Print(`

Examples:
    tuplespace send '["cmd", "uname -a"]'

    tuplespace read '["cmd", null]'
`)
	}
	pflag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	if len(pflag.Args()) < 2 {
		fatalf("invalid number of arguments\n")
	}

	timeout := *timeoutFlag
	command := pflag.Arg(0)

	c := client.NewTupleSpaceClient(*serverFlag)

	switch command {
	case "send":
		tuple := parseTuple(pflag.Arg(1))
		tuples := make([]tuplespace.Tuple, *copiesFlag)
		for i := 0; i < *copiesFlag; i++ {
			tuples[i] = tuple
		}
		log.Info("Sending %d tuples", *copiesFlag)
		err := c.SendMany(tuples, timeout)
		if err != nil {
			fatalf("failed to send tuples: %s", err)
		}

	case "read", "take":
		match := tuplespace.MustMatch(pflag.Arg(1))
		tuple := map[string]interface{}{}
		var err error
		switch command {
		case "read":
			err = c.Read(match, timeout, tuple)
		case "take":
			err = c.Take(match, timeout, tuple)
		}
		if err != nil {
			fatalf("failed to read tuple: %s", err)
		}
		if !*silentFlag {
			fmt.Printf("%v\n", tuple)
		}

	case "readall", "takeall":
		match := tuplespace.MustMatch(pflag.Arg(1))
		tuples := []map[string]interface{}{}
		var err error
		switch command {
		case "readall":
			err = c.ReadAll(match, timeout, &tuples)
		case "takeall":
			err = c.TakeAll(match, timeout, &tuples)
		}
		if err != nil {
			fatalf("failed to read tuples: %s", err)
		}
		if !*silentFlag {
			for _, tuple := range tuples {
				fmt.Printf("%s\n", tuple)
			}
		}

	default:
		fatalf("unknown command: %s", command)
	}
}
