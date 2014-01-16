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
		var tuple tuplespace.Tuple
		var err error
		switch command {
		case "read":
			tuple, err = c.Read(match, timeout)
		case "take":
			tuple, err = c.Take(match, timeout)
		}
		if err != nil {
			fatalf("failed to read tuple: %s", err)
		}
		if !*silentFlag {
			fmt.Printf("%v\n", tuple)
		}

	case "readall", "takeall":
		match := tuplespace.MustMatch(pflag.Arg(1))
		var tuples []tuplespace.Tuple
		var err error
		switch command {
		case "readall":
			tuples, err = c.ReadAll(match, timeout)
		case "takeall":
			tuples, err = c.TakeAll(match, timeout)
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
