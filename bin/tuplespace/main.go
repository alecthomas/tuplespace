package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/alecthomas/kingpin"

	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/service"
)

var (
	tuplespaceFlag = kingpin.Flag("tuplespace", "Address of TupleSpace service.").Default("127.0.0.1:2619").TCP()
	spaceFlag      = kingpin.Flag("space", "Default space to act on.").Default("test").String()
	timeoutFlag    = kingpin.Flag("timeout", "Tuple timeout.").Default("0s").Duration()

	statusCommand = kingpin.Command("status", "Status of TupleSpace.")

	sendCommand    = kingpin.Command("send", "Send a tuple.")
	sendCopiesflag = sendCommand.Flag("copies", "Number of copies of the tuple to send.").PlaceHolder("N").Int()
	sendtupleArg   = sendCommand.Arg("tuple", "Tuple to send.").Required().StringMap()

	readCommand  = kingpin.Command("read", "Read a tuple.")
	readMatchArg = readCommand.Arg("expr", "Expression to match with.").String()

	readallCommand  = kingpin.Command("readall", "Read all matching tuples.")
	readallMatchArg = readallCommand.Arg("expr", "Expression to match with.").String()

	takeCommand                = kingpin.Command("take", "Take a tuple.")
	takeReservationTimeoutFlag = takeCommand.Flag("reservation_timeout", "Reservation timeout.").Default("0s").Duration()
	takeMatchArg               = takeCommand.Arg("expr", "Expression to match with.").String()
)

func main() {
	command := kingpin.Parse()
	factory, err := service.Dial((*tuplespaceFlag).String())
	kingpin.FatalIfError(err, "")
	defer factory.Close()
	space, err := factory.Space(*spaceFlag)
	kingpin.FatalIfError(err, "")
	defer space.Close()
	switch command {
	case "status":
		status, err := space.Status()
		kingpin.FatalIfError(err, "")
		b, _ := json.Marshal(status)
		fmt.Printf("%s\n", b)

	case "send":
		tuple := tuplespace.Tuple{}
		for k, v := range *sendtupleArg {
			n, err := strconv.ParseFloat(v, 64)
			if err == nil {
				tuple[k] = n
			} else {
				tuple[k] = v
			}
		}
		if *sendCopiesflag > 0 {
			tuples := []tuplespace.Tuple{}
			for i := 0; i < *sendCopiesflag; i++ {
				tuples = append(tuples, tuple)
			}
			err = space.SendMany(tuples, *timeoutFlag)
		} else {
			err = space.Send(tuple, *timeoutFlag)
		}
		kingpin.FatalIfError(err, "")

	case "read":
		tuple, err := space.Read(*readMatchArg, *timeoutFlag)
		kingpin.FatalIfError(err, "")
		b, _ := json.Marshal(tuple)
		fmt.Printf("%s\n", b)

	case "readall":
		tuples, err := space.ReadAll(*readallMatchArg, *timeoutFlag)
		kingpin.FatalIfError(err, "")
		for _, tuple := range tuples {
			b, _ := json.Marshal(tuple)
			fmt.Printf("%s\n", b)
		}

	case "take":
		reservation, err := space.Take(*takeMatchArg, *timeoutFlag, *takeReservationTimeoutFlag)
		kingpin.FatalIfError(err, "")
		b, _ := json.Marshal(reservation.Tuple())
		fmt.Printf("%s\n", b)
		err = reservation.Complete()
		kingpin.FatalIfError(err, "")
	}
}
