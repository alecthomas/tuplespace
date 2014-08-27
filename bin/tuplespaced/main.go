package main

import (
	"io"
	"net"
	"net/rpc"

	"github.com/hashicorp/yamux"

	"github.com/alecthomas/go-logging"
	"github.com/alecthomas/kingpin"
	"github.com/alecthomas/util"

	"github.com/alecthomas/tuplespace/service"
)

var (
	log = logging.MustGetLogger("tuplespaced")

	bindFlag = kingpin.Flag("bind", "Bind address for service.").Default("127.0.0.1:2619").TCP()
)

func main() {
	util.Bootstrap(kingpin.CommandLine, util.AllModules, nil)
	s := service.New()
	err := rpc.Register(s)
	kingpin.FatalIfError(err, "")
	bind, err := net.Listen("tcp", (*bindFlag).String())
	kingpin.FatalIfError(err, "")
	for {
		conn, err := bind.Accept()
		kingpin.FatalIfError(err, "")
		log.Infof("New connection %s -> %s", conn.RemoteAddr(), conn.LocalAddr())
		go func(conn net.Conn) {
			session, err := yamux.Server(conn, nil)
			if err != nil {
				log.Errorf("Failed to start multiplexer: %s", err)
				return
			}
			defer session.Close()
			for {
				stream, err := session.Accept()
				if err == io.EOF {
					return
				}
				if err != nil {
					log.Errorf("Failed to accept new stream on multiplexed connection: %s", err)
					return
				}
				go rpc.ServeConn(stream)
			}
		}(conn)
	}
}
