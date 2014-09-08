package service

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/tuplespace"

	"github.com/stretchrcom/testify/assert"
)

var (
	serverAddr string
	server     sync.Once
)

func listenTCP() (net.Listener, string) {
	l, e := net.Listen("tcp", "127.0.0.1:0") // any available address
	if e != nil {
		log.Fatalf("net.Listen tcp :0: %v", e)
	}
	return l, l.Addr().String()
}

func startServer() {
	rpc.Register(New())
	var l net.Listener
	l, serverAddr = listenTCP()
	log.Println("Test RPC server listening on", serverAddr)
	go rpc.Accept(l)
}

func dial(space string) *ClientSpace {
	server.Do(startServer)
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalln(err)
	}
	return HijackConn(serverAddr, space, conn)
}

func TestServerSend(t *testing.T) {
	c := dial("TestServerSend")
	bob := tuplespace.Tuple{"name": "bob", "age": 60}
	err := c.Send(bob, 0)
	assert.NoError(t, err)
	err = c.Send(tuplespace.Tuple{"name": "fred", "age": 30}, 0)
	assert.NoError(t, err)

	status, err := c.Status()
	assert.NoError(t, err)
	assert.Equal(t, 2, status.Tuples.Seen)

	tuple, err := c.Read(`age > 50`, 0)
	assert.NoError(t, err)
	assert.Equal(t, bob, tuple)
}

func BenchmarkServerSend(b *testing.B) {
	c := dial("BenchmarkServerSend")
	bob := tuplespace.Tuple{"name": "bob", "age": 60}
	for i := 0; i < b.N; i++ {
		c.Send(bob, time.Second*5)
	}
}

func benchmarkServerTakeN(n int, b *testing.B) {
	c := dial(fmt.Sprintf("BenchmarkServerTake%d", n))
	tuple := tuplespace.Tuple{"i": 0}
	for i := 0; i < b.N/n; i++ {
		for j := 0; j < n; j++ {
			c.Send(tuple, 0)
		}
		for j := 0; j < n; j++ {
			r, err := c.Take("", 0, 0)
			if err != nil {
				panic(err)
			}
			r.Complete()
		}
	}
}

func BenchmarkServerTake1(b *testing.B) {
	benchmarkServerTakeN(1, b)
}

func BenchmarkServerTake10(b *testing.B) {
	benchmarkServerTakeN(10, b)
}

func BenchmarkServerTake100(b *testing.B) {
	benchmarkServerTakeN(100, b)
}

func BenchmarkServerTake1000(b *testing.B) {
	benchmarkServerTakeN(1000, b)
}
