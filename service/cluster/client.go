package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alecthomas/tuplespace"

	"github.com/cenkalti/backoff"

	"gopkg.in/tomb.v2"

	"github.com/alecthomas/tuplespace/service"
)

var (
	// PeerHeartbeatPeriod is the amount of time between
	PeerHeartbeatPeriod = time.Second * 5
	PeerTimeout         = time.Second * 10
	// OperationTimeout is the amount of time to wait for operations such as
	// Send() and Read() to complete.
	OperationTimeout = time.Second * 15

	ErrNoPeers = errors.New("no peers remaining")
)

type Logger interface {
	Infof(fmt string, args ...interface{}) error
	Warningf(fmt string, args ...interface{}) error
	Errorf(fmt string, args ...interface{}) error
}

type clients struct {
	lock    sync.Mutex
	tomb    tomb.Tomb
	log     Logger
	keys    []string
	clients map[string]*service.Client
	backoff map[string]backoff.BackOff
	next    map[string]time.Time
}

func newClients(log Logger) *clients {
	c := &clients{
		clients: map[string]*service.Client{},
		backoff: map[string]backoff.BackOff{},
		next:    map[string]time.Time{},
	}
	c.tomb.Go(c.run)
	return c
}

func (c *clients) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.tomb.Kill(nil)
	for _, client := range c.clients {
		client.Close()
	}
	return nil
}

func (c *clients) run() error {
	for _, client := range c.clients {
		go c.monitor(client)
	}
	<-c.tomb.Dying()
	return nil
}

// Monitor the peer list on a single server, removing and adding as required.
func (c *clients) monitor(client *service.Client) {
	space, err := client.Space(".cluster.peers")
	if err != nil {
		c.log.Errorf("Could not connect to .cluster.peers on %s, marking dead.", client.RemoteAddr())
		c.dead(client)
		return
	}
	for {
		start := time.Now()
		peers, err := space.ReadAll("", PeerHeartbeatPeriod)
		if err != nil {
			c.log.Errorf("Failed to read peer list from %s, marking dead.", client.RemoteAddr())
			c.dead(client)
			return
		}
		for _, peer := range peers {
			addr, ok := peer["addr"].(string)
			if !ok {
				c.log.Errorf("Invalid peer tuple %+v, continuing.", peer["addr"])
				continue
			}
			if c.canUpdate(addr) {
				if err := c.update(addr); err != nil {
					c.log.Warningf("Failed to update peer %s, continuing.", addr)
					continue
				}
			}
		}
		elapsed := time.Now().Sub(start)
		time.Sleep(PeerHeartbeatPeriod - elapsed)
	}
}

// Send addresses of all known peers to the given space.
// func (c *clients) heartbeat(space *service.ClientSpace) error {
// 	c.lock.Lock()
// 	defer c.lock.Unlock()
// 	tuples := make([]tuplespace.Tuple, 0, len(c.clients))
// 	for addr := range c.clients {
// 		tuples = append(tuples, tuplespace.Tuple{"addr": addr})
// 	}
// 	return space.SendMany(tuples, PeerTimeout)
// }

type Operation func(*service.ClientSpace) error

// SendRandom sends tuples to a random set of peers.
func (c *clients) SendRandom(space string, tuples []tuplespace.Tuple, expires time.Duration, timeout time.Duration) error {
	// NOTE: We don't use a sync.WaitGroup here because we can't use select
	// with it.

	// Track worker completion.
	workers := 0
	workersch := make(chan bool)
	// Track tuples successfully delivered.
	acks := 0
	acksch := make(chan int, len(tuples))
	// Send tuples to workers.
	tuplesch := make(chan tuplespace.Tuple)

	// Start one worker per peer.
	c.lock.Lock()
	for _, client := range c.clients {
		workers++
		go func(client *service.Client) {
			defer func() { workersch <- true }()
			s, err := client.Space(space)
			if err != nil {
				c.log.Errorf("Couldn't connect to space %s on %s, marking dead.", space, client.RemoteAddr())
				c.dead(client)
				return
			}
			for tuple := range tuplesch {
				if err := s.Send(tuple, expires); err != nil {
					c.log.Errorf("Couldn't send to %s on %s, marking dead.", space, client.RemoteAddr())
					c.dead(client)
					// Send the tuple back for a retry.
					tuplesch <- tuple
					return
				}
				acksch <- 1
			}
		}(client)
	}
	c.lock.Unlock()

	timeoutEnd := time.Now().Add(timeout)
	for _, tuple := range tuples {
		select {
		case tuplesch <- tuple:
		case <-workersch:
			workers--
			// We ran out of workers before we ran out of tuples :(
			// TODO: Check if there are any live clients and spin up new workers.
			if workers <= 0 {
				return ErrNoPeers
			}
		case n := <-acksch:
			acks += n
		case <-time.After(timeoutEnd.Sub(time.Now())):
			close(tuplesch)
		}
	}

COMPLETION:
	for {
		select {
		case <-workersch:
			workers--
			if workers <= 0 {
				break
			}
		case n := <-acksch:
			acks += n
			if acks >= len(tuples) {
				break COMPLETION
			}
		case <-time.After(timeoutEnd.Sub(time.Now())):
			close(tuplesch)
			return fmt.Errorf("timed out before sending all tuples")
		}
	}
	return nil
}

// Update the client connection fro an address. If the connection exists, do
// nothing. If it does not exist, attempt to reconnect. Reconnects are rate
// limited.
func (c *clients) update(addr string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.clients[addr]; ok {
		return nil
	}
	boff, ok := c.backoff[addr]
	if !ok {
		boff = backoff.NewExponentialBackOff()
		boff.Reset()
		c.backoff[addr] = boff
	} else {
		if time.Now().Before(c.next[addr]) {
			return fmt.Errorf("Too many reconnects to %s", addr)
		}
	}
	c.next[addr] = time.Now().Add(boff.NextBackOff())
	client, err := service.Dial(addr)
	if err != nil {
		return err
	}
	c.clients[addr] = client
	c.updateKeys()
	boff.Reset()
	return nil
}

// Returns true if the given address can be updated. This will return false if
// too many reconnect attempts have been made in a short period.
func (c *clients) canUpdate(addr string) bool {
	next, ok := c.next[addr]
	return !ok || time.Now().After(next)
}

func (c *clients) dead(client *service.Client) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.clients, client.RemoteAddr())
	c.updateKeys()
}

func (c *clients) updateKeys() {
	c.keys = make([]string, 0, len(c.clients))
	for k := range c.clients {
		c.keys = append(c.keys, k)
	}
}

func (c *clients) Size() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.clients)
}

type Client struct {
	lock    sync.Mutex
	log     Logger
	clients *clients
}

type nullLogger struct{}

func (nullLogger) Infof(fmt string, args ...interface{}) error    { return nil }
func (nullLogger) Warningf(fmt string, args ...interface{}) error { return nil }
func (nullLogger) Errorf(fmt string, args ...interface{}) error   { return nil }

func Dial(seeds []string) (*Client, error) {
	clients := newClients(nullLogger{})
	for _, addr := range seeds {
		if clients.update(addr) != nil {
			continue
		}
	}
	if clients.Size() == 0 {
		return nil, ErrNoPeers
	}
	return &Client{clients: clients}, nil
}

func (c *Client) SetLogger(log Logger) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.log = log
}

func (c *Client) Space(space string) (*ClientSpace, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return &ClientSpace{
		space:   space,
		clients: c.clients,
	}, nil
}

type ClientSpace struct {
	space   string
	clients *clients
}

func (c *ClientSpace) Send(tuple tuplespace.Tuple, expires time.Duration) error {
	return c.clients.SendRandom(c.space, []tuplespace.Tuple{tuple}, expires, OperationTimeout)
}
