package cluster

import (
	"errors"
	"fmt"
	"math/rand"
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
	ErrTimeout = errors.New("timeout")
)

type Logger interface {
	Infof(fmt string, args ...interface{}) error
	Warningf(fmt string, args ...interface{}) error
	Errorf(fmt string, args ...interface{}) error
}

// Observe adding and removal of clients to the cluster client.
type clientObserver interface {
	// Notify observer about a client being added. If the method returns an
	// error the client will be closed.
	addClient(client *service.Client) error
	// Notify observer that a client has been removed.
	removeClient(addr string)
}

// Maintains client connections to the active peers in the cluster.
type clients struct {
	lock      sync.Mutex
	log       Logger
	keys      []string
	clients   map[string]*service.Client
	backoff   map[string]backoff.BackOff
	next      map[string]time.Time
	observers map[clientObserver]struct{}
}

func newClients(log Logger) *clients {
	c := &clients{
		clients:   map[string]*service.Client{},
		backoff:   map[string]backoff.BackOff{},
		next:      map[string]time.Time{},
		observers: map[clientObserver]struct{}{},
	}
	c.addObserver(c)
	return c
}

func (c *clients) Space(space string) *ClientSpace {
	s := newClientSpace(space, c)
	c.addObserver(s)
	return s
}

func (c *clients) addClient(client *service.Client) error {
	go c.monitor(client)
	return nil
}

func (c *clients) removeClient(addr string) {}

func (c *clients) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for addr := range c.clients {
		c.delete(addr)
	}
	return nil
}

// Monitor the peer list on a single server, removing and adding as required.
func (c *clients) monitor(client *service.Client) {
	space, err := client.Space(".cluster.peers")
	if err != nil {
		c.log.Errorf("Could not connect to .cluster.peers on %s, marking dead.", client.RemoteAddr())
		c.Dead(client.RemoteAddr())
		return
	}
	for {
		start := time.Now()
		peers, err := space.ReadAll("", PeerHeartbeatPeriod)
		if err != nil {
			c.log.Errorf("Failed to read peer list from %s, marking dead.", client.RemoteAddr())
			c.Dead(client.RemoteAddr())
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

// Update the client connection for an address. If the connection exists, do
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

// Mark a client as dead.
func (c *clients) Dead(addr string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.delete(addr)
}

func (c *clients) delete(addr string) {
	// TODO: This should deal with observers blocking...
	for observer := range c.observers {
		observer.removeClient(addr)
	}
	if client, ok := c.clients[addr]; ok {
		delete(c.clients, addr)
		c.updateKeys()
		client.Close()
	}
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

// Random returns a random client from available clients.
func (c *clients) Random() (*service.Client, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	count := len(c.clients)
	if count == 0 {
		return nil, ErrNoPeers
	}
	n := rand.Intn(count)
	key := c.keys[n]
	return c.clients[key], nil
}

func (c *clients) addObserver(observer clientObserver) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for addr, client := range c.clients {
		// TODO: This should deal with observers blocking...
		err := observer.addClient(client)
		if err != nil {
			c.delete(addr)
		}
	}
	c.observers[observer] = struct{}{}
}

func (c *clients) removeObserver(observer clientObserver) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.observers, observer)
}

type nullLogger struct{}

func (nullLogger) Infof(fmt string, args ...interface{}) error    { return nil }
func (nullLogger) Warningf(fmt string, args ...interface{}) error { return nil }
func (nullLogger) Errorf(fmt string, args ...interface{}) error   { return nil }

type Client struct {
	lock    sync.Mutex
	log     Logger
	clients *clients
}

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

func (c *Client) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.clients.Close()
}

func (c *Client) SetLogger(log Logger) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.log = log
}

func (c *Client) Space(space string) (*ClientSpace, error) {
	return c.clients.Space(space), nil
}

// ClientSpace maintains a current list of clients to all cluster peers, for a
// particular space.
type ClientSpace struct {
	lock    sync.Mutex
	space   string
	clients *clients
	keys    []string
	spaces  map[string]*service.ClientSpace
}

func newClientSpace(space string, clients *clients) *ClientSpace {
	return &ClientSpace{
		space:   space,
		clients: clients,
		spaces:  map[string]*service.ClientSpace{},
	}
}

func (c *ClientSpace) addClient(client *service.Client) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	space, err := client.Space(c.space)
	if err != nil {
		return err
	}
	c.keys = append(c.keys, client.RemoteAddr())
	c.spaces[client.RemoteAddr()] = space
	return nil
}

func (c *ClientSpace) removeClient(addr string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.spaces, addr)
	// Rebuild key slice.
	c.keys = []string{}
	for key := range c.spaces {
		c.keys = append(c.keys, key)
	}
}

// Get a copy of all known clients.
func (c *ClientSpace) allClients() map[string]*service.ClientSpace {
	c.lock.Lock()
	defer c.lock.Unlock()
	clients := make(map[string]*service.ClientSpace, len(c.spaces))
	for addr, client := range c.spaces {
		clients[addr] = client
	}
	return clients
}

func (c *ClientSpace) randomClient() (string, *service.ClientSpace) {
	c.lock.Lock()
	defer c.lock.Unlock()
	n := rand.Intn(len(c.keys))
	key := c.keys[n]
	return key, c.spaces[key]
}

func (c *ClientSpace) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clients.removeObserver(c)
	for _, client := range c.spaces {
		client.Close()
	}
	return nil
}

func (c *ClientSpace) Status() (*tuplespace.Status, error) {
	// Collect status from all clients into statuses.
	lock := sync.Mutex{}
	statuses := []*tuplespace.Status{}
	action := func(c *service.ClientSpace) error {
		status, err := c.Status()
		if err == nil {
			lock.Lock()
			statuses = append(statuses, status)
			lock.Unlock()
		}
		return err
	}

	if err := newBatch(c).All(action).Wait(); err != nil {
		return nil, err
	}
	status := &tuplespace.Status{}
	for _, s := range statuses {
		status.Tuples.Count += s.Tuples.Count
		status.Tuples.Seen += s.Tuples.Seen
		status.Waiters.Count += s.Waiters.Count
		status.Waiters.Seen += s.Waiters.Seen
	}
	return status, nil
}

func (c *ClientSpace) Send(tuple tuplespace.Tuple, expires time.Duration) error {
	return c.SendMany([]tuplespace.Tuple{tuple}, expires)
}

func (c *ClientSpace) SendMany(tuples []tuplespace.Tuple, expires time.Duration) error {
	batch := newBatch(c)
	for _, tuple := range tuples {
		batch.Random(func(s *service.ClientSpace) error {
			return s.Send(tuple, expires)
		})
	}
	return batch.Wait()
}

func (c *ClientSpace) SendWithAcknowledgement(tuple tuplespace.Tuple, expires time.Duration) error {
	return newBatch(c).
		Random(func(s *service.ClientSpace) error { return s.SendWithAcknowledgement(tuple, expires) }).
		Wait()
}

func (c *ClientSpace) Read(match string, timeout time.Duration) (out tuplespace.Tuple, err error) {
	err = newBatch(c).
		Random(func(c *service.ClientSpace) error {
		return nil
	}).
		Wait()
	return
}

func (c *ClientSpace) ReadAll(match string, timeout time.Duration) ([]tuplespace.Tuple, error) {
	b := newBatch(c)
	tuples := make(chan []tuplespace.Tuple, b.Size())
	b.All(func(c *service.ClientSpace) error {
		t, err := c.ReadAll(match, timeout)
		if err != nil {
			return err
		}
		tuples <- t
		return nil
	})
	if err := b.Wait(); err != nil {
		return nil, err
	}
	out := []tuplespace.Tuple{}
	for t := range tuples {
		out = append(out, t...)
	}
	return out, nil
}

func (c *ClientSpace) Take(match string, timeout time.Duration, reservationTimeout time.Duration) (reservation *service.ClientReservation, err error) {
	reservations := make(chan *service.ClientReservation)
	err = newBatch(c).First(func(cancel cancelAction, c *service.ClientSpace) error {
		errors := make(chan error)
		// reservations := make(chan *service.ClientReservation)
		go func() {
			reservation, err := c.Take(match, timeout, reservationTimeout)
			if err != nil {
				errors <- err
			} else {
				reservations <- reservation
			}
		}()
		select {
		case err := <-errors:
			return err
		case reservation = <-reservations:
			close(cancel)
		case <-cancel:
			c.Close()
		}
		return err
	}).Wait()
	return
}

// A batch operation on a space.
type batch struct {
	tomb tomb.Tomb
	c    *ClientSpace
}

func newBatch(c *ClientSpace) *batch {
	return &batch{c: c}
}

func (b *batch) Wait() error {
	return b.tomb.Wait()
}

func (b *batch) Size() int {
	return b.c.clients.Size()
}

// Run action on all clients in parallel. If all clients fail, the batch will
// fail. A single success results in success.
func (b *batch) All(action func(*service.ClientSpace) error) *batch {
	clients := b.c.allClients()
	errors := make(chan error)

	// Goroutine to manage errors.
	b.tomb.Go(func() error {
		var lastError error
		expected := len(clients)
		failed := 0
		for i := 0; i < expected; i++ {
			err := <-errors
			if err != nil {
				failed++
				lastError = err
			}
		}
		if failed == len(clients) {
			return lastError
		}
		return nil
	})

	for addr, client := range clients {
		b.tomb.Go(func() error {
			err := action(client)
			if err != nil {
				b.c.clients.Dead(addr)
			}
			errors <- err
			return nil
		})
	}
	return b
}

type cancelAction chan struct{}

func (c cancelAction) Cancel() {
	close(c)
}

// Issue action to all clients in parallel, and accept the first.
func (b *batch) First(action func(cancelAction, *service.ClientSpace) error) *batch {
	cancel := make(cancelAction)
	b.All(func(c *service.ClientSpace) error {
		errors := make(chan error)
		go func() { errors <- action(cancel, c) }()
		select {
		case err := <-errors:
			if err != nil {
				cancel.Cancel()
			}
			return err

		case <-cancel:
			c.Close()
			// FIXME: Don't close the TCP connection here. Instead, close and
			// reopen the muxed connection.
			b.c.clients.Dead(c.RemoteAddr())
			return nil
		}
	})
	return b
}

// Run action repeatedly until it succeeds or it reaches the maximum number of
// retries.
func (b *batch) Random(action func(*service.ClientSpace) error) *batch {
	boff := backoff.NewExponentialBackOff()
	boff.Reset()
	work := func() error {
		errors := make(chan error)
		for {
			key, client := b.c.randomClient()
			go func() { errors <- action(client) }()
			select {
			case err := <-errors:
				if err == nil {
					return nil
				}
				b.c.clients.Dead(key)
				d := boff.NextBackOff()
				if d == backoff.Stop {
					return err
				}
				time.Sleep(d)

			case <-b.tomb.Dying():
				client.Close()
				return nil
			}
		}
	}
	b.tomb.Go(work)
	return b
}
