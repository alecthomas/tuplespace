# A RESTful tuple space server [![Build Status](https://travis-ci.org/alecthomas/tuplespace.png)](https://travis-ci.org/alecthomas/tuplespace)

This is an implementation of a [tuple space](http://www.mcs.anl.gov/~itf/dbpp/text/node44.html) as a RESTful HTTP service.

- Tuples are inherently unreliable and may disappear at any time, including before their timeout.
- "Tuples" are arbitrary maps.
- Tuples can be matched using arbitrary expressions. eg. `age > 36 && id % 2 == 0`.
- Senders can wait (with optional timeout) for a tuple to be processed by a receiver.
- Subsequent reads may return the same tuple.
- Take operations are performed inside a [reservation](#reservations).

## Example

In two separate terminals, run the following commands at the same time.

Send a tuple and wait for it to be processed:

		$ curl --data-binary '{"tuple": {"age": 20}, "ack": true}' http://localhost:2619/tuplespaces/users/tuples
		{"s":201}

Take a tuple (also marking it as processed):

		$ curl -X DELETE http://localhost:2619/tuplespaces/users/tuples
		{"s":200,"d":{"age":20}}

## Operations

The following operations are supported:

- `Read(match, timeout) -> Tuple` - Read a single tuple from the tuple space.
- `ReadAll(match, timeout) -> []Tuple` - Read all matching tuples from the tuple space.
- `Send(tuple, expires)` - Send a tuple.
- `SendMany(tuples, expires)` - Send many tuples at once.
- `SendWithAcknowledgement(tuple, expires)` - Send a tuple and wait for it to be processed.
- `Take(match, timeout, reservationTimeout) -> Reservation`

`TakeAll()` is not supported. To support such an operation would require server-server coordination, which is deliberately outside the scope of the server implementation.

## Features

### Reservations

A reservation (`Take()`) provides timed, exclusive access to a tuple. If neither a `Complete()` nor a `Cancel()` are called on the reservation before the reservation times out, it is returned to the tuple space.

### Scalability

Due to the constraints around the provided operations, the tuple space service can be scaled by simply adding nodes. Clients discover new server nodes by periodically reading all tuples from a management space. All clients connect to all servers simultaneously.

- `Read()` should be issued to multiple servers at once. One result is then used, and the remainder discarded.
- `Take()` should be issued to multiple servers at once. One reservation is then used and the rest are cancelled.
-  `Send()` and `SendWithAcknowledgement()` should be sent round-robin to all servers.

### Installation

Install the server and client with:

```bash
$ go get github.com/alecthomas/tuplespace/bin/tuplespaced
$ go get github.com/alecthomas/tuplespace/bin/tuplespace
```
