# A RESTful tuple space server [![Build Status](https://travis-ci.org/alecthomas/tuplespace.png)](https://travis-ci.org/alecthomas/tuplespace)

This is an implementation of a [tuple space](http://www.mcs.anl.gov/~itf/dbpp/text/node44.html) as a RESTful HTTP service.

It has the following constraints and/or features:

- "Tuples" are arbitrary maps. *Strict tuples are less obvious and more restrictive in how they can be used.*
- Tuples can be matched using arbitrary expressions. eg. `age > 36`. *Again, to bypass the restrictions of strict tuple matching.*
- Senders can wait (with optional timeout) for a tuple to be processed by a receiver.

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

A reservation is effectively a two-phase commit, where a client maintains exclusive access to a tuple for the period of the reservation. The client can then mark the tuple as processed or cancelled, or it may time out. A tuple is returned to the space if it times out or is cancelled. No other clients may consume the tuple while it is reserved.

All `Take()` operations are issued within a reservation.

### Scalability

The tuple space service can be scaled by simply adding nodes. Clients discover new server nodes by repeatedly reading tuples from a management space.

## Background

A tuple space is a shared communication medium where tuples are posted and receivers take or read tuples that match a pattern.

The original definition of a tuple space supported tuples in the formal sense:

		("age", 10)
		("age", 20)

and tuple matches in this form:

		("age", ?)

This turns out to be quite restrictive, so this implementation is really only *inspired* by tuple spaces and not strictly adherent to the formal definition.## RESTful tuplespace server

### Installation

Install the server and client with:

```bash
$ go get github.com/alecthomas/tuplespace/bin/tuplespaced
$ go get github.com/alecthomas/tuplespace/bin/tuplespace
```
