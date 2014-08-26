# A RESTful tuple space server [![Build Status](https://travis-ci.org/alecthomas/tuplespace.png)](https://travis-ci.org/alecthomas/tuplespace)

This is an implementation of a [tuple space](http://www.mcs.anl.gov/~itf/dbpp/text/node44.html) as a RESTful HTTP service.

 It implementation has the following constraints and/or features:

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

## Features

### Reservations

A reservation is effectively a two-phase commit, where a client maintains exclusive access to a tuple for the period of the reservation. The client can then mark the tuple as processed or cancelled, or it may time out. A tuple is returned to the space if it times out or is cancelled.

## Operations

The following extended tuplespace operations are supported:

- `Send(tuple, timeout)` - Send a tuple into the tuplespace, with an optional timeout.
- `SendMany(tuples, timeout)` - Send tuples into the tuplespace, with an optional timeout.
- `Take(match, timeout) -> Tuple` - Take (read and remove) a tuple from the tuplespace, with an optional timeout.
- `Read(match, timeout) -> Tuple` - Read a tuple from the tuplespace, with an optional timeout.
- `TakeAll(match, timeout) -> []Tuple` - Take (read and remove) all tuples from the tuplespace, with an optional timeout.
- `ReadAll(match, timeout) -> []Tuple` - Read all tuples from the tuplespace, with an optional timeout.

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
