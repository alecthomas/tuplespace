# A RESTful tuple space server [![Build Status](https://travis-ci.org/alecthomas/tuplespace.png)](https://travis-ci.org/alecthomas/tuplespace)

This is an implementation of a [tuple space](http://www.mcs.anl.gov/~itf/dbpp/text/node44.html) as a RESTful HTTP service.

## Background

A tuple space is a shared communication region where tuples are posted to the region and receivers take or read tuples that match constraints.

The original definition of a tuple space supported tuples in the formal sense:

		("age", 10)
		("age", 20)

and tuple matches in this form:

		("age", ?)

This turns out to be quite restrictive, so this implementation is really *inspired* by tuple spaces, but not strictly adherent to the formal definition.

## Features

This implementation has the following constraints and/or features:

- "Tuples" are arbitrary maps (not tuples). *Strict tuples are less obvious and more restrictive in how they can be used.*
- Tuples can be matched using arbitrary expressions. eg. `age > 36`. *Again, to bypass the restrictions of strict tuple matching.*
- Senders can wait (with optional timeout) for a tuple to be "processed" by a receiver.
- Supports reservations. A reservation is a set of read/take operations that are either all marked as "processed" or none are. If a transaction is not committed or aborted within a timeout, the transaction times out and is rolled back.


## Example

Send a tuple and wait for it to be processed:

		$ curl --data-binary '{"tuples": [{"age": 20}], "ack": true}' http://localhost:2619/tuplespaces/users/tuples
		{"s":201}

Take a tuple (also marking it as processed):

		$ curl -X DELETE http://localhost:2619/tuplespaces/users/tuples
		{"s":200,"d":[{"age":20}]}

## Caveats

- No replication.
- No reservations (transactions).
- Matching (read/take) is currently very naive and simply iterates over all tuples.

## Plans

- Implement fault tolerance (possibly using Raft).
- Implement reservations.
- Implement indexing on tuple fields to speed up matching.

## Glossary

- **Tuple**: A fixed length ordered collection of typed values.
- **Match**: A tuple-like object that is matched against tuples in the tuplespace. Null elements will match any value, and non-null elements match the same value in a tuple.

## Operations

The following extended tuplespace operations are supported:

- `Send(tuple, timeout)` - Send a tuple into the tuplespace, with an optional timeout.
- `SendMany(tuples, timeout)` - Send tuples into the tuplespace, with an optional timeout.
- `Take(match, timeout) -> Tuple` - Take (read and remove) a tuple from the tuplespace, with an optional timeout.
- `Read(match, timeout) -> Tuple` - Read a tuple from the tuplespace, with an optional timeout.
- `TakeAll(match, timeout) -> []Tuple` - Take (read and remove) all tuples from the tuplespace, with an optional timeout.
- `ReadAll(match, timeout) -> []Tuple` - Read all tuples from the tuplespace, with an optional timeout.

## RESTful tuplespace server

### Installation

Install the server and client with:

```bash
$ go get github.com/alecthomas/tuplespace/bin/tuplespaced
$ go get github.com/alecthomas/tuplespace/bin/tuplespace
```

### Running the RESTful API server

Run the server with:

```bash
$ tuplespaced --bind=0.0.0.0:2619
```

By default the server will listen on `127.0.0.1:2619`.

### Running the client binary

Send a tuple:

```bash
$ tuplespace send '["cmd", "uname -a"]'
```

Read a tuple:
```bash
$ tuplespace read '["cmd", null]'
("cmd", "uname -a")
```

### RESTful API

All requests should be sent with the following headers:

```
Content-Type: application/json
Accept: application/json
```

Following are the RESTful equivalents for each tuplespace operation.

#### Errors

If a non-2XX response is returned the response will be JSON in the following format:

```python
{
	"error": <error>
}
```

#### Send()

###### URL

	POST /tuplespace/

###### JSON payload

```python
{
	"tuples": [<tuple>],         # The tuples to insert.
	"timeout": <nanos>           # 0 (or omitted) means no timeout
}
```

###### Response status

- `201 Created` - Tuple was created successfully.
- `500 Internal Server Error` - Tuple could not be created.

###### Response payload

```python
{}
```

A tupleid is an unsigned 64-bit integer.


#### Read()

###### URL

	GET /tuplespace/

###### JSON payload

```python
{
	"match": <tuple>,             # See above for details.
	"timeout": <nanos>,           # Time to wait for tuple (can be omitted to wait indefinitely).
	"all": <bool>,                # If true, return all matching tuples. If omitted or false, return one.
}
```

###### Response status

- `200 OK` - Tuple was read successfully.
- `504 Gateway Timeout` - Timed out waiting for tuple.
- `500 Internal Server Error` - Any other tuplespace error.

###### Response payload

	{ ... }                         // Payload body is the tuple.

#### Take()

###### URL

	DELETE /tuplespace/

###### JSON payload

```python
{
	"match": <tuple>,             # See above for details.
	"timeout": <nanos>,           # Time to wait for tuple (can be omitted to wait indefinitely).
	"all": <bool>,                # If true, take all matching tuples. If omitted or false, take one.
}
```

###### Response status

- `200 OK` - Tuple was taken successfully.
- `504 Gateway Timeout` - Timed out waiting for tuple.
- `500 Internal Server Error` - Any other tuplespace error.

###### Response payload

```python
{
	"tuples": [<tuple>, ...]      # List of matching tuples.
}
```

