# A RESTful tuple space server

This is an implementation of a [tuple space](http://www.mcs.anl.gov/~itf/dbpp/text/node44.html) as a RESTful HTTP service.

The tuple space uses [LevelDB](https://code.google.com/p/leveldb/) Go [bindings](https://github.com/jmhodges/levigo) for persistence.

## Features

Includes an in-process tuple space implementation for Go (eg. `tuplespace.NewTupleSpace(store.NewMemoryStore())`).

A RESTful tuple space server:

```bash
$ tuplespaced &
$ curl -X POST -H "Content-Type: application/json" -d '{"tuples": [["cmd", "uname -a"]]}' -i http://localhost:2619/tuplespace/`
```

Go client:

```go
ts, err := client.NewTupleSpaceClient("http://127.0.0.1:2169/tuplespace/")
ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, time.Minute)
```

Python client:

```python
>>> import tuplespace
>>> ts = tuplespace.TupleSpace()
>>> ts.take(('cmd', None))
('cmd', 'uname -a')
```

## Performance

On a 2012 MacBook Air 11".

`MemoryStore`:

- `SendMany` 1M - 306K tps
- `ReadAll` 1M - 116K tps
- `TakeAll` 1M - 114K tps

`LevelDBStore`:

- `SendMany` 1M - 307K tps
- `ReadAll` 1M - 61K tps
- `TakeAll` 1M - 33K tps

## Caveats

- No replication.
- No reservations (transactions).

## Plans

- Implement fault tolerance (possibly using Raft).
- Implement reservations.
- Implement

## Glossary

- **Tuple**: A fixed length, ordered collection, of typed values.
- **Match**: A tuple-like object that is matched against tuples in the tuplespace. Null elements will match any value, and non-null elements match the same value in a tuple.

## Operations

The following extended tuplespace operations are supported:

### `Send(tuples, timeout)`

Send tuples into the tuplespace, with an optional timeout.

### `Take(match, timeout) -> Tuple`

Take (read and remove) a tuple from the tuplespace, with an optional timeout.

### `Read(match, timeout) -> Tuple`

Read a tuple from the tuplespace, with an optional timeout.

### `TakeAll(match, timeout) -> []Tuple`

Take (read and remove) all tuples from the tuplespace, with an optional timeout.

### `ReadAll(match, timeout) -> []Tuple`

Read all tuples from the tuplespace, with an optional timeout.

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

