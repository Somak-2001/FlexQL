# FlexQL Design Document

## Repository

Add your GitHub repository link here before submission.

## Overview

FlexQL is a lightweight SQL-like client/server database driver implemented fully in C++. The system is split into:

- A TCP database server that parses and executes SQL-like commands
- A C-style client API layer that communicates with the server
- An interactive REPL built on top of the client API

The implementation supports `CREATE TABLE`, `INSERT`, `SELECT`, single-condition `WHERE`, and `INNER JOIN`.

## Storage Design

### Row-Major Storage

The database uses a row-major in-memory layout:

- Each table stores a vector of rows
- Each row stores a vector of string values aligned with the schema
- This makes inserts simple and keeps row reconstruction cheap during `SELECT *`

### Schema Representation

Each table keeps:

- A list of columns with name, type, and primary-key metadata
- A hash map from column name to column index
- A row container

Supported types:

- `INT`
- `DECIMAL`
- `VARCHAR`
- `DATETIME`

Type checks are performed during insertion. `DATETIME` values are normalized internally to epoch seconds so comparisons remain simple.

## Indexing Method

Primary indexing is implemented as a hash map:

- Key: primary-key value
- Value: row offset inside the table storage

This design makes exact-match lookups on the indexed column efficient and keeps insert overhead low.

Current assumption:

- At most one primary-key column per table

## Query Execution

### CREATE TABLE

- Parses schema metadata
- Verifies duplicate column names are not present
- Registers the table in the in-memory catalog

### INSERT

- Validates row width against schema
- Validates type compatibility
- Enforces unique primary-key values
- Stores an optional expiration timestamp for the row

### SELECT

- Supports `SELECT *` and projected columns
- Supports a single `WHERE` predicate
- Supports `INNER JOIN ... ON ...`

### WHERE Restriction

- Only one condition is supported
- No `AND` or `OR`

## Expiration Handling

Each row stores an optional expiration timestamp in epoch seconds.

Supported insertion forms:

- `TTL <seconds>`
- `EXPIRES AT 'YYYY-MM-DD HH:MM:SS'`

Expired rows are removed lazily during future access to the table. This avoids doing background cleanup work while still preserving correctness.

## Caching Strategy

FlexQL uses an LRU result cache for `SELECT` queries.

- Cache key: normalized SQL text
- Cache value: complete query result including columns and rows
- Capacity: 128 query results

The cache is invalidated whenever:

- A table is created
- A row is inserted
- Expired rows are purged

This keeps cached results correct after writes.

## Multithreading Design

The server follows a thread-per-client model:

- The listening thread accepts incoming TCP connections
- Each client connection is handled by a worker thread
- Shared database state is protected with `std::shared_mutex`
- The query cache has its own mutex

This allows multiple clients to issue queries concurrently while preventing race conditions on shared structures.

## Networking Design

Client and server communicate through a simple framed protocol:

- Every message is prefixed with a 32-bit payload length
- The client sends one SQL statement per request
- The server replies with result headers, row frames, and a terminal `END` frame

This avoids ambiguity from newline-based parsing and makes binary-safe message transfer straightforward.

## Performance Notes

The implementation is designed to scale reasonably for assignment workloads through:

- Row-major storage for cheap appends
- Hash-based primary indexing
- LRU caching for repeated reads
- Lazy expiration cleanup

The included benchmark utility can be used to measure:

- Insert throughput
- Point-select latency

## Compilation Instructions

```bash
make
```

## Execution Instructions

Start the server:

```bash
./flexql_server 9000
```

Start the REPL:

```bash
./flexql_repl 127.0.0.1 9000
```

Run the benchmark:

```bash
./flexql_benchmark 127.0.0.1 9000 10000
```

## Verification

Local verification completed for:

- Build success with `make`
- SQL parser and engine smoke test with `CREATE`, `INSERT`, `SELECT`, `WHERE`, `TTL`, and `INNER JOIN`

Because the current sandbox isolates long-running listening processes from separate client processes, the full TCP round-trip could not be validated here in one shared runtime, but the server, client library, and engine all build successfully.
