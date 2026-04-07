# FlexQL

FlexQL is a simplified SQL-like database driver written in C++ for the Design Lab assignment. It provides:

- A TCP server
- A C-style client API
- A REPL client
- Persistent on-disk storage
- Write-ahead logging and crash recovery
- Primary-key indexing
- Query-result caching
- Batch insert support

## Project Structure

- `flexql.h`: public client API
- `flexql.cpp`: client API implementation
- `engine.h`, `engine.cpp`: SQL parser, execution engine, storage, recovery
- `protocol.h`, `protocol.cpp`: framed client/server protocol
- `flexql_server.cpp`: benchmark-compatible server entrypoint
- `server_main.cpp`: standalone server entrypoint
- `repl_main.cpp`: interactive REPL client
- `benchmark_flexql.cpp`: professor benchmark client
- `smoke_test.cpp`: basic feature test
- `persistence_test.cpp`: restart persistence test
- `DESIGN.md`: design document

## Features

### Supported SQL

- `CREATE TABLE`
- `INSERT`
- Batch `INSERT INTO ... VALUES (...), (...), ...`
- `SELECT *`
- `SELECT column1, column2`
- `WHERE` with one condition
- `INNER JOIN`

### Supported Operators

- `=`
- `>`
- `<`
- `>=`
- `<=`
- `!=`

### Supported Types

- `DECIMAL`
- `VARCHAR`

### Expiration Support

- `TTL <seconds>`
- `EXPIRES AT 'YYYY-MM-DD HH:MM:SS'`

## Persistence and Recovery

FlexQL stores durable state inside `flexql_data/`.

- `catalog.txt`: schema metadata
- `tables/*.rows`: checkpointed table rows
- `wal.log`: write-ahead log

Every `CREATE TABLE` and `INSERT` statement is first appended to the WAL and synced to disk. After that, the in-memory state is updated and checkpointed back to the catalog and table snapshot files. On restart, FlexQL reloads the snapshots and replays any newer WAL entries that were not checkpointed yet.

## Build

Build everything:

```bash
make
```

Build the professor benchmark workflow binaries:

```bash
sh compile.sh
```

This produces:

- `./server`
- `./benchmark`

## Run

### Benchmark Workflow

Terminal 1:

```bash
sh compile.sh
./server
```

Terminal 2:

```bash
./benchmark --unit-test
./benchmark 1000
```

### Manual REPL Workflow

Terminal 1:

```bash
make flexql_repl
./server
```

Terminal 2:

```bash
./flexql_repl 127.0.0.1 9000
```

Exit the REPL with:

```text
.exit
```

## Example Queries

```sql
CREATE TABLE TEST_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);
INSERT INTO TEST_USERS VALUES (1, 'Alice', 1200, 1893456000);
INSERT INTO TEST_USERS VALUES (2, 'Bob', 450, 1893456000), (3, 'Carol', 2200, 1893456000);
SELECT * FROM TEST_USERS;
SELECT NAME, BALANCE FROM TEST_USERS WHERE ID = 2;
SELECT NAME FROM TEST_USERS WHERE BALANCE > 1000;
```

Join example:

```sql
CREATE TABLE TEST_ORDERS(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);
INSERT INTO TEST_ORDERS VALUES (101, 1, 50, 1893456000), (102, 1, 150, 1893456000), (103, 3, 500, 1893456000);
SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT
FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID
WHERE TEST_ORDERS.AMOUNT > 100;
```

## Tests

Run the basic feature test:

```bash
./smoke_test
```

Run the persistence test:

```bash
g++ -std=c++17 -O2 -Wall -Wextra -pedantic persistence_test.cpp engine.cpp -o persistence_test
./persistence_test
```

Run the professor benchmark unit tests:

```bash
./benchmark --unit-test
```

## Notes

- The durable source of truth is on disk, not only in RAM.
- RAM is used for active tables, indexes, and cache.
- Batch insert support is included for better benchmark performance.
- The current durability design checkpoints after each mutating statement, which is simpler and safer but slower than a more advanced database design.
- TTL rows are persisted with their absolute expiration timestamp and are removed lazily on later access.

## Submission Files

For submission, the important files are:

- Source code
- `README.md`
- `DESIGN.md`
- Benchmark-compatible workflow using `./server` and `./benchmark`
