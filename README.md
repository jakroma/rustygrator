# **Rustygrator**

- [**Rustygrator**](#rustygrator)
  - [Process overview](#process-overview)
    - [Diagrams](#diagrams)
  - [Run](#run)
    - [cargo](#cargo)
    - [Temporary files (dbc v3)](#temporary-files-dbc-v3)
    - [Live transfer (CDC)](#live-transfer-cdc)
    - [docker](#docker)
    - [Desktop app release](#desktop-app-release)
    - [Tests](#tests)
    - [Benchmarks](#benchmarks)
  - [TODO](#todo)
    - [DB](#db)
      - [Steps of migration](#steps-of-migration)
    - [Migrations](#migrations)
    - [SQL -\> Nosql settings](#sql---nosql-settings)
    - [Live data transfer](#live-data-transfer)
    - [Functions](#functions)


## Process overview

### Diagrams

One-off migration, optionally followed by live streaming (`--live`):

```mermaid
flowchart TD
    SRC[("Source database<br/>PostgreSQL · MySQL · Oracle · SQLite · MongoDB · ScyllaDB")] --> SCHEMA
    SCHEMA{"Does the source have a schema?"}
    SCHEMA -->|"yes: SQL, ScyllaDB"| CATALOG["Read tables, keys, indexes<br/>from the system catalog"]
    SCHEMA -->|"no: MongoDB"| ANALYZE["Analyze documents<br/>infer fields and types"]
    CATALOG --> DEFS
    ANALYZE --> DEFS
    DEFS["Data definitions + migration order<br/>(parents before children)"] --> TRANSFER
    TRANSFER{"--transfer / --mode"}
    TRANSFER -->|"channel (default, one process)"| CHANNEL["Bounded in-memory channels<br/>column batches"]
    TRANSFER -->|"file, or provide + receive"| DBC["Encrypted dbc files<br/>compressed column blocks"]
    CHANNEL --> RECEIVER
    DBC -->|"same or another process (--key-file)"| RECEIVER
    RECEIVER["Data Receiver<br/>create tables, load rows,<br/>then indexes and foreign keys"] --> TARGET[("Target database")]
    RECEIVER --> LIVE{"--live?"}
    LIVE -->|no| DONE(["Done"])
    LIVE -->|yes| BATCHES["Batches of inserts / updates / deletes"]
    SRC -. change capture, started before the snapshot .-> BATCHES
    BATCHES --> APPLY["Receiver: upserts and deletes by key"]
    APPLY --> TARGET
    APPLY --> CHECKPOINT["Checkpoint saved<br/>(a restart continues here)"]
    CHECKPOINT --> BATCHES
```

## Run

### cargo
```
cargo build --release
./target/release/rustygrator -f <provider url> -t <receiver url> [options]
```

Connection strings:

| Database   | Example                                                                 |
| ---------- | ----------------------------------------------------------------------- |
| PostgreSQL | `postgresql://user:password@127.0.0.1:5433/test?schema=public`          |
| MongoDB    | `mongodb://root:example@127.0.0.1:27019/test` (any driver URL incl. `?replicaSet=`, `mongodb+srv://`) |
| SQLite     | `sqlite://path/to/file.db`, `sqlite:///absolute/path.db`, `sqlite://D:/data/file.db` |
| ScyllaDB   | `scylla://[user:password@]host1[:9042][,host2...]/keyspace` (`cassandra://` works too) |
| MySQL      | `mysql://user:password@host[:3306]/database` (`mariadb://` too; `?sslmode=require\|verify-ca\|verify-full`, `sslrootcert=`, `sslcert=`, `sslkey=` like PostgreSQL). A receiver creates the database if it is missing |
| Oracle     | `oracle://user:password@host[:1521]/service_name` (Easy Connect). Schema = the user unless `?schema=SHOP`; `?case=upper` folds target table / column names to upper case; `?sslmode=require|verify-ca|verify-full` uses TCPS with the wallet in the folder of `sslrootcert`. Needs [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html) on `PATH` (Windows) / `LD_LIBRARY_PATH` (Linux); without it only Oracle connections fail. Oracle has no empty string: `''` from other databases is stored as NULL |

`?schema=` on a PostgreSQL provider limits the migration to that schema; on a receiver it is the target schema (created if missing, default `public`).

Main options (`rustygrator --help` lists all):

| Option | Default | Meaning |
| ------ | ------- | ------- |
| `--mode full\|provide\|receive` | `full` | run provider and receiver together, or only one side (`provide` writes definitions + `dbc` files, `receive` imports them later / elsewhere) |
| `--transfer channel\|file` | `channel` | `full` mode: in-memory bounded channels or through `dbc` files |
| `--temp-dir` | `./dbcTemp` | definitions, `dbc` data files, migration order, checkpoint |
| `-p, --pagination-limit` | `4096` | rows per batch (start value of the adaptive batch size) |
| `--adaptive-batch`, `--min-batch`, `--max-batch`, `--max-batch-mb` | `true`, `256`, `16384`, `8` | batch size tuned from the measured throughput, capped in rows and memory |
| `--tl` | `4` | batches buffered between provider and receiver |
| `--parallel-tables` | CPU cores | independent tables (no foreign key between them) migrated at the same time |
| `--parallel-ranges` | `1` | readers per large table (PostgreSQL key ranges / MongoDB `_id` ranges on one snapshot) |
| `--strategy denormalized\|normalized` | `denormalized` | SQL -> MongoDB / ScyllaDB: embed referenced rows (documents / frozen UDTs) or keep references |
| `--compression lz4\|zstd\|none`, `--zstd-level` | `lz4`, `3` | dbc block compression |
| `--block-rows` | `8192` | rows per dbc block |
| `--key-file` | | dbc key for separate provide / receive runs (else `DM_DBC_KEY`, else a one-time key) |
| `--keep-temp` | off | keep the temporary files after a successful run |
| `--scylla-replication`, `--scylla-consistency` | `{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}`, `local_quorum` | ScyllaDB receiver keyspace / write consistency |
| `--scylla-concurrency`, `--scylla-token-ranges` | `64`, `32` | ScyllaDB requests in flight, token ranges read per table |
| `--scylla-key table=(a,b),c` | | ScyllaDB key override: partition key `a, b`, clustering key `c` (repeatable) |
| `--include a,b` / `--exclude c` | | tables / collections (full `schema.table` or short name) |
| `--schema-sample N` | `0` (all) | MongoDB provider: documents analyzed to infer the schema |
| `--live` | off | after the initial copy keep streaming changes |
| `--batch-size`, `--batch-wait-ms` | `500`, `1000` | live: events per batch / max wait for a batch |
| `--checkpoint-file` | `<temp-dir>/checkpoint.json` | live: resume position |
| `--slot-name`, `--publication` | `data_mover_slot`, `data_mover_pub` | live with a PostgreSQL provider |
| `--max-retries`, `--stats-interval-secs` | `10`, `10` | live: retries with backoff, statistics log interval |

Loads are idempotent for tables with a primary key: running the same migration again updates rows instead of duplicating them.

### Temporary files (dbc v3)

Data files (`<table>_data.dbc`) are column-oriented, compressed (LZ4 by default) and encrypted with AES-256-GCM (ChaCha20-Poly1305 on CPUs without AES instructions); definitions, migration order and checkpoint are encrypted containers (`*.dmb`). Every block is authenticated together with the header and its block number, and all files are verified before the first row is written, so a wrong key, a changed or cut file, or an old v2 file stops the run with an error.

* Provider and receiver in one run: a random key that only exists in memory - nobody can read the files, not even a later run.
* Separate `--mode provide` / `--mode receive` runs: the same `--key-file` or passphrase in `DM_DBC_KEY` (stretched with Argon2id) on both sides. The key is never written to the files or the logs.
* Live mode keeps its checkpoint in an encrypted container too: use `--key-file` / `DM_DBC_KEY` to resume after a restart (with a one-time key a restart takes a new snapshot).
* The temporary directory and files are private to the current user (Unix permissions; on Windows they inherit the folder's ACL) and are removed after a successful run unless `--keep-temp`.

### Live transfer (CDC)

`--live` first copies everything, then streams inserts / updates / deletes until stopped with Ctrl+C or SIGTERM (the current batch is flushed and the checkpoint saved). Restarting with the same checkpoint file continues where it stopped without a new snapshot.

| Provider   | Requirement |
| ---------- | ----------- |
| PostgreSQL | `wal_level=logical`, a user allowed to create replication slots / publications; tables need a primary key. Remove a slot you no longer use (`SELECT pg_drop_replication_slot('data_mover_slot')`), otherwise PostgreSQL keeps WAL for it. |
| MongoDB    | replica set or sharded cluster (change streams) |
| SQLite     | none; rustygrator installs triggers and a `_data_mover_changelog` table in the source file |
| ScyllaDB   | vnode keyspace (CDC does not work with tablets); rustygrator enables `cdc` on the streamed tables |
| MySQL      | `binlog_format=ROW` and `binlog_row_image=FULL` (MySQL 8 defaults), a user with `REPLICATION SLAVE` and `REPLICATION CLIENT`; tables need a primary key. MariaDB works for migrations, not for live mode |
| Oracle     | `ARCHIVELOG` mode and supplemental logging (`ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` for complete rows; `(PRIMARY KEY) COLUMNS` gives key + changed columns), a user with `LOGMINING`, `SELECT ANY TRANSACTION`, `SELECT_CATALOG_ROLE`, `EXECUTE_CATALOG_ROLE`; tables need a primary key. LOB changes made after the insert are not streamed |

Any provider can stream into any receiver, e.g. MySQL into PostgreSQL:
```
rustygrator -f mysql://repl:secret@db1/shop -t postgresql://user:pw@db2/shop --live --key-file shop.key
```
Use `--key-file` (or `DM_DBC_KEY`) for live runs: the checkpoint is encrypted, and without a fixed key a restart cannot read it and copies everything again. Changes are applied in batches (`--batch-size`, `--batch-wait-ms`) as upserts / deletes by primary key, so delivering an event twice is harmless; `--include` / `--exclude` choose the streamed tables. Live paths and their tests: [SPEC.md section 5.3](SPEC.md).

### docker
1. Build image
```
docker build -t rustygrator .
```

2. Run image
```
docker run --rm --network host rustygrator rustygrator -f <provider url> -t <receiver url>
```

### Desktop app release
```
scripts/release.sh [--skip-tests]
```
Runs the desktop tests, builds `rustygrator-desktop` in release mode for this machine's platform and writes `target/dist/rustygrator-desktop-<version>-<target>.tar.gz` plus a `.sha256` file. On macOS the archive holds `Rustygrator.app`; on Windows / Linux the executable. Build on each platform you ship for; signed installers (`.dmg`, `.msi`, AppImage) need signing certificates and are not made yet.

### Tests
```
cargo test --workspace                       # unit tests + SQLite integration tests
e2e/run-tests.sh                             # every database in docker-compose
```
All scripts are bash (on Windows: Git Bash or WSL).
`e2e/docker-compose.test.yml` starts PostgreSQL (logical replication), MongoDB (replica set), ScyllaDB, MySQL (row-based binlog) and Oracle Free (ARCHIVELOG, LogMiner) and runs the whole suite, including every migration direction and live transfer:
```
docker compose -f e2e/docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from tests
```
Against your own databases: set `DM_TEST_PG_URL` / `DM_TEST_MONGO_URL` / `DM_TEST_SCYLLA_URL` / `DM_TEST_MYSQL_URL` / `DM_TEST_ORACLE_URL` (an administrator of a PDB, e.g. `oracle://system:pw@host:1521/FREEPDB1`) and run `cargo test --workspace -- --include-ignored`.

Test data: `dumps/shop/` has the same shop data set (keys, foreign keys, unicode, nulls, decimals, json, bytes, uuid, dates) for PostgreSQL, MySQL, Oracle, SQLite, MongoDB and ScyllaDB; `core/tests/dumps.rs` loads each one and migrates it. Load one by hand with `psql -f`, `mysql <`, `sqlplus @`, `sqlite3 x.db <`, `cqlsh -k <keyspace> -f`.

### Benchmarks
```
cargo bench -p rustygrator-core                                  # micro: dbc v2 vs v3, conversions
e2e/run-benchmarks.sh [quick|full] [--save-baseline]            # end to end
cargo run --release -p rustygrator-core --example profile -- <from> <to> [flamegraph.svg]
```
`e2e/docker-compose.bench.yml` starts PostgreSQL, MongoDB and ScyllaDB with fixed versions and CPU / memory limits and runs `rustygrator-bench`: every migration path on generated, repeatable data sets (narrow 5 columns, wide 100 columns, 1 MiB values, foreign keys, nested documents; 10k rows in the quick profile, 10k / 1M / 10M rows in the full one), a live (CDC) run under constant write load (events/s, lag p50 / p95 / p99) and a memory check (peak memory must not grow with the table size). Each run reports time, rows/s, MB/s, peak RSS, CPU and dbc size as a table and as JSON in `e2e/bench-results/`; with a saved baseline the run fails when a scenario is more than 10 % slower or uses more than 10 % more memory.

## TODO

### DB

#### Steps of migration

| Database    | Generate simple tables from provider | Import to receiver db | Tests |
| ----------- | :----------------------------------: | :-------------------: | :---: |
| Postgressql |          :white_check_mark:          |  :white_check_mark:   | :white_check_mark: |
| Mongodb     |          :white_check_mark:          |  :white_check_mark:   | :white_check_mark: |
| SQLite      |          :white_check_mark:          |  :white_check_mark:   | :white_check_mark: |
| ScyllaDB    |          :white_check_mark:          |  :white_check_mark:   | :white_check_mark: |
| MySQL       |          :white_check_mark:          |  :white_check_mark:   | :white_check_mark: |
| Oracle      |          :white_check_mark:          |  :white_check_mark:   | :large_blue_circle: |

Type mapping: [postgresql_data_types.rs](core/src/db/postgresql/postgresql_data_types.rs), [mysql_data_types.rs](core/src/db/mysql/mysql_data_types.rs), [oracle_data_types.rs](core/src/db/oracle/oracle_data_types.rs), [mongodb_data_types.rs](core/src/db/mongodb/mongodb_data_types.rs), [sqlite_data_types.rs](core/src/db/sqlite/sqlite_data_types.rs), [scylladb_data_types.rs](core/src/db/scylladb/scylladb_data_types.rs).

### Migrations

|             |    Postgressql     |      Mongodb       |       SQLite       |      ScyllaDB      |       MySQL        |
| ----------- | :----------------: | :----------------: | :----------------: | :----------------: | :----------------: |
| Postgressql |                    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| Mongodb     | :white_check_mark: |                    | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| SQLite      | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: | :white_check_mark: |
| ScyllaDB    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| MySQL       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

Rows: source, columns: target. MySQL ↔ PostgreSQL / SQLite / MongoDB / MySQL are tested (`core/tests/mysql.rs`, `core/tests/dumps.rs`); MySQL ↔ ScyllaDB goes through the same provider / receiver code without a test of its own.

Oracle ↔ every other database: written and unit tested; the tests against an Oracle server (`core/tests/oracle.rs`, `dump4_oracle_shop`) have not run yet (:large_blue_circle: in the tables).


### SQL -> Nosql settings

| Strategy     |      Mongodb       |      ScyllaDB      |
| ------------ | :----------------: | :----------------: |
| Denormalized | :white_check_mark: | :white_check_mark: |
| Normalized   | :white_check_mark: | :white_check_mark: |

### Live data transfer

| Database    | Change capture                      |                    |
| ----------- | ----------------------------------- | :----------------: |
| Postgressql | logical replication (`pgoutput`)    | :white_check_mark: |
| Mongodb     | change streams                      | :white_check_mark: |
| SQLite      | triggers + changelog table          | :white_check_mark: |
| ScyllaDB    | ScyllaDB CDC log tables             | :white_check_mark: |
| MySQL       | row-based binlog (read as a replica) | :white_check_mark: |
| Oracle      | LogMiner (redo / archive logs)       | :large_blue_circle: |

Live paths (rows: source, columns: target); :white_check_mark: tested end to end, :large_blue_circle: supported through the same code, no test of its own yet:

|             | PostgreSQL | MySQL | SQLite | MongoDB | ScyllaDB |
| ----------- | :--------: | :---: | :----: | :-----: | :------: |
| PostgreSQL  | :large_blue_circle: | :large_blue_circle: | :white_check_mark: | :white_check_mark: | :large_blue_circle: |
| MySQL       | :large_blue_circle: | :large_blue_circle: | :white_check_mark: | :large_blue_circle: | :large_blue_circle: |
| SQLite      | :white_check_mark: | :large_blue_circle: | :white_check_mark: | :large_blue_circle: | :large_blue_circle: |
| MongoDB     | :white_check_mark: | :large_blue_circle: | :large_blue_circle: | :large_blue_circle: | :large_blue_circle: |
| ScyllaDB    | :white_check_mark: | :large_blue_circle: | :large_blue_circle: | :large_blue_circle: | :large_blue_circle: |

### Functions

| Name                            |   Functionality    |
| ------------------------------- | :----------------: |
| Channels                        | :white_check_mark: |
| Data by file                    | :white_check_mark: |
| Indexes                         | :white_check_mark: |
| Data by enumerator              | :white_check_mark: |
| Migration order                 | :white_check_mark: |
| specify schema import           | :white_check_mark: |
| specify temporary file location | :white_check_mark: |
