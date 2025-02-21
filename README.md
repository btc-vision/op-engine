# OP-Engine: A High-Throughput, Reorg-Aware Storage Layer (Rust)

![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)
![Bitcoin](https://img.shields.io/badge/Bitcoin-000?style=for-the-badge&logo=bitcoin&logoColor=white)
![AssemblyScript](https://img.shields.io/badge/assembly%20script-%23000000.svg?style=for-the-badge&logo=assemblyscript&logoColor=white)
![TypeScript](https://img.shields.io/badge/TypeScript-007ACC?style=for-the-badge&logo=typescript&logoColor=white)
![NodeJS](https://img.shields.io/badge/Node%20js-339933?style=for-the-badge&logo=nodedotjs&logoColor=white)
![NPM](https://img.shields.io/badge/npm-CB3837?style=for-the-badge&logo=npm&logoColor=white)
![Gulp](https://img.shields.io/badge/GULP-%23CF4647.svg?style=for-the-badge&logo=gulp&logoColor=white)
![ESLint](https://img.shields.io/badge/ESLint-4B3263?style=for-the-badge&logo=eslint&logoColor=white)

[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

Welcome to the **Op-Engine** repository, a custom, **lightweight** storage engine in Rust designed for blockchain-like
operations. This database is optimized around **append-only segments**, **reorganization** support (reorgs), and a
**sharded in-memory memtable** for high insertion throughput. It includes:

- **Sharded memtable** for concurrent inserts
- **Segment files** for on-disk persistence
- **B-Tree indexing** for fast lookups in segments
- **Reorg manager** to roll back to a previous block height
- **Configurable concurrency** using a custom thread pool

---

## Table of Contents

1. [High-Level Overview](#high-level-overview)
2. [Core Components](#core-components)
    - [OpNetDB](#1-opnetdb)
    - [Collections and the Key-Value Model](#2-collections-and-the-key-value-model)
    - [ShardedMemTable](#3-shardedmemtable)
    - [SegmentManager](#4-segmentmanager)
    - [ReorgManager](#5-reorgmanager)
    - [ThreadPool](#6-threadpool)
3. [On-Disk Structure](#on-disk-structure)
4. [Reorganization Flow](#reorganization-flow)
5. [Concurrency and Parallelism](#concurrency-and-parallelism)
6. [Memory, Flushing, and Segment Layout](#memory-flushing-and-segment-layout)
7. [Indexing and Lookups](#indexing-and-lookups)
8. [Example Usage](#example-usage)
9. [Low-Level Architecture Details](#low-level-architecture-details)
10. [License](#license)

---

## High-Level Overview

**Op-Engine Database** is a layered system:

1. **Sharded in-memory tables** capture writes in memory for fast insertion.
2. When a shard grows beyond a configurable threshold (the `memtable_size`), data is **flushed** to an **on-disk segment
   **.
3. Each on-disk segment is accompanied by a B-Tree or similar index for rapid lookups without scanning entire files.
4. **Reorg logic** allows rolling back recent writes if the underlying blockchain or ledger rewinds to a lower block
   height.
5. All these pieces are brought together under a single `OpNetDB` instance, which can hold multiple “collections,” each
   collection storing a distinct data model (e.g. `utxo`, `utxo_by_address`, etc.).
6. A custom **thread pool** is used for concurrency, allowing parallel segment indexing, multi-shard insert parallelism,
   and more.

This structure is reminiscent of an **LSM-tree** style engine (in the sense of staged, sorted segments) but with extra *
*reorg** features, a simpler flush model, and custom concurrency.

---

## Core Components

### 1) `OpNetDB`

The primary struct that application code interacts with is:

```rust
pub struct OpNetDB {
    pub config: DbConfig,
    pub thread_pool: Arc<Mutex<ThreadPool>>,
    pub sharded_tables: Arc<RwLock<HashMap<String, ShardedMemTable>>>,
    pub segment_manager: Arc<Mutex<SegmentManager>>,
    pub reorg_manager: Arc<Mutex<ReorgManager>>,
    pub collections: Arc<RwLock<HashMap<String, CollectionMetadata>>>,
}
```

1. **`DbConfig`** holds paths, concurrency settings, memory thresholds, etc.
2. A **thread pool** for background tasks and parallel index loading.
3. A **map of collection name → `ShardedMemTable`** for the in-memory portion.
4. A **`SegmentManager`** for on-disk segment creation, indexing, and rollbacks.
5. A **`ReorgManager`** that knows the current block height and can revert changes.
6. A **collection registry** so that each named collection can be typed and retrieved.

**Lifecycle**:

- **`OpNetDB::new`** initializes threads, sets up data paths, loads existing segments from disk, and configures the
  reorg state (height).
- **`register_collection`** adds a new logical collection name (e.g., `"utxo"`), allocating a new `ShardedMemTable`.
- **`collection`** returns a strongly-typed handle, `Collection<T>`, so you can `insert` or `get` typed records.
- **`flush_all`** forces all memtables to disk, checkpointing the writes.
- **`reorg_to`** allows rolling back segment files to a specified height.

### 2) Collections and the Key-Value Model

A `Collection<T>` is a **logical store** for records of type `T`. Each type `T` implements:

- **`KeyProvider`** to define how to produce a byte array key for the record.
- **`CustomSerialize`** to define how to serialize/deserialize the record bytes.

Hence, each `Collection<T>` is basically a typed key-value store where keys are derived from `T`. The main methods:

- **`insert(record, block_height)`**: store or update an object in the memtable. If the memtable is too large, flush it.
- **`get(&key_args)`**: get a record by key, looking first in memtable, then on disk.

Internally, each `Collection` references the same `SegmentManager` for on-disk data but uses a *distinct* shard in the
`ShardedMemTable`.

### 3) `ShardedMemTable`

The in-memory store for a collection uses multiple shards:

```rust
pub struct ShardedMemTable {
    shards: Vec<Mutex<MemTableShard>>,
    shard_count: usize,
    max_size: usize,
}
```

- Each shard is protected by a separate `Mutex<MemTableShard>` so multiple threads can insert in parallel.
- The `shard_for_key` function chooses which shard to use, typically by hashing the key.
- `insert(key, value)` locks only that shard and updates the map, tracking approximate memory usage.
- If the total memory usage across shards exceeds `max_size`, we flush the entire sharded table to a new **segment**.

**Why Sharded?**

- To reduce lock contention on large writes: instead of one global `Mutex`, we have `shard_count` locks, allowing
  parallel inserts to different shards.

### 4) `SegmentManager`

When data is flushed from a memtable, the `SegmentManager`:

- Writes out a **`.seg` file** containing all key-value pairs from the sharded memtable in an append-like fashion.
- Builds a **B-Tree** index of `(key → file offset)` pairs, then writes it to a separate **`.idx` file**.
- Tracks each segment’s `(start_height, end_height)`, plus its loaded index in memory.

**Key tasks**:

- **`flush_sharded_memtable_to_segment`**: Takes all shard data, writes it to a new `.seg` file, builds the index,
  writes `.idx`.
- **`rollback_to_height(height)`**: Removes any segments whose `end_height > height`, physically deleting `.seg` and
  `.idx` files.
- **`find_value_for_key(collection_name, key)`**: Searches segments in *reverse chronological order*, looking up offsets
  in the B-Tree index, then reading the record from disk.

### 5) `ReorgManager`

Tracks the **current chain height** and allows “reorg to X” if needed. Typically:

1. **Reorg** sets the internal `current_height` to `X`.
2. Tells `SegmentManager` to `rollback_to_height(X)`.
3. Clears any in-memory shards so we don’t have data above that height.

### 6) `ThreadPool`

A simple custom **thread pool** providing:

1. **Fixed number of worker threads**.
2. **Jobs** submitted via `execute(|| { ... })`.
3. A `TaskHandle<T>` to retrieve typed results or wait for completion.

Used for:

- **Parallel loading of segment indexes** on startup.
- Potential concurrency expansions like asynchronous flush or merges.

---

## On-Disk Structure

1. **Data Files (`.seg`)**: Contains raw `(key, value)` pairs appended. Each pair is stored as:
    - `u64` length of the key + the key bytes
    - `u64` length of the value + the value bytes
2. **Index Files (`.idx`)**: A B-Tree or sorted structure with `(key → offset-in-.seg-file)`, read into memory for quick
   lookups.
3. The file naming format is typically:  
   **`{collection_name}_{startBlock}_{endBlock}.seg`** and **`{collection_name}_{startBlock}_{endBlock}.idx`**  
   or sometimes with an added numeric suffix if the same `(startBlock, endBlock)` is used.

Hence, each flush yields a new pair of `.seg` and `.idx` files.

---

## Reorganization Flow

A typical **block pipeline** is:

1. A new block at height `H` arrives, you do several inserts into `Collection<T>` with `block_height = H`.
2. If `sharded_memtable` grows too large, the system flushes. The resulting `.seg` and `.idx` are labeled with
   `[start=H, end=H]`.
3. If a **chain reorg** happens and you must drop blocks above `X`, call `reorg_to(X)`.
    - The **`SegmentManager`** deletes any segment with `end_height > X`.
    - The **sharded memtables** are cleared from memory to remove uncommitted data.

---

## Concurrency and Parallelism

- **Concurrent inserts**: Each `Collection::insert` picks a shard by hashing the key. Only that shard’s mutex is locked,
  allowing multiple threads to insert different keys concurrently.
- **Parallel segment indexing**: On startup, each segment’s `.idx` file is loaded in parallel using the global thread
  pool.
- **Multi-thread flush**: The code can be extended to write different shards in parallel. Currently, the flush is done
  under a single lock in `SegmentManager`, but indexing or writing can further be parallelized.

---

## Memory, Flushing, and Segment Layout

### Memtable Size Threshold

Each collection has a `memtable_size`. Once the total size of data across all shards in the `ShardedMemTable` exceeds
that threshold:

1. We call `SegmentManager::flush_sharded_memtable_to_segment(...)`.
2. The entire memtable for that collection is written out to a `.seg` file.
3. A new B-Tree index is built in memory and then written out as `.idx`.
4. The in-memory memtable is cleared.

**Why entire memtable**?  
We currently do a “full flush”—this is simpler than partial flush. An alternative approach might flush one shard at a
time, or a fraction of the data, but we do a big chunk to reduce overhead.

---

## Indexing and Lookups

### B-Tree Index

Each `.idx` file stores a B-Tree (`BTreeIndex`) mapping `(key → offset in .seg)`. For example:

```text
index: key=someKey -> offset=1234
```

During **lookup**:

1. We search from the *newest segment to oldest*, because the newest segment has the most recent data.
2. If the key is in that B-Tree, we read the offset from disk.
3. We verify the key at that offset matches exactly, then return the value.

### Range Searches

We optionally allow scanning a range of keys (e.g., `[start_key .. end_key]`) with a limit, which merges the results
from newest to oldest segments, deduplicates, and returns up to that many matches.

---

## Example Usage

Below is a **simplified** usage snippet:

```rust
fn main() {
    // Create config
    let config = DbConfig {
        data_path: "mydata/".into(),
        wal_path: "mydata/wal/".into(),
        num_threads: 4,
        memtable_size: 1024 * 1024,
        height: 100,
    };

    // Instantiate database
    let db = OpNetDB::new(config).expect("Failed to init DB");

    // Register a collection
    db.register_collection("utxo").expect("Collection error");

    // Retrieve typed handle
    let utxo_coll = db.collection::<Utxo>("utxo").expect("Get coll error");

    // Insert a UTXO record
    let my_utxo = Utxo {
        tx_id: [0xab; 32],
        output_index: 0,
        address: [0xcd; 33],
        amount: 10_000,
        script_pubkey: vec![0xAA, 0xBB, 0xCC],
        deleted_at_block: None,
    };
    utxo_coll.insert(my_utxo, 101).expect("insert fail");

    // Flush
    db.flush_all(101).expect("flush fail");

    // Lookup
    let found = utxo_coll.get(&([0xab; 32], 0)).unwrap();
    println!("Found => {:?}", found);

    // Reorg
    db.reorg_to(100).expect("reorg fail");
}
```

---

## Low-Level Architecture Details

Below is a **step-by-step** breakdown of the critical paths in the engine, from insertion to on-disk layout:

1. **Insertion**
    - `Collection<T>::insert(record, block_height)`:
        1. Serialize `record` using `CustomSerialize`.
        2. Find the shard to place the key in (`hash(key) % shard_count`).
        3. Insert `(key, serialized_value)` into `MemTableShard`.
        4. If the total size exceeds `memtable_size`, we flush.

2. **Flush**
    - `SegmentManager::flush_sharded_memtable_to_segment`:
        1. Create a `.seg` file with a large buffered writer.
        2. Iterate over each shard:
            - For each `(key, value)`, write them in raw form (`[length, key bytes, length, value bytes]`).
            - Build a **B-Tree** (or in-memory index) mapping `(key → offset_in_file)`.
        3. Write the B-Tree out to a `.idx` file.
        4. Append the new `SegmentMetadata` to `segments` list in memory.
        5. Clear the sharded memtable.

3. **Query** (`Collection<T>::get(key_args)`):
    1. Convert `key_args` to a byte array (`KeyProvider::compose_key`).
    2. Look in the memtable for that key. If found, deserialize and return.
    3. If not found in memtable, search from newest segment to oldest:
        - Use B-Tree index in memory to see if the key offset exists.
        - If offset found, open `.seg` file, read the key-value at that offset, verify, return the value.

4. **Reorg** (`OpNetDB::reorg_to(height)`):
    1. `ReorgManager` sets `current_height` to `height`.
    2. `SegmentManager::rollback_to_height(height)`:
        - Remove segments above this height and delete their `.seg` + `.idx` files from disk.
    3. Clear the in-memory shards to discard unflushed data beyond the old height.

---

## License

This repository is licensed under the MIT License (or your chosen license).  
Feel free to use, modify, and extend this code in your projects!