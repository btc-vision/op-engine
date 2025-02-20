# OP-Engine: A High-Throughput, Reorg-Aware Rust Storage Layer

OP-Engine is a Rust-based storage engine tailored for **OPNet**, an overlay protocol that adds smart-contract capabilities on top of Bitcoin. This system is designed to handle massive reads/writes, large data sizes (terabytes), and complex reorganization (reorg) requirements — all while offering flexible indexing, crash safety, and both hot/cold storage tiers.

---

## Table of Contents

1. [Overview](#overview)
2. [Key Features](#key-features)
3. [Architecture](#architecture)
    1. [Data Model & Collections](#data-model--collections)
    2. [Hot vs Cold Storage](#hot-vs-cold-storage)
    3. [Concurrency & Threading](#concurrency--threading)
    4. [Memory Management](#memory-management)
    5. [Persistence & Failsafe](#persistence--failsafe)
4. [Reorg Handling](#reorg-handling)
5. [Serialization](#serialization)
6. [Indexing](#indexing)
7. [Creating a New Collection](#creating-a-new-collection)
10. [License](#license)

---

## Overview

- **Purpose**: Manage all data for OPNet (e.g. blocks, transactions, UTXOs, contract states) with high throughput and robust support for Bitcoin-like chain reorganizations.
- **Core Requirements**:
    1. **Threading** for parallel reads/writes.
    2. **In-memory caching** and deferring writes to disk.
    3. **Crash recovery** through write-ahead logs (WAL).
    4. **Full reorg support**: revert to a previous chain state seamlessly.
    5. **Optimized** for large-scale blockchain data (terabytes).
    6. **Dynamic** support for various hardware environments.
    7. **Hot/Cold** storage for performance vs. cost trade-offs.
    8. **Multiple indexes** and composite keys for advanced queries.
    9. **Custom serialization** for efficient, binary-based data encoding (no JSON).

---

## Key Features

- **Multi-threaded**: Uses Rust concurrency primitives to safely handle high-volume parallel operations.
- **Collection-based**: Each type of data (blocks, transactions, UTXOs, etc.) is stored in its own “collection,” with configurable indexes and schemas.
- **In-memory & On-disk**: Keeps recent writes in memory for speed, flushes periodically to disk or cold storage.
- **Crash Safety**: WAL-based approach ensures minimal data loss; can replay uncommitted transactions after a crash.
- **Reorganization Support**: Can revert to a previous block height on demand, discarding orphaned chain data.
- **Hot vs. Cold**: Move older data to cheaper/slower media, reduce the footprint of frequently-accessed data.

---

## Architecture

### Data Model & Collections

**OP-Engine** organizes all data into **collections**. Each collection corresponds to a specific data type:

- **Blocks** (block headers, raw block data, metadata)
- **Transactions** (raw bytes, indexed by TxID, block pointer, etc.)
- **UTXOs** (indexed by `(TxID, outputIndex)` and address or script)
- **Smart Contracts** (state data, possibly multi-indexed)
- **Wallet/Key** metadata (public keys, user data)

Each collection defines:
- **Primary Index**: Uniquely identifies an item (e.g., `(TxID, outputIndex)` for UTXOs).
- **Optional Secondary Indices**: For queries by different fields (e.g., `address -> [UTXOs]`).
- **Custom serialization** rules.

**Data Flow**:
1. **Incoming Data** → **In-memory store (memtable)** → (optionally) → **WAL** → **Flush to segment files on disk**.
2. **Lookups** check memtable first, then disk/cold storage segments.

---

### Hot vs Cold Storage

1. **Hot Storage**:
    - Typically an SSD or high-speed disk.
    - Stores recent data (e.g., the last `N` blocks, unspent UTXOs).
    - Enables quick lookups and updates.

2. **Cold Storage**:
    - Could be a slower HDD, tape, or remote object store.
    - Stores finalized (deep) chain data.
    - Often compressed or archived to save space.

**Policy**: Configurable. E.g.:
- _“After `X` confirmations, move the block or transaction data to cold storage.”_

---

### Concurrency & Threading

- **Thread-Pool**: A pool of worker threads can handle read/write operations in parallel.
- **Sharded Collections**: Optionally split large data sets by key range or hash (e.g., shard by `TxID` prefix). Each shard manages its own WAL and memtable to reduce contention.
- **Locking**:
    - Fine-grained locks at the shard or index level, rather than global locks.
    - Or use lock-free/atomic data structures where possible (e.g., crossbeam skiplist, concurrent B-Trees).

---

### Memory Management

1. **In-Memory Caches** (Memtables):
    - Act as high-speed buffers for writes.
    - Once a memtable reaches a threshold (size or time-based), it’s flushed (written) to disk.
2. **Snapshots**:
    - When flushing, create a snapshot (checkpoint) of the state to allow crash recovery.
    - The snapshot references a consistent set of on-disk segment files + offsets.

---

### Persistence & Failsafe

1. **Write-Ahead Log (WAL)**:
    - Before data is stored in the memtable, it’s written sequentially to a WAL file.
    - On crash, **replay** the WAL to restore the in-memory data that wasn’t fully flushed.

2. **Segment Files** (LSM-like Approach):
    - Flushed memtables become immutable segment files.
    - Over time, we **merge** segment files to remove outdated entries (e.g., spent UTXOs).
    - Each segment is versioned with a block height range for easy reorg discards.

3. **Checkpoints**:
    - After a successful flush, record a checkpoint that ties the WAL offsets to the newly created segment files.
    - This checkpoint is used to:
        - Mark safe WAL segments that can be truncated or archived.
        - Identify the last consistent state in a crash scenario.

---

## Reorg Handling

Bitcoin-like reorgs require rolling back all changes after a certain block height:

1. **Snapshot / Delta Approach**:
    - We keep track of changes per block or per small batch of blocks.
    - On reorg, discard the segments (or block deltas) that belong to the orphaned chain.

2. **Reorg API**:
    - A method (e.g. `reorg_to(height: u64)`) triggers the database to:
        1. Identify segments associated with blocks above `height`.
        2. Mark or delete them (move them to an "orphaned" state).
        3. Roll back in-memory indexes to reflect the previous chain’s state.

3. **Consistency**:
    - Reorg must be atomic across all collections so that blocks, transactions, UTXOs, and contract states are rolled back together.
