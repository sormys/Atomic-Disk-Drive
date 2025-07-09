# Atomic Disk Drive

**Note:** This project was developed as an assignment for a Distributed Systems university course.

---

A Rust implementation of a distributed block device. This project provides a user-space library that uses an **(N, N)-Atomic Register** algorithm to create a fault-tolerant storage system.

---

## Core Concept

The system ensures **linearizability** for read and write operations across multiple nodes. This means that despite being distributed, the device behaves like a single, consistent unit.

The core logic is based on a two-phase protocol:
1.  **Read Phase:** A process queries a majority of nodes to find the most recent data version.
2.  **Write Phase:** The process instructs a majority of nodes to update to this new version.

The system remains operational as long as a majority of processes are active, making it resilient to crashes.

---

## Architecture

The solution is an asynchronous library built on the **Tokio** runtime for efficient I/O handling. It is structured around several key components:

* **`AtomicRegister`**: Implements the core register logic for a single data sector.
* **`SectorsManager`**: Handles efficient and atomic persistent storage of sectors on the filesystem.
* **`RegisterClient`**: Manages the internal TCP communication between processes.

To achieve high throughput, operations on different sectors are handled concurrently.

---

## Communication Protocol

Communication relies on a custom binary protocol over TCP. All messages are secured with **HMAC-SHA256** to ensure integrity and authenticity.

* **Client Communication:** External clients (e.g., a Linux driver) issue `READ` and `WRITE` commands to the system.
* **Internal Communication:** Processes exchange messages to coordinate state and execute the atomic register algorithm.

---

## Key Features

* **Fault Tolerance:** Designed to handle process crashes and recoveries without data loss.
* **Efficient Storage:** Uses minimal disk space overhead for storing sector data and metadata.
* **High Performance:** Built for concurrency to handle many simultaneous operations.
* **Secure:** All network traffic is authenticated to prevent unauthorized modifications.