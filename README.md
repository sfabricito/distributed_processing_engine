# **Distributed Processing Engine**

### *A Minimal Distributed Dataflow System Implemented from Scratch (Rust)*

**Course:** Principles of Operating Systems
**Professor:** Kenneth Obando 
**Authors:** Fabricio Solís Alpízar, Pavel Zamora Araya
**License:** MIT

---

## **1. Overview**

This project implements a fully-functional distributed batch data-processing engine from scratch, featuring a custom master–worker architecture, a DAG-based execution model, and a complete pipeline for parallel operator processing across data partitions. It is built entirely in `Rust`, a safe and performant systems programming language, the engine provides explicit control over concurrency, scheduling, fault handling, and distributed coordination. The project is designed as a learning exercise in Operating Systems and Distributed Systems, emphasizing multi-threading, IPC, networking, memory management, and cluster-level orchestration.
It demonstrates fundamental concepts including:

The key concepts correspond to:

* Distributed process/thread concurrency
* Master–worker coordination, worker registry, and heartbeats
* Task scheduling, load-aware balancing, and retry policies
* Partitioned data storage with spill-to-disk support
* Shuffle, repartitioning, and stage-based DAG execution
* CPU-bound operator execution and IO-bound data access patterns
* Fault detection, re-execution, and idempotent task attempts
* Metrics, structured logging, and system-level observability

Notes:

* No external distributed frameworks (Spark, Flink) are used.
* Scheduling, execution, coordination, partitioning, and operator logic are implemented manually.
* The engine is scalable and supports stage-parallel execution across multiple workers and an extensible operator layer driven by a JSON-based DAG specification.
* Intended primarily for education and experimentation rather than production-grade deployment.

---

## **2. Features Summary**

### **Distributed Architecture**

The system follows a classic **master–worker distributed architecture**, where a single coordinator manages scheduling, job state, and worker health, while multiple workers execute tasks in parallel. This design enables horizontal scalability and isolates failures to individual nodes.

* Master–Worker cluster
* Worker registry with heartbeats and status reporting  
* Load-aware round-robin task scheduling
* Distributed partitions with dynamic task routing

### **DAG Execution**

Jobs are defined through a **Directed Acyclic Graph (DAG)** that specifies operators, dependencies, and execution order. The engine interprets this DAG to construct multi-stage pipelines and orchestrates execution across workers.

* Multi-stage pipeline construction
* Automatic stage scheduling across workers
* Shuffle and repartition logic for key-based operations

### **Operators Implemented**

The engine provides a set of core operators that enable flexible data transformation and aggregation across partitions. Each operator is executed independently per partition to maximize data parallelism.

* `map` – element-wise transformation
* `filter` – predicate-based filtering 
* `flat_map` – expansion into multiple records  
* `map_flap` – custom flat-map variant 
* `reduce` – aggregation of partition data
* `reduce_by_key` – group-by and key-based reduction
* `read` – ingestion from CSV/JSONL input files

CHANGE
    
### **Worker Runtime**

Workers handle the actual computation of tasks. Each worker features a **multi-threaded runtime**, a local partition manager, and fault-handling mechanisms to ensure reliability during distributed execution.

* Multi-threaded task executor 
* In-memory partitions with disk spill for large datasets 
* Task retry and fault propagation  
* Worker-side monitoring and status updates

### **Master Runtime**

The master node is responsible for global orchestration. It manages job lifecycles, assigns tasks to workers, and tracks the execution progress of each stage, ensuring deterministic and reproducible job execution.

* FIFO task queue and global scheduler
* Worker load metrics and selection strategies
* Persistent job state management
* DAG parsing, validation, and planning   

### **Execution Model**

The engine uses a **partition-based execution model**, where datasets are split into independent partitions that flow through DAG stages. This approach enables concurrent processing of partitions, reliable results, fault-tolerant execution, and scalable parallelism.

* Partition-parallel execution across workers
* Deterministic, reproducible transformations
* Retry-on-failure with per-task attempt tracking
* Idempotent task identifiers
* Persistent job metadata for recovery
    
### **API**

A lightweight HTTP API enables interaction with the cluster for job submission, progress tracking, and retrieving metadata. Requests use a JSON-based format for easy integration.

* Job submission endpoint
* Job and stage status reporting
* Partition and task metadata   
* Cluster and worker information
    
### **Observability**

The system includes basic observability tools to diagnose issues, measure performance, and track distributed execution at runtime. Logs and metrics provide visibility at both the master and worker levels.

* Structured logging per node
* Worker heartbeat monitoring
* Execution status reporting  
* Debug-level detail for operator execution
    
### **Deployment**

The engine is designed for flexible deployment, supporting both standalone multi-process setups and containerized environments using Docker. Configuration is customizable through environment files.

* Multi-process execution on a single machine
* Full cluster startup via docker-compose
* .env-based configuration for ports, roles, and master address
* JSON-based DAG submission through a CLI or HTTP client

## **3. Project Structure**

### **Project Structure**

The repository is organized into a modular architecture that cleanly separates concerns across the master node, worker node, HTTP interface, shared logic, and operator implementations. This structure ensures maintainability, clear component boundaries, and straightforward extensibility for additional operators or scheduling policies. Each directory plays a dedicated role in the engine’s functionality, allowing the system to behave like a miniature distributed runtime.

````
distributed_processing_engine/
├── Cargo.toml
├── README.md
├── envExample
├── src/
│   ├── main.rs                # Entry point (master/worker launcher)
│   ├── lib.rs
│   ├── common/                # Shared utilities and domain types
│   │   ├── config.rs          # Cluster and node configuration
│   │   ├── dag.rs             # DAG parsing & node definitions
│   │   ├── state_store.rs     # Basic persistence for job metadata
│   │   ├── types.rs           # Core structs (Partition, Job, Task)
│   ├── http/                  # Lightweight custom HTTP interface
│   │   ├── server.rs          # Socket listener + HTTP 1.1 handler
│   │   ├── handlers.rs        # Routes for job submission, status, etc.
│   │   ├── router.rs          # Request -> handler dispatch
│   │   ├── request.rs         # HTTP parsing utilities
│   ├── master/
│   │   ├── registry.rs        # Worker registration & heartbeats
│   │   ├── scheduler.rs       # Global task queue + scheduling logic
│   ├── worker/
│   │   ├── executor.rs        # Multi-threaded task runner
│   │   ├── monitor.rs         # Worker self-reporting and health
│   │   ├── partition.rs       # Partition storage, loading, spill-to-disk
│   │   └── ops/               # Operator implementations
│   │       ├── read.rs
│   │       ├── map.rs
│   │       ├── filter.rs
│   │       ├── reduce.rs
│   │       ├── map_flap.rs
│   │       └── mod.rs
└── target/

````

Overall, the structure mirrors the architecture of modern data-processing engines: a centralized coordinator, distributed executors, a clear operator layer, and an HTTP entrypoint enabling external control of job execution.

## **4. Requirements**

This project is designed to run in a UNIX-like environment and relies solely on the Rust standard library plus lightweight third-party crates when necessary. No distributed frameworks are used, so the system interacts directly with the OS for networking, threading, and filesystem operations.

### **System Requirements**

To build and run the engine, ensure the following are installed:

* **Linux or WSL2 (recommended):** ideal for running multiple processes and Docker-based clusters.
* **Rust stable toolchain:** the project is built using Cargo and targets stable Rust.
* **Docker & docker-compose (optional):** required only if you choose to run the master and workers inside containers for a full multinode simulation.
    
### **Build Tools**

To build the project in release mode:

`cargo build --release`

This produces optimized binaries suitable for running multi-worker setups.

### **Configuration**

The project reads configuration values from either:

* .env file (example provided in envExample), or
* the Config struct located in common/config.rs.

These settings define:

* Node role (master or worker)
* Master address
* Ports 
* Heartbeat interval
* Worker thread pool size
* Partition directory
    
This makes deployment flexible and reproducible across environments.

## **5. Build & Run**

The engine supports both **single-node** and **multi-node** execution. In single-node mode, the master and workers run on the same machine as separate processes. This setup is ideal for development, debugging, and small workloads.

### **Single Node (Master + Worker)**

To launch the cluster manually:

**Terminal 1 — Start the Master Node**

`cargo run -- master`

The master initializes:

* the registry
* scheduler
* DAG planner
* HTTP control interface
* persistent job store
  
It then waits for workers to register.

**Terminal 2 — Start a Worker Node**

`cargo run -- worker`

Upon startup, the worker:

* Loads its configuration
* Registers with the master
* Starts its heartbeat loop
* Spawns the multi-threaded executor
* Prepares the partition manager (in-memory + spill directory)
    
Workers automatically appear in the master’s registry as soon as they connect. You may start **multiple** workers to scale execution:

`cargo run -- worker`
`cargo run -- worker`
`cargo run -- worker`

Each worker contributes additional compute resources, enabling parallel execution across partitions and DAG stages.

## **6. Architecture**

### High-Level Diagram

The distributed engine follows a **master–worker model**, where all coordination, scheduling, and job management is centralized in the master node, while workers are responsible solely for executing tasks over assigned partitions. This separation of concerns enables scalable parallelism, fault isolation, and clear responsibility boundaries across the system.

Below is a high-level representation of the system architecture and data flow:

````

               +------------------+
               |      Client      |
               +---------+--------+
                         |
                         v
               +---------+--------+
               |      Master      |
               |  - Scheduler     |
               |  - Registry      |
               |  - DAG Planner   |
               +---------+--------+
                         |
          +--------------+---------------+
          |                              |
          v                              v
 +--------+---------+           +--------+---------+
 |      Worker 1    |           |      Worker 2    |
 | - Executor       |           | - Executor       |
 | - Partitions     |           | - Partitions     |
 +------------------+           +------------------+
````

The master orchestrates every stage of execution, while each worker independently processes partitions using its own local executor and storage backend.

### Master Node

The **master node** acts as the control plane of the system. It is responsible for maintaining a global view of the cluster, receiving jobs from clients, decomposing DAGs into executable stages, and scheduling tasks across available workers. The master does not perform computation itself; instead, it ensures efficient distribution and reliable execution of work across the cluster.

Internally, the master implements:

* **registry.rs** – Tracks active workers, manages heartbeats, and maintains worker load metrics used for scheduling.
* **scheduler.rs** – Provides a global FIFO task queue and a load-aware round-robin scheduling policy.
* **common/dag.rs** – Parses the user’s JSON DAG, performs dependency analysis, and builds stage execution plans.

The master manages the job lifecycle end-to-end:

* ACCEPTED → QUEUED → RUNNING → COMPLETED / FAILED
* Assigns tasks to workers and monitors progress
* Handles task retries and rescheduling after failures
* Orchestrates shuffle and repartition operations between stages
* Determines final job outputs and stores persistent metadata

Through these responsibilities, the master ensures correctness, determinism, and resilience within the distributed engine.

### Worker Node

The **worker node** serves as the execution layer of the system. Each worker receives tasks assigned by the master and processes them using local compute threads and storage resources. Workers operate autonomously but maintain continuous communication with the master to report status, health, and results.

A worker implements:

* A **multi-threaded executor** (executor.rs) capable of running multiple tasks in parallel
* A **partition manager** (partition.rs) for loading, storing, and spilling partitions to disk when memory limits are exceeded
* **Operator execution modules** (ops/mod.rs) implementing map, filter, flat_map, reduce, reduce_by_key, and data ingestion
* A **health & heartbeat monitor** (monitor.rs) that reports liveness and prevents stale workers from staying in the registry
    
Workers are designed to be **stateless relative to job coordination**: if a worker crashes, the master simply reassigns its tasks, enabling a simple but effective model for fault recovery.

### **HTTP Server**

To facilitate external interaction, the system includes a custom **lightweight HTTP server** that allows clients to submit jobs and query their results. The server is intentionally minimal, using manual request parsing and routing to preserve visibility into OS-level behavior.

The HTTP layer provides:

* **Raw request parsing** for methods, paths, and JSON bodies
* A **router** that dispatches endpoints to specific handlers
* **Handlers** that translate HTTP requests into master-level commands
* JSON-encoded responses for job IDs, statuses, and result metadata

This design enables the engine to be controlled programmatically via curl, scripts, or other HTTP-capable tools, without relying on external web frameworks.

## **7. API Specification**

The system exposes a simple HTTP API that allows clients to interact with the cluster, submit workloads, and retrieve results. All APIs accept and return **JSON**, making them easy to integrate with scripts or higher-level automation tools.

### **POST /jobs — Submit a Job**

Submits a new DAG-based job to the master.The request body includes:

* Job name
* DAG nodes (operators)
* Edges (dependencies)
* Input paths and partition count

On success, returns:

* Assigned job ID
* Initial state (ACCEPTED)

### **GET /jobs/{id} — Job Status**

Retrieves detailed information about a job’s execution state.The response includes:

* Overall job state:ACCEPTED / RUNNING / FAILED / SUCCEEDED
* Current stage and stage progress
* Partition assignment details
* Retry counts
* Any detected worker failures
  
This endpoint enables clients to poll jobs in real time.

### **GET /jobs/{id}/results — Job Results**

Returns the output generated by the final stage of the DAG.Depending on the operator chain, the response may include:

* File paths for each partition output
* A merged output artifact (optional)
* Metadata such as partition sizes and operator timings

This endpoint allows downstream systems or users to retrieve and consume the final results of the distributed computation.

CHANGE

## **8. Operators**

The engine provides a set of core data-transformation operators that form the foundation of its distributed computation model. Each operator is implemented in the worker/ops/ module and is applied at the partition level, enabling fine-grained parallelism across the cluster. Together, these operators support a wide range of workloads.

### **Transformations**

The following operators are fully implemented and can be combined to form complex dataflows defined in the user’s DAG:

OperatorDescription**map**Applies a unary function to each element of the partition, producing a transformed partition of equal size.**filter**Retains only the elements that satisfy a user-defined predicate.**flat_map**Expands each input element into zero, one, or many elements, enabling tokenization, decomposition, and other multi-output transformations.**reduce**Aggregates all elements in a partition into a single value using an associative reduction function.**reduce_by_key**Performs key-based grouping and aggregation, distributing intermediate values into buckets before reducing each group.**read**Ingests raw data from CSV or JSONL input files and splits it into partitions for downstream processing.

CHANGE

These operators are intentionally kept low-level and explicit, providing transparency into how real distributed engines execute transformations internally.

### **Execution Model**

Operator execution is fully distributed and parallelized across workers. Each DAG stage is decomposed into **per-partition tasks**, which workers execute in isolation. This model provides fault tolerance, concurrency, and reproducibility.

Operators run on:

* **Individual partitions**, ensuring that large datasets can be processed incrementally and independently
* **Parallel worker thread pools**, leveraging multicore CPUs for increased throughput
* **A retry-aware execution path**, where failed tasks can be reissued to healthy workers without impacting other partitions
    
Because operators do not maintain shared state across partitions, this architecture minimizes contention and simplifies reasoning about correctness—an approach modeled after modern distributed systems.

## **9. Execution Model**

The overall execution model follows a **Batch DAG architecture**, similar to Spark’s RDD transformation pipeline. Jobs are not executed line-by-line; instead, the system analyzes the full DAG, identifies stage boundaries, and processes the data through clearly defined phases.

### **Batch DAG**

Distributed execution follows these steps:

1. **Parse the DAG** – The user submits a JSON DAG describing operators and dependencies; the master validates and loads it.
2. **Resolve dependencies** – The DAG is topologically sorted to determine the order of execution and stage boundaries.
3. **Stage-level execution** – Each stage is executed in parallel across workers, with tasks corresponding to individual partitions.
4. **Shuffle & repartition** – For key-based operations (e.g., reduce_by_key), data is redistributed across workers according to partitioning logic.
5. **Reduce final stage** – Final aggregation or consolidation is performed to produce the final dataset.
6. **Collect output** – Partition results are written to disk and returned via the API.
    
This staged execution ensures determinism, fault isolation, and efficient distribution of work through the cluster.

CHECK

### **Partition Model**

The engine processes data at the **partition** level, allowing control over memory, concurrency, and scheduling. The partition model defines how data flows through operators:

* Input data files are **split into partitions**, each representing an independent chunk of data. 
* Each operator runs **separately** on every assigned partition, enabling parallel computation. 
* Intermediate results **propagate** from one stage to the next, preserving the full dataflow described by the DAG.
    
This model mirrors real-world distributed engines, where partition-level parallelism is fundamental for scalability and robustness.

## **10. Metrics & Observability**

Observability is essential in distributed systems, where failures, delays, and load imbalances can occur across multiple nodes. The engine includes logging and metrics tools that provide insights into cluster behavior, operator performance, and data movement.

The system tracks and exposes:

* **Worker load:** Number of active tasks, CPU usage (approx), and outstanding queue depth
* **Task attempt count:** Number of retries, failed executions, and overall task stability
* **Partition sizes:** Memory footprint and spill behavior for large datasets
* **Operator duration per stage:** Execution time for each operator on per-partition and per-stage granularity
* **Job-level state tracking:** High-level progress, completed stages, and final job status
* **Structured logs:** Detailed logs emitted by each node (master and workers) for debugging and auditing

CHECK
    
These metrics allow users to measure performance, and verify the correctness of distributed execution. They also form the foundation for future enhancements such as autoscaling or adaptive scheduling.

## **11. Testing**

Reliable distributed systems require comprehensive testing to validate correctness, resilience, and compliance with the expected execution model. This project incorporates several layers of tests to ensure that each subsystem—ranging from low-level operators to full-cluster execution—functions predictably under diverse conditions.

### **Unit Tests**

Unit tests validate the smallest, most isolated components of the engine. These tests ensure that core logic behaves correctly without the involvement of networking, concurrency, or distributed coordination.

They cover:

* **Operator correctness:** Verifies the behavior of map, filter, flat_map, reduce, and other operators for correctness and determinism.
* **DAG parsing:** Ensures that job definitions are validated properly, dependencies resolved, and malformed DAGs rejected gracefully.
* **Partition operations:** Tests reading, serialization, spill-to-disk, and partition workflows to ensure reproducibility and consistency.
    
Unit testing these areas helps guarantee that the computational layer remains correct regardless of cluster size or load.

### **Integration Tests**

Integration tests validate the interactions between multiple subsystems operating together, particularly those involving the master-worker protocol and distributed scheduling.

They include:
    
* **Master–Worker communication:** Confirms that registration, task assignment, heartbeats, and status updates work in realistic multi-process scenarios.
* **Registry updates:** Ensures that workers join, update their status, and disconnect correctly, and that the master responds to worker failures.
* **Scheduling flow:** Validates end-to-end scheduling, ensuring tasks are enqueued, distributed, executed, retried when necessary, and finalized.
    
Integration testing is essential for detecting subtle coordination issues such as race conditions, stale worker records, or inconsistent scheduling.

### **End-to-End Tests**

These tests simulate full real-world use of the engine by submitting complete DAG jobs and validating the outputs. End-to-end (E2E) tests ensure that all stages—from job submission to result collection—operate as intended in a running cluster.

E2E tests include:

* **Full DAG job execution:** Submitting a job, monitoring its progress, and validating the final outputs.  
* **Multiple worker environments:** Ensuring that scheduling distributes work efficiently and deterministically under multi-worker setups.
* **Failure + retry demonstration:** Killing a worker mid-task to verify correct reassignment, retry attempts, and preservation of job correctness.
These tests provide confidence that the system behaves robustly under realistic distributed conditions.

## **12. Benchmarks & Performance**

Performance evaluation is a fundamental aspect of distributed systems, where scalability and throughput determine practical usability. This project includes benchmarking recommendations and tests for measuring how the engine behaves under varying workloads, partition counts, and cluster sizes.

### **Suggested Workloads**

The following types of workloads provide insights into operator performance and distributed behavior:

* **WordCount (CSV):** Classic map → flat_map → reduce pipeline, useful for testing shuffle and heavy fanout.
* **JSONL aggregation:** Validates key-based grouping, serialization speed, and reduce_by_key efficiency.
* **Reduce-heavy loads:** Stress-tests CPU-bound operators and verifies the performance of multi-threaded execution.


CHECK

### **Performance Metrics**

Benchmarking should focus on the following dimensions:
* **Stage times:** Time spent by each DAG stage, revealing bottlenecks in parsing, shuffle, or aggregation.
* **End-to-end completion:** Measures overall job duration, from submission to result collection.
* **Worker throughput:** Evaluates tasks processed per second and the impact of worker count.
* **Effect of partition count:** Analyzes how varying the number of partitions affects execution time and resource utilization.
    
These metrics help identify scaling characteristics, operator efficiency, and the effectiveness of the scheduling strategy.

## **13. Fault Tolerance**

Fault tolerance is a core characteristic of distributed engines. The system must remain resilient to worker failures, network delays, and transient errors. This project includes several mechanisms that allow the cluster to recover predictably from node-level failures without compromising job correctness.

### **Fault-Tolerance Features**

The engine supports the following reliability features:

* **Worker drop detection via heartbeats:** The master continuously monitors worker liveness. If a worker stops sending heartbeats, it is marked as DOWN and removed from scheduling rotation.
* **Task retries on new worker:** Any task that was assigned to a dropped or unresponsive worker is automatically rescheduled onto a different worker. This ensures that no DAG stage is left incomplete.
* **Idempotent attempt tracking:** Each task execution attempt is uniquely tracked, allowing the master to differentiate between fresh executions and duplicated attempts caused by failures.

check 

Together, these mechanisms provide a solid foundation for predictable distributed execution, even in scenarios where worker nodes fail unexpectedly or behave unreliably.


## **14. Troubleshooting**

Distributed systems can fail in subtle and unexpected ways due to concurrent execution, network variability, and the decentralized nature of workers. This section summarizes common issues that users may encounter, along with explanations to help diagnose and correct them. These cases are derived from typical distributed-engine behavior and the internal mechanisms implemented in this project.

| Issue | Explanation |
| ----: | ----: |
| **Workers never register** | The master is unreachable, the worker was started with the wrong `MASTER_HOST`/`PORT`, or firewall/network misconfiguration prevents heartbeat connections. |
| **No workers available** | There are no active workers registered (none started or all marked DOWN), so tasks cannot be scheduled until a worker joins. |
| **Job stuck in RUNNING** | A worker may have crashed mid-task or become unresponsive; an unimplemented operator or invalid DAG parameters can also block progress. |
| **Partition missing** | The spill directory may have been cleaned before job completion, or there was insufficient disk space during intermediate stage execution. |
| **Scheduler panic** | The submitted DAG may contain cyclic dependencies or an invalid topological structure, causing the planner to fail during dependency resolution.  check|
| **High task retry counts** | Workers may be failing tasks due to operator errors, insufficient resources, or data corruption; check worker logs for specific error messages. |

In general, inspecting logs from both the master and workers provides immediate insight into runtime behavior. The system’s structured logging and heartbeat reports are designed to make these issues identifiable with minimal effort.

## **15. Security Notes**

Because this project is an academic distributed engine, its security model is minimal and prioritizes transparency of OS-level behavior over production-grade safeguards. Users should be aware of the following considerations:

* **No TLS/HTTPS:** Communication between master and workers is unencrypted, relying on trusted environments such as localhost or controlled networks.  
* **No authentication:** Any client capable of reaching the HTTP API may submit jobs or query the cluster, making it unsuitable for deployment on untrusted or public networks.
* **Inputs must be trusted:** DAG definitions, file paths, and function parameters are accepted without sandboxing. Malformed or malicious input may cause unexpected behavior.
* **Academic scope:** The design assumes a safe environment where nodes behave correctly, and adversarial attack resistance is not a goal.

    
## **16. License**

This project is distributed under the **MIT License**, a permissive open-source license that allows anyone to use, modify, and distribute the software with minimal restrictions.
See the [LICENSE](LICENSE) file for details.

## **17. Conclusion**

This project showcases the core principles behind a distributed batch-processing engine, implemented entirely from scratch to provide full visibility into the underlying operating system mechanisms. The system demonstrates:

* **Master–Worker coordination** for task distribution
* **DAG-based scheduling** for structured, dependency-aware computation
* **Partition-level parallelism** enabling scalable, data-independent execution
* **Multi-threaded worker runtimes** for efficient use of CPU resources
* **Retry logic and fault recovery**, ensuring robustness under worker failures
* **Custom operator implementations** that are key features of data transformation and found in modern distributed systems such as Apache Spark and Flink.
    
Through this approach, the engine bridges theoretical concepts from operating systems with practical distributed execution, offering a hands-on understanding of concurrency, synchronization, cluster coordination, and dataflow computation. It is a solid foundation for further improvements, optimizations, and extensions that could be integrated into real-world data processing systems. If you have any questions or would like to contribute, please feel free to reach out!

A investigation paper detailing design decisions and performance analysis is avaiable at:  
[Distributed Processing Engine](https://example.com/research-paper)

You can reach us at:  
- Fabricio Solis Alpizar:
- Pavel Zamora Araya: 
