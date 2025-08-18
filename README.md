# MultiPaxos Chatbot (ZooKeeper-inspired Coordinator)

A lightweight distributed coordination system that implements **Multi-Paxos** for leader-based consensus and exposes cluster state/operations through a **chat/LLM interface**. The system runs multiple nodes (proposer/acceptor/learner roles), maintains an ordered replicated log, and supports majority-quorum fault tolerance—conceptually similar to ZooKeeper semantics, but implemented with Paxos and without using Apache ZooKeeper.

---

## Tech Stack

- **Language:** Python 3.10+
- **Core:** Multi-Paxos (proposer/acceptor/learner), majority quorum, replicated log
- **Runtime:** `asyncio` / sockets (custom networking)
- **LLM Integration:** Pluggable provider via `llm_integration.py` (API-key based)
- **Config:** `config.py` (cluster membership, ports, timeouts)

---

## What It Does

- **Leader-based consensus (Multi-Paxos):** Elects/stabilizes a leader to streamline proposals (Phase 1 amortization), commits values to a replicated, totally-ordered log.
- **Quorum replication:** Writes require a majority of acceptors; the system tolerates minority failures and preserves safety under partitions.
- **Replicated state machine (RSM):** Learners apply committed log entries; deterministic state evolves consistently across nodes.
- **Networking layer:** `network_server.py` routes messages across nodes; `node.py` implements Paxos roles and heartbeats.
- **Chat/LLM interface:** `llm_integration.py` lets you query cluster status (leader, log, quorum health) and issue safe operations via natural language (configurable provider).

---


