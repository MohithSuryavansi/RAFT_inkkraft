# Inkraft - Distributed Collaborative Whiteboard

Inkraft is a real-time collaborative drawing platform built on a fault-tolerant distributed architecture. It enables multiple users to draw simultaneously while maintaining consistency across replicas using the Raft consensus algorithm.

## Features

- 🎨 **Real-Time Collaborative Drawing**
  - Multiple users can draw together with instant synchronization using WebSocket communication.
  - Supports live propagation of strokes and canvas updates across connected clients.

- ⚡ **Raft-Based Distributed Consensus**
  - Implements leader election, heartbeat monitoring, and automatic failover.
  - Ensures consistent drawing state through replicated logs across multiple nodes.

- 🔄 **Fault Tolerance & Recovery**
  - Handles replica crashes and dynamically elects a new leader.
  - Restores failed nodes through log synchronization after recovery.

- 🌐 **Gateway Architecture**
  - Routes client requests to the active leader.
  - Tracks leader changes and broadcasts committed updates to all users.

- 🐳 **Containerized Deployment**
  - Packaged frontend, gateway, and replica nodes using Docker.
  - Tested distributed behavior, scaling, and failure recovery using Kubernetes.

## Tech Stack

- Python
- aiohttp
- WebSockets
- Raft Consensus Algorithm
- Docker
- Kubernetes

## Project Structure

```
RAFT_inkraft/
│
├── frontend/        # Collaborative whiteboard UI
├── gateway/         # WebSocket gateway and leader routing
├── replica/         # Raft consensus replica nodes
└── docker-compose.yml
```

## Architecture

```
Client(s)
    ↓
Gateway
    ↓
Raft Leader
    ↓
Replica Nodes
    ↓
Consensus + Log Replication
```

## Workflow

1. Users draw on the shared canvas.
2. Drawing actions are sent to the gateway via WebSocket.
3. Gateway forwards updates to the current Raft leader.
4. Leader replicates logs across follower replicas.
5. After consensus, updates are broadcast to all connected users.

## Running the Project

### Clone Repository

```bash
git clone <repository-url>
cd RAFT_inkraft
```

### Start Cluster

```bash
docker compose up --build
```

This launches:
- Frontend whiteboard client
- Gateway service
- Raft replica cluster

Open:

```text
http://localhost:9000
```

Open multiple browser tabs to test collaborative drawing.

## Testing Fault Tolerance

Stop a replica:

```bash
docker stop inkraft-replica1
```

Monitor Raft leader election:

```bash
docker compose logs -f
```

Restart the replica:

```bash
docker start inkraft-replica1
```

Stop all services:

```bash
docker compose down
```

## Goal

To demonstrate how distributed consensus algorithms can power reliable real-time collaborative applications with consistency, high availability, and fault tolerance.
