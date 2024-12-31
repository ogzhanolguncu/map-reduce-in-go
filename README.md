# Distributed MapReduce Implementation in Go

A distributed implementation of the MapReduce based on Google's MapReduce paper.

## Architecture Overview

The system consists of two main components:

### Coordinator (Master)

- Manages the overall MapReduce workflow
- Tracks task states and worker availability
- Handles dynamic worker registration
- Manages fault tolerance through task reassignment
- Coordinates the transition between Map and Reduce phases

### Workers

- Register with the coordinator using RPC
- Execute assigned Map and Reduce tasks
- Handle intermediate file I/O
- Support graceful shutdown and failure reporting
- Provide health status updates via heartbeat

## Features

- **Dynamic Worker Pool**: Workers can join and leave the cluster dynamically
- **Fault Tolerance**:
  - Automatic detection of worker failures
  - Task reassignment for failed workers
  - Handling of stragglers through task replication
  - Configurable retry limits for failed tasks
- **RPC Communication**: All inter-process communication uses Go's net/rpc package
- **Flexible Task Assignment**: Supports configurable number of reduce tasks
- **Clean Shutdown**: Graceful shutdown with proper cleanup of resources
- **Progress Monitoring**: Real-time tracking of task progress and worker status

## Prerequisites

- Go 1.19 or higher
- A Unix-like operating system (Linux/macOS)
- Available network ports for RPC communication

## Installation

```bash
git clone [repository-url]
cd map-reduce-in-go
go mod download
```

## Usage

### Starting the Coordinator

```bash
go run main.go -coordinator \
    -addr=localhost:5000 \
    -reduce=5 \
    -input=file1.txt,file2.txt \
    -intermediate-dir=/tmp/mr-tmp
```

Parameters:

- `-coordinator`: Run in coordinator mode
- `-addr`: Address for the coordinator to listen on
- `-reduce`: Number of reduce tasks
- `-input`: Comma-separated list of input files
- `-intermediate-dir`: Directory for intermediate files

### Starting Workers

```bash
go run main.go -addr=localhost:5000 -workers=4
```

Parameters:

- `-addr`: Address of the coordinator
- `-workers`: Number of worker processes to spawn (defaults to CPU cores)

## Implementation Details

### Task States

- `TaskIdle`: Task is available for assignment
- `TaskInProgress`: Task is currently being processed
- `TaskCompleted`: Task has been successfully completed

### Workflow

1. Coordinator starts and initializes task tracking
2. Workers register with coordinator via RPC
3. Coordinator assigns Map tasks to available workers
4. Workers process Map tasks and write intermediate files
5. After all Map tasks complete, Reduce phase begins
6. Workers process Reduce tasks and generate final output
7. System completes when all Reduce tasks finish

### Fault Handling

- Heartbeat mechanism monitors worker health
- Failed tasks are automatically reassigned
- Straggler detection triggers task replication
- Configurable timeout and retry parameters

## Development

### Project Structure

```bash
.
├── coordinator.go      # Coordinator implementation
├── worker.go          # Worker implementation
├── task.go            # Task management and tracking
├── rpc_types.go       # RPC interfaces and types
├── main.go            # Entry point and CLI
└── tests/             # Test suite
```

### Running Tests

```bash
go test ./...
```

### Adding New MapReduce Applications

To implement a new MapReduce application:

1. Implement the Mapper interface:

```go
type Mapper interface {
    Map(filename string, content string) ([]KeyValue, error)
}
```

2. Implement the Reducer interface:

```go
type Reducer interface {
    Reduce(key string, values []string) (string, error)
}
```

## Acknowledgments

- Inspired by Google's MapReduce paper
- Built with Go's excellent standard library
- Based on MIT's 6.824 Distributed Systems course

## Future Improvements

- Add support for custom partitioning functions
- Implement more sophisticated load balancing
- Add distributed filesystem support
- Improve monitoring and debugging tools
- Add support for hierarchical task scheduling
