# YADFS: Yet Another Distributed File System
A simple, educational distributed file system written in Go, inspired by HDFS.
Supports a single NameNode (metadata manager) and multiple DataNodes (storage nodes).

# Architecture
## NameNode:

Manages the file system namespace, directory tree, file-to-block mapping, and DataNode health.
Stores metadata in JSON files (file_metadata.json, folder_directory.json, file_mappings.json).
Exposes RPC endpoints for metadata and file operations.
## DataNodes:

Store and serve file blocks as individual files.
Respond to RPC and TCP requests for block upload/download.
Periodically ping the NameNode for health monitoring.
Support block replication for fault tolerance.
## Client:

Command-line tool to interact with the DFS(To be Integrated)
Handles file splitting, uploading, downloading, and directory operations via RPC and TCP.
## Data Organization
Files are split into fixed-size blocks (default: 1024 bytes).
Each block is stored as a separate file on a DataNode.
Metadata is maintained by the NameNode, mapping files to blocks and blocks to DataNodes.
Supports a virtual directory tree with persistent storage in JSON.
## Fault Tolerance
Each block is replicated across multiple DataNodes (default replication factor: 3).
NameNode monitors DataNode health and manages re-replication if a DataNode fails.

# Communication Flow
Client ↔️ NameNode
Protocol: Go RPC (Remote Procedure Call) over TCP.
Purpose:
Directory operations (mkdir, cd, ls).
File creation and metadata queries.
Discovering which DataNodes hold which blocks.
How:
The client connects to the NameNode’s RPC port.
Sends commands like MakeDirCommand, ChangeDirCommand, CreateFileCommand, etc.
Receives responses with metadata, block assignments, and directory listings.
## Client ↔️ DataNode
## Protocol
Raw TCP sockets.
## Purpose
Uploading/Downloading File Chunks
## How
The client connects to the DataNode’s TCP port (address discovered via NameNode).
For upload:
Sends a header (operation, block ID, checksum, replication count, etc.) followed by the block data.
Receives an acknowledgment or error.
For download:
Sends a header (operation, block ID, offset, length).
Receives the checksum and the requested block data.
## NameNode ↔️ DataNode
## Protocol
DataNodes periodically ping the NameNode via RPC to report their status.
NameNode does not directly send data to DataNodes; it only manages metadata and assignments.
## Purpose
Health monitoring, Block assignment and replication management.

# How to Run
1. Clone the Repository
```
git clone https://github.com/anuragvadhyar/Distributed_File_System.git
cd DFS_Go
```
2. Start the NameNode
```
cd namenode
go run namenode.go
```
3. Start The DataNode
```
cd datanode
go run datanode.go
```
4. Start The Client
```
cd client
go run client.go
```
#Usage
## Directory Operations
mkdir, cd, ls via RPC to the NameNode.
## File Upload
Client splits file, uploads blocks to assigned DataNodes, and updates NameNode metadata.
## File Download
Client queries NameNode for block locations, fetches blocks from DataNodes, and reassembles the file.
## Replication
Each block is stored on multiple DataNodes for fault tolerance.

# To-Do
Integrating Database in future iterations to replace json persistent storage.
