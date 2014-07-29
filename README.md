p2p_python
==========
A simplistic P2P file-sharing system.

Central Server
=============
It is the job of the central server to keep track of all active peers and files.

File management
=============
A command line interface to upload files.
Once a file has been uploaded, the server acts as a peer for that file
till the time at least one other client has a complete copy of it.
At upload time, each file is converted into shards, each with size SHARD SIZE.
For each file it creates a P2P file object with

Peer management
=============
The first time a peer runs, it sends a register request to the server.
A client which has not sent a heartbeat for HEARTBEAT TIMEOUT is marked as killed 

Peers
=============
The server’s public key is hardcoded within each peer’s code.

The first time a client starts up, it is registered with the server and locally store the artifacts returned by the server.

After it has initialised, it sends a heartbeat signal to the server every HEARTBEAT INTERVAL, asking for the current set of files. If any of the files is new, it retrieves the corresponding P2P file object from the server.

It maintains a list of its current peers. Each time it encounters a new peer, it will exercise an authentication handshake.

Every PEER CONTACT INTERVAL it contacts each of its peers and give it information regarding each P2P file object that it possesses. 

If a peer has not touched base in PEER CONTACT TIMEOUT, it will be re- moved from the peers list.

Whenever a request for a specific shard is received it is served as long as the number of current connections is less than MAX PEERS and the peer has been authenticated. Each shard is also checksummed.

The goal of each client is to download every file within the network.

