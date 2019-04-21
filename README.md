# Distributed Coordination Service

An API to allow distributed applications to run over it.
***

## User Functionalities Provided:
This API provides the following functionalities to the  user and to the application running over itself:
1. Add/Modify/Read/Delete File(params : dirPath, filename, ...)
2. File Locking for series of updates
3. Delete server
4. Add server (any number of servers supported)
5. Auto-handling of upto n-1 **crash faults**

## Model Assumptions
The following assumptions are made to simplify the model:-
1. Fully Connected Topology
2. Asynchronous and reliable system
3. FIFO channel
4. No link failure or Byzantine Fault
5. Client does not contact a failed server for read/write or make multiple attempts at various
servers
6. Sponsor node does NOT fail while data is being copied-over to a new server
7. Not more than n-1 crash faults (n - number of nodes in the network)

## System Guarantees
1. Consistency Model: Sequential Consistency
2. Fault model: Reliability, Availability: 100%, subject to fault model constraints
