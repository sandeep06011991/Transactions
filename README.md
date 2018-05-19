## Transactions on Gigapaxos

This framework is used to implement transactions over replicated services provided by gigapaxos. 
We use a variant of 2PC where the coordinator is replicated using the service provided by gigapaxos.
The coordinator could be a fixed group or it could be randomly chosen from one of the participant list. 

A jar of gigapaxos with the patch required to run has to copied into the lib directory.
Right now this included in the git repo. But once the patch is merged with master, this would
be deleted and a gigapaxos jar . 

## How to run

1. Choose required configuration in TxConstants.java
   gigapaxosConfig=src/testing/gigapaxos.properties  is used as a config file

2. make server : cleans all old databases and starts servers

3. make client : Runs the basic test which issues 2 transactions and checks if they are committed.

Please Node: to only use BasicTest as a reference when building client applications
The files Latency tester and Simulator use old references which are deprecated and need to be changed.

