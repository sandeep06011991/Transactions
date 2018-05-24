# Transactions on Gigapaxos

## Introduction

This framework is used to implement transactions over replicated services provided by gigapaxos. 
We use a variant of 2PC where the coordinator is replicated using the service provided by gigapaxos.
The coordinator could be a fixed group or it could be randomly chosen from one of the participant list. 

2PC is implemented on top of Paxos (like nested paxos in scatter). By implementing as a wrapper on top
of paxos, we can forget about replication and just coordinate between services.

## Implementation

A jar of gigapaxos with the patch required to run has to copied into the lib directory.
Please note to ENABLE_TRANSACTIONS(true) and that the Transaction wrapper class is 
"edu.umass.cs.transaction.DistTransactor"

Right now this included in the git repo. But once the patch is merged with master, this would
be deleted and a gigapaxos jar . 

The DistTransactor is CoordinatorCallback which is run  after a request gets coordinated
on all services using the preExecute method. Thus the DistTransactor processes a relevant message
before the app has a chance filtering out messages such as LOCK,EXECUTE and UNLOCK.

## How to run

A make file is used for convenience.

1. Choose required configuration in TxConstants.java.
Participant Leader or Coordinator Leader.

2. gigapaxosConfig=src/testing/gigapaxos.properties  is used as a config file

3. make server : cleans all old databases and starts servers

4. make client : Runs the basic test which issues 2 transactions and checks if they are committed.

Please Node: to only use BasicTest as a reference when building client applications

## Running tests

1. After putting the updated gigapaxos jar in the directory as describe above.

2. Run ant test. This runs all the reconfiguration tests. Writing some tests for transactions is a pending task.


## Running Latency Tests

1. make server: Deploys servers according to the configuration testing/gigapaxos.properties

2. make latency

3. The results would be in the client gigapaxos.log.Appending to the end.
After this is run completely, you will see "All done" printed onto the console.

4. Tip: Run on local before deploying on cluster. 


## How to write a sample client.

1. To write a sample client extend BaseTxnClient. Follow CalculatorTXClient as an Example.

2. Augment getRequest and getRequestTypes with packet types of underlying app.


## Notes and TODO

1. The Simulator code has not yet been refactored after seperation of code changes. 

 


