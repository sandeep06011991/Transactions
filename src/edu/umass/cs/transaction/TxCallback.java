package edu.umass.cs.txn;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.GigaPaxosClient;
import edu.umass.cs.txn.txpackets.AbortRequest;
import edu.umass.cs.txn.txpackets.LockRequest;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxStateRequest;
import edu.umass.cs.txn.txpackets.UnlockRequest;

/**
 * @author arun
 * 
 *         Each active replica maintains a map of transaction state that are in
 *         progress at that replica.
 *         Deleted:
 *         Sending lock,unlock and abort requests. This is done by the Protocol Executor
 *         Contains a callback after Lock,Execute methods are executed.
 *         Again done by executor
 */
public abstract class TxCallback implements ExecutedCallback {

}
