package edu.umass.cs.transaction.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.transaction.DistTransactor;
import edu.umass.cs.transaction.Transaction;
import edu.umass.cs.transaction.exceptions.ResponseCode;
import edu.umass.cs.transaction.txpackets.*;
import org.omg.SendingContext.RunTime;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TxLockProtocolTask<NodeIDType> extends
        TransactionProtocolTask<NodeIDType>{

//    The lock protocol has 3 options to transition into
//    1) AbortProtocol: If the locks could not be acquired
//    2) Execute Protocol: If the locks could be acquired


    TreeSet<String> awaitingLock;

    Set<String> leaderActives;

    /*If some lock failed */
    boolean lockFailure = false;

    Set<String> previousActives;

    private static final Logger log = Logger
            .getLogger(DistTransactor.class.getName());
    public TxLockProtocolTask(Transaction transaction,ProtocolExecutor protocolExecutor,Set<String> leaderActives)
    {
        super(transaction,protocolExecutor);
        this.leaderActives = leaderActives;
        awaitingLock = transaction.getLockList();
        log.log(Level.INFO,"Primary: Lock "+transaction.getTXID());
    }

    @Override
    public void  onStateChange(TxStateRequest request) {
        this.cancel();
        if(request.getState() == TxState.ABORTED){
//            This would have been started by the same primary
//            if the primary had changed, there would be a take over message in between
            protocolExecutor.spawn(new TxAbortProtocolTask(transaction,protocolExecutor,request.getPreviousActives(),request.getRpe()));
            return;
        }
        if(request.getState() == TxState.COMMITTED){
//            This happens when the server is recovering
            protocolExecutor.spawn(new TxCommitProtocolTask(transaction,protocolExecutor));
            return;
        }

        if(request.getState() == TxState.COMPLETE){
            System.out.println("Wierd occurance Debug later");
//            This is only happenning on recovery and that too at high load only
        }
//        throw new RuntimeException("Safety Violation"+request.toString());
    }


    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult)
                &&(((TXResult) event).opPacketType==TXPacket.PacketType.LOCK_REQUEST)) {
            TXResult txResult = (TXResult) event;
            String opId = txResult.getOpId();
//          A lock already acquired or unrelated lock
            if (!awaitingLock.remove(opId)) return null;
            //FIXME: Is there a better way to map lock opId to Lock requests
            if(txResult.isFailed()){
                lockFailure = true;
                previousActives = txResult.getPrevLeaderQuorumIfFailed();

            }
//            if(txResult.isFailed()){
//                TxStateRequest stateRequest = new TxStateRequest(this.transaction.getTXID(), TxState.ABORTED,
//                        transaction.getLeader() ,ResponseCode.LOCK_FAILURE,txResult.getPrevLeaderQuorumIfFailed());
//                assert stateRequest.getPreviousActives() != null;
//                return getMessageTask(stateRequest);
//            }

        if(awaitingLock.isEmpty()){
                if(!lockFailure) {
                    this.cancel();
                    ptasks[0] = new TxExecuteProtocolTask(this.transaction, getProtocolExecutor());
                }else{
                    TxStateRequest stateRequest = new TxStateRequest(this.transaction.getTXID(), TxState.ABORTED,
                            transaction.getLeader() ,ResponseCode.LOCK_FAILURE,previousActives);
                    assert stateRequest.getPreviousActives() != null;
                    return getMessageTask(stateRequest);
                }
            }
        }
        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType,?>[] start() {
        ArrayList<Request> requests=new ArrayList<>();
        for (String t : awaitingLock) {
//          Low Priority: cleaner method exists
            LockRequest lockRequest = new LockRequest(t, transaction.getTXID(),transaction.getLeader(),leaderActives);
            requests.add(lockRequest);

            }
        return getMessageTask(requests);

    }


    @Override
    public Set<TXPacket.PacketType> getEventTypes()
    {   Set<TXPacket.PacketType> txPackets=new HashSet<>();
        txPackets.add(TXPacket.PacketType.RESULT);
        return txPackets;
    }

    @Override
        public String getKey() {
            return transaction.getTXID();
        }

}
