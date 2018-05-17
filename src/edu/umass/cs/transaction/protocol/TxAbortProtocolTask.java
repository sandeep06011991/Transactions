package edu.umass.cs.txn.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.txn.DistTransactor;
import edu.umass.cs.txn.Transaction;
import edu.umass.cs.txn.exceptions.ResponseCode;
import edu.umass.cs.txn.txpackets.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TxAbortProtocolTask<NodeIDType>
            extends TransactionProtocolTask<NodeIDType> {
//    FIXME: There should be a retry mechanism
    TreeSet<String> unlockList;

    ResponseCode rpe;

    Set<String> previousLeaderActives;

    boolean respondToClient = true;

    private static final Logger log = Logger
            .getLogger(DistTransactor.class.getName());


    public TxAbortProtocolTask(Transaction transaction, ProtocolExecutor protocolExecutor, Set<String> previousQuorum, ResponseCode rpe) {
        super(transaction, protocolExecutor);
        unlockList = transaction.getLockList();
        assert unlockList.size() >0;
        if(((rpe == ResponseCode.LOCK_FAILURE)&&(previousQuorum == null))){
/*  When recovering from checkpoint, we are not presisting which quorum failed */
            rpe = ResponseCode.RECOVERING;
        };
        this.rpe = rpe;
        this.previousLeaderActives = previousQuorum;
        log.log(Level.INFO,"Primary: Abort "+transaction.getTXID());
    }

    @Override
    public void onStateChange(TxStateRequest request) {
        if(TxState.COMPLETE == request.getState()){
            this.cancel();
        }
    }


    @Override
    public GenericMessagingTask<NodeIDType, ?>[] handleEvent(ProtocolEvent<TXPacket.PacketType, String> event,
                                                             ProtocolTask<NodeIDType, TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult) && (((TXResult) event).opPacketType == TXPacket.PacketType.UNLOCK_REQUEST)){
            TXResult txResult=(TXResult)event;
            String opId= txResult.getOpId();
            if(unlockList.remove(opId)){
//                System.out.println("Unlocked "+opId);
            }
            if(unlockList.isEmpty()){
//                FixME: Redundant make this a function
                ArrayList<Request> re= new ArrayList<>();
                TxStateRequest request=new TxStateRequest(transaction.getTXID(),TxState.COMPLETE,transaction.getLeader(),
                        ResponseCode.COMPLETE,null);
                re.add(request);
                return getMessageTask(re);
            }

        }

        return null;
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        ArrayList<Request> requests = new ArrayList<>();
        for(String t: unlockList ){
            UnlockRequest unlockRequest = new UnlockRequest(t,transaction.getTXID(),false,transaction.getLeader());
            requests.add(unlockRequest);
        }
        if(respondToClient){
        TxClientResult result=new TxClientResult(transaction,rpe,previousLeaderActives);
        requests.add(result);
        respondToClient = false;
        }
        if(unlockList.isEmpty()){
            TxStateRequest request=new TxStateRequest(transaction.getTXID(),TxState.COMPLETE,transaction.getLeader(),
                    ResponseCode.COMPLETE,null);
            requests.add(request);
        }
//      TEST: retry mechanism, return null
        return getMessageTask(requests);
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        Set<TXPacket.PacketType> txPackets = new HashSet<>();
        txPackets.add(TXPacket.PacketType.RESULT);
        return txPackets;
    }

    @Override
    public String getKey() {
        return transaction.getTXID();
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] restart() {
//            FIXME: To build an exponential back off
        return start();
    }

    @Override
    public long getPeriod() {
//        FIXME: Write a test that Test this getPeriod
//        FIXME: Write a random wait Period generator
        return  20000;
    }
}
