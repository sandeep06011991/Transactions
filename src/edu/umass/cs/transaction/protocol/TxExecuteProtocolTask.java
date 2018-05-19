package edu.umass.cs.transaction.protocol;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.transaction.DistTransactor;
import edu.umass.cs.transaction.Transaction;
import edu.umass.cs.transaction.exceptions.ResponseCode;
import edu.umass.cs.transaction.interfaces.TxOp;
import edu.umass.cs.transaction.txpackets.*;

import javax.sound.sampled.LineEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TxExecuteProtocolTask<NodeIDType>
        extends TransactionProtocolTask<NodeIDType> {

    //    ArrayList<String> sent=new ArrayList<>();
//    Request Ids that are awaiting execution
    ArrayList<Long> toExecuteRequests = new ArrayList<>();
    HashMap<Long,ClientRequest> map = new HashMap<>();

    private static final Logger log = Logger
            .getLogger(DistTransactor.class.getName());

    public TxExecuteProtocolTask(Transaction transaction,ProtocolExecutor protocolExecutor){

        super(transaction,protocolExecutor);
        for(Request r: transaction.getRequests()){
//          FixME: This casting should not be required, change definition in transaction
            toExecuteRequests.add(((ClientRequest)r).getRequestID());
            map.put(((ClientRequest)r).getRequestID(),(ClientRequest) r);
        }
        log.log(Level.INFO,"Primary: Execute "+transaction.getTXID());
    }

    @Override
    public void onStateChange(TxStateRequest request) {
//        System.out.println(request.toString());
        if(request.getState()== TxState.COMMITTED){
            this.cancel();
            protocolExecutor.spawn(new TxCommitProtocolTask<>(transaction,protocolExecutor));
        }else{
            if(request.getState()!=TxState.ABORTED){
                log.log(Level.INFO,"VIOLATION!!!!!");
                log.log(Level.INFO,"TxID:"+transaction.getTXID());
                log.log(Level.INFO,"RequestID:"+transaction.getRequestId());

            }
//          Will not go directly to COMPLETE
//            assert request.getState() == TxState.ABORTED;
//          We are not concerned with this failure
            this.cancel();
            protocolExecutor.spawn(new TxAbortProtocolTask<>(transaction,protocolExecutor,request.getPreviousActives(),request.getRpe()));
        }
    }


    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
        if((event instanceof TXResult)&&(((TXResult) event).opPacketType==TXPacket.PacketType.TX_OP_REQUEST)){
            TXResult txResult=(TXResult)event;
        if(!toExecuteRequests.isEmpty() && toExecuteRequests.get(0) == Long.parseLong(txResult.getOpId())){
//               because an older request could be recieved
                toExecuteRequests.remove(0);
        }
        if(toExecuteRequests.isEmpty()){
            TxStateRequest stateRequest = new TxStateRequest(this.transaction.getTXID(),
                    TxState.COMMITTED,this.transaction.getLeader(), ResponseCode.COMMITTED, null);
            return getMessageTask(stateRequest);
        }else{
            return sendPendingMessage();
        }

        }
        return null;
    }

    private GenericMessagingTask<NodeIDType, ?>[] sendPendingMessage(){
        assert toExecuteRequests.size() > 0;
//      Add a feature to parallelize thisw
        Long rqId = toExecuteRequests.get(0);
        ClientRequest toSend = map.get(rqId);
        return  getMessageTask(new TxOpRequest(transaction.getTXID(),toSend,this.transaction.getLeader()));
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
        return sendPendingMessage();
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        Set<TXPacket.PacketType> set=new HashSet<>();
        set.add(TXPacket.PacketType.RESULT);
        return set;
    }

    @Override
    public String getKey() {
        return transaction.getTXID();
    }
}
