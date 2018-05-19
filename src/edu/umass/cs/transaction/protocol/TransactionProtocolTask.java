package edu.umass.cs.transaction.protocol;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.transaction.Transaction;
import edu.umass.cs.transaction.exceptions.ResponseCode;
import edu.umass.cs.transaction.txpackets.TXPacket;
import edu.umass.cs.transaction.txpackets.TxState;
import edu.umass.cs.transaction.txpackets.TxStateRequest;

import java.util.ArrayList;
import java.util.Set;

abstract public class TransactionProtocolTask<NodeIDType> implements
        SchedulableProtocolTask<NodeIDType, TXPacket.PacketType, String> {

    Transaction transaction;

    ProtocolExecutor<NodeIDType,TXPacket.PacketType,String> protocolExecutor;

    int retry = 0;

    final int MAX_RETRY=5;
//   FixMe: This warning can be easily fixed my adding generic types into method signature
//
    @SuppressWarnings("unchecked")
    TransactionProtocolTask(Transaction transaction,ProtocolExecutor protocolExecutor){
        this.transaction=transaction;
        this.protocolExecutor=protocolExecutor;
    }

    public Transaction getTransaction() {
        return transaction;

    }

    public ProtocolExecutor getProtocolExecutor() {
        return protocolExecutor;
    }
//  FixME: Is this dummy required. If so write a justification
    static Object[] dummy={null,null};

    @SuppressWarnings("unchecked")
    public GenericMessagingTask<NodeIDType, ?>[] getMessageTask(Request request){
        Request[] ls=new Request[1];
        ls[0]=request;
        GenericMessagingTask<NodeIDType,Object> temp = new GenericMessagingTask<>(dummy,ls);
        GenericMessagingTask<NodeIDType, Object>[] mtasks = new GenericMessagingTask[1];
        mtasks[0]=temp;
        return mtasks;
    }
    @SuppressWarnings("unchecked")
    public GenericMessagingTask<NodeIDType,?>[] getMessageTask(ArrayList<Request> requests){
//    FIXME:Dont know why I have to do this wierd
        GenericMessagingTask<NodeIDType,?>[] ret=new GenericMessagingTask[requests.size()];
        int i=0;
        ArrayList<Integer> integers=new ArrayList<>(1);
        for(Object request:requests){
            ArrayList<Object> req=new ArrayList<>();
            req.add(request);
            ret[i]=new GenericMessagingTask<>(integers.toArray(),req.toArray());
            i++;
        }
        return ret;
    }

    // Returns the protocol task that must be spawned in place of the current protocol task
//    when state change request is recieved
    public abstract void onStateChange(TxStateRequest request);

    public void cancel(){
/**
 *         FIXME: Is there a better way to cancel a task
 *         Defend this design idea. Seems like an extremely over kill idea
 */
        protocolExecutor.remove(this.getKey());
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] restart(){
        retry++;
        if(retry <=MAX_RETRY){
            return start();
        }
        TxStateRequest stateRequest = new TxStateRequest(this.transaction.getTXID(), TxState.ABORTED,transaction.getLeader(), ResponseCode.TIMEOUT,null);
//        System.out.println("Protocol task has timed out");
        return getMessageTask(stateRequest);
    }

    @Override
    public long getPeriod() {
//        FIXME: Write a test that Test this getPeriod
//        FIXME: Write a random wait Period generator
        return  100;
    }

}

