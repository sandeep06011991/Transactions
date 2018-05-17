package edu.umass.cs.transaction.protocol;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.transaction.DistTransactor;
import edu.umass.cs.transaction.Transaction;
import edu.umass.cs.transaction.exceptions.ResponseCode;
import edu.umass.cs.transaction.txpackets.*;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TxSecondaryProtocolTask<NodeIDType> extends TransactionProtocolTask<NodeIDType> implements
        SchedulableProtocolTask<NodeIDType, TXPacket.PacketType, String>  {

    TxState state;

    long period;

    Set<String> leaderActives;

    Set<String> prevLeaderActives;

    ResponseCode rpe;

    private static final Logger log = Logger
            .getLogger(DistTransactor.class.getName());

    public TxSecondaryProtocolTask(Transaction transaction, TxState state
            , ProtocolExecutor protocolExecutor,Set<String> leaderActives ){
        super(transaction,protocolExecutor);
        this.state=state;
        this.period =  (10+new Random().nextInt(30))*1000;
        this.leaderActives = leaderActives;
        log.log(Level.INFO,"Secondary: "+this.state+" "+transaction.getTXID());
        //Secondaries timeout after 2 min
//        System.out.println("Secondary inititated with timeout "+period);
    }

    public TxState getState() {
        return state;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    @Override
    public void onStateChange(TxStateRequest request) {
        TxState newState=request.getState();
        if(newState == TxState.COMPLETE){
            log.log(Level.INFO,"Secondary: "+newState+" "+transaction.getTXID());
            this.cancel();
        }
        if((state == TxState.INIT )){
            log.log(Level.INFO,"Secondary: "+newState+" "+transaction.getTXID());
            state = newState;
            if(newState == TxState.ABORTED){
                prevLeaderActives = request.getPreviousActives();
                rpe = request.getRpe();
            }
        }
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[]
    handleEvent(ProtocolEvent<TXPacket.PacketType, String> event, ProtocolTask<NodeIDType,TXPacket.PacketType, String>[] ptasks) {
//        This state does  not handle any events
//            FIXME: Pull request in gigapaxos, check the keytype before handling an event
            return null;
//        throw new RuntimeException("Should never be called");
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] start() {
         return  null;
    }

    @Override
    public Set<TXPacket.PacketType> getEventTypes() {
        Set<TXPacket.PacketType> txPackets=new HashSet<>();
//        This does no packet handling
        return txPackets;
    }

    @Override
    public String getKey() {
        return transaction.getTXID();
    }

    @Override
    public GenericMessagingTask<NodeIDType, ?>[] restart() {
        ProtocolTask pt =null;
        switch(state){
            case INIT:  pt = new TxLockProtocolTask(transaction,protocolExecutor,leaderActives);
                        break;
            case COMMITTED: pt = new TxCommitProtocolTask(transaction,protocolExecutor);
                            break;
            case ABORTED:   pt = new TxAbortProtocolTask(transaction,protocolExecutor,prevLeaderActives,rpe);
                            break;
        }
        this.cancel();
        assert  pt!=null;
        log.log(Level.INFO,"Transaction ID:"+transaction.getTXID()+"    secondary timeout"+state);
        protocolExecutor.spawn(pt);
        return null;
    }


    @Override
    public long getPeriod() {
//        FIXME: Write a test that Test this getPeriod
//        FIXME: Write a random wait Period generator
        return  period;
    }

    public void setRpe(ResponseCode rpe){
        this.rpe = rpe;
    }
}
