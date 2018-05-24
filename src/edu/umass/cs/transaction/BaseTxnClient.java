package edu.umass.cs.transaction;
/*
*  @ author Sandeep
* */

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.transaction.txpackets.TXPacket;
import edu.umass.cs.transaction.txpackets.TxClientRequest;
import edu.umass.cs.transaction.txpackets.TxClientResult;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/*This is the basic client wrapper around reconfigurable app client async
* used to support transactions. */
public abstract class BaseTxnClient extends ReconfigurableAppClientAsync<Request> {

    static Random random = new Random();

    public BaseTxnClient() throws IOException {
        super();
    }

    public RequestFuture<Request> sendTransaction(TxClientRequest txClientRequest,
                                                  RequestCallback requestCallback) throws IOException{
        if(TxConstants.fixedGroups){
            return this.sendRequestAnycast(txClientRequest,requestCallback);
        }else{
            int randParticipant =random.nextInt(txClientRequest.getRequests().size());
            String participantLeader = txClientRequest.getRequests().get(randParticipant).getServiceName();
            txClientRequest.setServiceName(participantLeader);
            return  this.sendRequest(txClientRequest,requestCallback);
        }
    }

    @Override
    public Request getRequest(String stringified) {
        try {
/*      DEBUG tip: If requests are not being recieved debug here */
            JSONObject jsonObject=new JSONObject(stringified);
            if(jsonObject.getInt("type")==262){
                System.out.println(stringified);
                return new TxClientResult(jsonObject);
            }
        } catch ( JSONException e) {
            // Assume that all requests are
            e.printStackTrace();
        }


        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> set = new HashSet<>();
        set.add(TXPacket.PacketType.TX_CLIENT_RESPONSE);
        return set;
    }


}
