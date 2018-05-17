package edu.umass.cs.txn.txpackets;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.txn.Transaction;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;

/*
*   Sent by Entry server after receiving the
* */
public class TXInitRequest extends TXPacket{
    public Transaction transaction;


    public TXInitRequest(Transaction transaction) {
        super(PacketType.TX_INIT, transaction.getTXID(),transaction.getLeader());
        this.transaction=transaction;
    }

    public TXInitRequest(JSONObject jsonObject, AppRequestParser appRequestParser) throws JSONException{
        super(jsonObject);
        this.transaction= new  Transaction(jsonObject.getJSONObject("transaction"),appRequestParser);
    }

    @Override
    public boolean needsCoordination(){
        return true;
    }



    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject=super.toJSONObject();
        jsonObject.put("transaction",transaction.toJSONObject());
        return jsonObject;
    }

    @Override
    public Object getKey() {
        return txid;
    }
}
