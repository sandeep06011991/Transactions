package edu.umass.cs.txn;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.txn.txpackets.TxState;
import org.json.JSONException;
import org.json.JSONObject;

public class OngoingTxn {

        String txId;
        Transaction transaction;
        TxState txState;

        public OngoingTxn( Transaction transaction,TxState txState){
            this.transaction = transaction;
            this.txState = txState;
            this.txId =transaction.getTXID();
        }

        JSONObject toJSONObject() throws JSONException {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("txId",txId);
            jsonObject.put("transaction",transaction.toJSONObject());
            jsonObject.put("txState",TxState. getIntFromTxState((txState)));
            return jsonObject;
        }

        OngoingTxn(JSONObject jsonObject, AppRequestParser appRequestParser) throws JSONException{
            txId =  jsonObject.getString("txId");
            transaction = new Transaction(jsonObject.getJSONObject("transaction"),appRequestParser);
            txState = TxState.getTxStateFromInt(jsonObject.getInt("txState"));
        }

}
