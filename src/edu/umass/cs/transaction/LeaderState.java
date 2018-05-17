package edu.umass.cs.transaction;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.transaction.exceptions.TxnState;
import edu.umass.cs.transaction.txpackets.TxState;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

/*
*   Dist Transactor stores a HashMap for each Leader
*   i.e <leader_name, leaderState>
*   LeaderState in turn contains map <txId,TxnState>
*
* */
public class LeaderState {

//    TxId,OngoingTxn
    HashMap<String,OngoingTxn> ongoingTxnHashMap = new HashMap<>();

    String leaderName;

    LeaderState(String leaderName){
        this.leaderName = leaderName;
    }

//  preExecuted method updates transaction state as it proceeds
    public void updateTransaction(String txId, TxState state){
            if(state == TxState.COMPLETE){
                ongoingTxnHashMap.remove(txId);
                return;
            }
        OngoingTxn ongoingTxn = ongoingTxnHashMap.get(txId);
        if(ongoingTxn.txState == TxState.INIT){
            ongoingTxn.txState = state;

        }
    }

//  inserted after the INIT transaction
    void insertNewTransaction(OngoingTxn ongoingTxn){
        if(ongoingTxnHashMap.containsKey(ongoingTxn.txId)){
            throw new RuntimeException("Why recreate an already crated transaction");

        }
        ongoingTxnHashMap.put(ongoingTxn.transaction.getTXID(),ongoingTxn);
    }

    JSONObject toJSONObject(String leaderName) throws JSONException{
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("leader",leaderName);
        ArrayList<JSONObject> arrayList = new ArrayList<>();
        for(OngoingTxn ongoingTxn: ongoingTxnHashMap.values()){
            arrayList.add(ongoingTxn.toJSONObject());
        }
        jsonObject.put("transactions",arrayList);
        return jsonObject;
    }


    public LeaderState(JSONObject jsonObject,AppRequestParser appRequestParser) throws JSONException{
        leaderName = jsonObject.getString("leader");
        JSONArray jsonArray = jsonObject.getJSONArray("transactions");
        for(int i=0;i<jsonArray.length();i++){
            OngoingTxn ongoingTxn = new OngoingTxn(jsonArray.getJSONObject(i),appRequestParser);
            ongoingTxnHashMap.put(ongoingTxn.txId,ongoingTxn);
        }

    }

    public boolean isEmpty(){
        return ongoingTxnHashMap.isEmpty();
    }
}

