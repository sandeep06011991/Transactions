package edu.umass.cs.txn.exceptions;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


/*
Class used to store the state of the participant at the time of locking
state: which is used to rollback
leaderQuorum : which is used when a contending transaction is issued
* */
//FIXME: This file is in a wrong location, move it
public class TxnState {

//  Transaction ID also used as lockID
    public String txId;
//  Checkpointed state before the start of the transaction
//  Also used for rollbacks
    public String state;

    ArrayList<String> requests = new ArrayList<>();
    HashSet<Long> requestId = new HashSet<>();
    Set<String> leaderQuorum = new HashSet<>();

    public  TxnState(String txId,String state,Set<String> leaderQuorum){
        this.txId = txId    ;
        this.state = state;
        this.leaderQuorum = leaderQuorum;
    }

    public  void add_request(ClientRequest request){
        requests.add(request.toString());
        requestId.add(new Long(request.getRequestID()));
    }

    public TxnState(JSONObject jsonObject) throws JSONException{
        this.txId = jsonObject.getString("txId");
        this.state =jsonObject.getString("state");
        if(jsonObject.has("requests")){
            JSONArray jsonArray= jsonObject.getJSONArray("requests");
            JSONArray jsonArray1= jsonObject.getJSONArray("requestId");

            for(int i=0;i<jsonArray.length();i++ ){
                requests.add(jsonArray.getString(i));
                requestId.add(jsonArray1.getLong(i));
            }
        }

        JSONArray tt = jsonObject.getJSONArray("leaderQuorum");
        for(int t=0;t<tt.length();t++){
            leaderQuorum.add(tt.getString(t));
        }

    }


    public JSONObject toJSONObject() {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("txId", txId);
            jsonObject.put("state",state);
            if(requests.size()!=0) {
                jsonObject.put("requests", requests);
                jsonObject.put("requestId",requestId);
            }
            jsonObject.put("leaderQuorum",leaderQuorum);

            return jsonObject;
        }catch(Exception e){
            throw new RuntimeException("Unsuccessfull in creating json object");
        }
    }

    @Override
    public String toString() {
        return toJSONObject().toString();
    }

    public HashSet<Long> getRequestId() {
        return requestId;
    }

    public ArrayList<String> getRequests() {
        return requests;
    }

    public Set<String> getLeaderQuorum() {
        return leaderQuorum;
    }
}
