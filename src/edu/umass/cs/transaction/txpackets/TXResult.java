package edu.umass.cs.txn.txpackets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.Key;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TXResult extends TXPacket{
//  opId is used to match packets after they have been recieved in the ProtocolTask
//  Used for identifying within the same key
//  Example diffrent operations
    String opId;
//    Used to demultiplex to protocolExecutor Handler
//    All packets for the same state have the same key
    String key;

    private enum  KEYS{
        OPPACKETTYPE, SUCCESS, KEY, OPID
    }

    boolean success=false;

    public IntegerPacketType opPacketType;

    Set<String> activesOfPreviousLeader = new HashSet<>();

    public TXResult(String txid,TXPacket.PacketType packetType,boolean success,String key,String opId,String leader) {
        super(PacketType.RESULT, txid,leader);
        opPacketType=packetType;
        this.success=success;
        this.key=key;
        this.opId=opId;
    }
    @Override
    public JSONObject toJSONObject() throws JSONException{
        JSONObject jsonObject=super.toJSONObject();
//        FIXME: Use ENUM to do all this
        jsonObject.put("opPacketType",opPacketType.getInt());
        jsonObject.put("success",success);
        jsonObject.put("key",key);
        jsonObject.put("opId",opId);
        if(activesOfPreviousLeader!=null){
            jsonObject.put("PreviousActives",activesOfPreviousLeader);
        }
        return jsonObject;
    }


    public TXResult(JSONObject jsonObject) throws JSONException{
        super(jsonObject);
        opPacketType= TXPacket.PacketType.intToType.get( jsonObject.getInt("opPacketType"));
        success=jsonObject.getBoolean("success");
        key=jsonObject.getString("key");
        opId=jsonObject.getString("opId");
        if(jsonObject.has("PreviousActives")){
            JSONArray j = jsonObject.getJSONArray("PreviousActives");
            for(int i=0;i<j.length();i++){
                activesOfPreviousLeader.add(j.getString(i));
            }

        }
    }


    public boolean isFailed() {
        return !success;
    }

    @Override
    public String getKey(){
        return key;
    }

    public String getOpId(){return opId;}

    public void setRequestId(long requestId){this.requestId=requestId;}

    public void setActivesOfPreviousLeader(Set<String> actives ){
        this.activesOfPreviousLeader = actives;
    }

    public Set<String> getPrevLeaderQuorumIfFailed(){return this.activesOfPreviousLeader;}
}
