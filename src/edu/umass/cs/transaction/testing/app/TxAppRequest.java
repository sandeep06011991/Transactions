package edu.umass.cs.transaction.testing.app;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Random;

public abstract class TxAppRequest extends JSONPacket implements ClientRequest,ReplicableRequest{

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public long requestID;

    enum keys {
        REQID,SERVICENAME,
    }

    public enum  TxRequestType implements IntegerPacketType{

        GET(1),

        PUT(2),

        OPERATE(3),

        RESULT(4);

        int id;

        static HashMap<Integer,TxRequestType> map;

        static{
            map=new HashMap<>();
            for(TxRequestType t:TxRequestType.values()){
                map.put(new Integer(t.getInt()),t);
            }

        }

        public static TxRequestType getPacketTypeFromInt(int key){
            if(map.containsKey(new Integer(key))){
                return map.get(new Integer(key));
            }
            return null;
        }

        TxRequestType(int id){
            this.id = id;
        }

        @Override
        public int getInt() {
            return this.id;
        }
    }

    public ClientRequest response;


    String serviceName;

    static Random random = new Random();

    public TxAppRequest(TxRequestType t,String serviceName) {
        super(t);
        this.serviceName = serviceName;
        this.requestID = random.nextLong();

    }

    public TxAppRequest(JSONObject json) throws JSONException {
        super(json);
        this.serviceName=json.getString(keys.SERVICENAME.toString());
        this.requestID= json.getLong(keys.REQID.toString());
    }

    @Override
    public ClientRequest getResponse() {
        return response;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return TxRequestType.getPacketTypeFromInt(type);
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public long getRequestID() {
        return requestID;
    }

    @Override
    public JSONObject toJSONObject() throws JSONException{
        JSONObject jsonObject = super.toJSONObject();
        jsonObject.put(keys.REQID.toString(),requestID);
        jsonObject.put(keys.SERVICENAME.toString(),serviceName);
        return jsonObject;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        return new JSONObject();
    }
}
