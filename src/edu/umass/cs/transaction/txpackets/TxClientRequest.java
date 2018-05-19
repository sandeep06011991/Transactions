package edu.umass.cs.transaction.txpackets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.security.Key;
import java.util.ArrayList;
import java.util.Random;

public class TxClientRequest extends JSONPacket implements ClientRequest{

    private ArrayList<ClientRequest> requests;
    private final long requestId;

    public InetSocketAddress clientAddr;

    public InetSocketAddress recvrAddr;

    static Random random=new Random();

    String entry;

    private enum Keys {
        REQUESTS,REQUESTID,ENTRY
    }

    public TxClientRequest(ArrayList<ClientRequest> requests){
        // FIXME: Ideally should not refer to not parent classes packet, gives clean implementation
        super(TXPacket.PacketType.TX_CLIENT);
        this.requests=requests;
        this.requestId=random.nextLong();
        /* In fixed groups , since any cast is used entry does not matter
        * if participant is chosen as leader, this is overriden with one of the participants name*/
        this.entry = "Some irrelevant name";

    }

    @Override
    public ClientRequest getResponse() {
        throw new RuntimeException("Client Response not yet built");
    }

    @Override
    public IntegerPacketType getRequestType() {
        return TXPacket.PacketType.TX_CLIENT;
    }

    public void setServiceName(String serviceName){
        this.entry = serviceName;
    }

    @Override
    public String getServiceName() {
        return this.entry;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
//        FIXME: Check if converting explicitly to a JSON object is required
        JSONObject jsonObject=new JSONObject();
        ArrayList<JSONObject> j=new ArrayList<>();
        for(Request request:requests){
            if (!(request instanceof JSONPacket)) {
                throw new RuntimeException("This was only built for requests which extend JSONPacket");
            }
            j.add(((JSONPacket)request).toJSONObject());
        }
        jsonObject.put(Keys.REQUESTS.toString(),j);
        jsonObject.put(Keys.REQUESTID.toString(),requestId);
        if(getServiceName()!=null){
            jsonObject.put(Keys.ENTRY.toString(),getServiceName());
        }
        return jsonObject;
    }

    public TxClientRequest(JSONObject jsonObject, AppRequestParser appRequestParser) throws JSONException{
        super(jsonObject);
        JSONArray jsonArray=jsonObject.getJSONArray(Keys.REQUESTS.toString());
        requests=new ArrayList<>();
        for(int i=0;i<jsonArray.length();i++){
            try{
                requests.add((ClientRequest) appRequestParser.getRequest(jsonArray.get(i).toString()));
                }catch(RequestParseException rpe){
               throw new JSONException("Request parse Exception");
            }
        }
        requestId=jsonObject.getLong(Keys.REQUESTID.toString());

        if(jsonObject.has(Keys.ENTRY.toString())){
            setServiceName(jsonObject.getString(Keys.ENTRY.toString()));
        }
    }

    @Override
    public long getRequestID() {
        return requestId;
    }

    public ArrayList<ClientRequest> getRequests() {
        return requests;
    }

}
