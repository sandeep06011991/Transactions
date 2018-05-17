package edu.umass.cs.transaction.txpackets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.Key;
import java.util.HashSet;
import java.util.Set;


/**
 * @author arun
 *
 */
public class LockRequest extends TXPacket {

	private static enum Keys {
		SERVICENAME, TXID, LEADERACTIVES
	};

	private final String serviceName;

	private  final Set<String> leaderActives = new HashSet<>();

	public LockRequest(String serviceName ,String txId,String leader,Set<String> leaderActives){
		super(PacketType.LOCK_REQUEST,txId,leader);
		this.serviceName = serviceName;
		if(leaderActives!=null) {
			this.leaderActives.addAll(leaderActives);
			}
		}
	/**
	 * @param json
	 * @throws JSONException
	 */
	public LockRequest(JSONObject json) throws JSONException {
		super(json);
		this.serviceName = json.getString(Keys.SERVICENAME.toString());
		JSONArray t =  json.getJSONArray(Keys.LEADERACTIVES.toString());
		for(int i=0;i<t.length();i++){
			this.leaderActives.add(t.getString(i));
		}

	}

	public JSONObject toJSONObject() throws JSONException{
		JSONObject jsonObject=super.toJSONObject();
		jsonObject.put(Keys.SERVICENAME.toString(),serviceName);
		jsonObject.put(Keys.LEADERACTIVES.toString(),leaderActives);
		return jsonObject;
	}
	/**
	 * @return Transaction id is the identification of
	 *
	 */
	public String getLockID() {
		return this.txid;
	}

	public String getServiceName() {
		return this.serviceName;
	}

	@Override
	public Object getKey() {
		return txid;
	}

	public Set<String> getLeaderActives(){
		return this.leaderActives;
	}

}
