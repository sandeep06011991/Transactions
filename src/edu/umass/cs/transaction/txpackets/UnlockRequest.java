package edu.umass.cs.transaction.txpackets;

import edu.umass.cs.transaction.protocol.TxMessenger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 */

// FIXME: Unlock is quite redundant with Commit Request
public class UnlockRequest extends TXPacket {

	private static enum Keys {
		SERVICENAME , TXID, COMMIT
	};

	private final String serviceName;
	private final String txID;
	private final boolean  commit;

	public UnlockRequest(String serviceName, String txID,boolean commit,String leader) {
		super(TXPacket.PacketType.UNLOCK_REQUEST, txID, leader);
		this.serviceName = serviceName;
		this.txID = txID;
		this.commit = commit;
	}

	public UnlockRequest(JSONObject json) throws JSONException {
		super(json);
		this.serviceName = json.getString(Keys.SERVICENAME.toString());
		this.txID = json.getString(Keys.TXID.toString());
		this.commit = json.getBoolean(Keys.COMMIT.toString());
	}

	public String getTXID() {
		return this.txID;
	}

	public JSONObject toJSONObject() throws JSONException{
		JSONObject jsonObject=super.toJSONObject();
		jsonObject.put(Keys.SERVICENAME.toString(),this.serviceName);
		jsonObject.put(Keys.COMMIT.toString(),this.commit);
		return jsonObject;

	}


	public String getLockID() {
		return this.txID;
	}

	public String getServiceName() {
		return this.serviceName;
	}

	@Override
	public boolean needsCoordination() {
		return true;
	}

	@Override
	public Object getKey() {
		return txid;
	}

	public boolean isCommited() {
		return commit;
	}
}
