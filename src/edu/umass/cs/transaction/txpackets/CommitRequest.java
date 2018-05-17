package edu.umass.cs.transaction.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.transaction.Transaction;

/**
 * @author arun
 *
 *         This request commits a transaction. At a transaction group member,
 *         the corresponding callback will trigger unlocks to participant
 *         groups.
 */
public class CommitRequest extends TXPacket {


	public CommitRequest(String txId,String leader) {
		super(PacketType.COMMIT_REQUEST, txId, leader);
	}

	public CommitRequest(JSONObject json) throws JSONException {
		super(json);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object getKey() {
		return "COMMIT"+txid;
	}
}
