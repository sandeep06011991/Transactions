package edu.umass.cs.transaction.txpackets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 * 
 *
 *
 */
public class AbortRequest extends TXPacket {


	public AbortRequest( String txid,String leader) {
		super(PacketType.ABORT_REQUEST, txid,leader);
	}

	@Override
	public Object getKey() {
		return "Abort"+txid;
	}
}
