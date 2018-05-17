package edu.umass.cs.txn.txpackets;

import edu.umass.cs.protocoltask.ProtocolEvent;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.txn.exceptions.ResponseCode;
import edu.umass.cs.utils.IntegerPacketTypeMap;

import java.security.Key;
import java.util.Random;

/**
 * @author arun
 *
 *         All transaction processing related packets inherit from this class.
 *         TxPackets are used in the protocol Executor for event handling,
 *         co ordinated at the actives and have a response that is sent back
 *         Must implement all 3 given classes
 *         Deleted respons
 */
public abstract class TXPacket extends JSONPacket implements ReplicableRequest,
		ClientRequest,ProtocolEvent {

	/**
	 * Transaction packet types.
	 */
	public enum PacketType implements IntegerPacketType {
//		FIXME: minor task to add comments and poperties for each packet type
		/**
		 * 
		 */
		LOCK_REQUEST(251),

		/**
		 * 
		 */
		UNLOCK_REQUEST(252),

		/**
		 * 
		 */
		ABORT_REQUEST(253),

		/**
		 * 
		 */
		COMMIT_REQUEST(254),

		/**
		 * 
		 */
		TX_STATE_REQUEST(255), 

		/**
		 *
		 */

		TX_OP_REQUEST (256),


		TX_INIT(257),

		LOCK_OK(258),

		RESULT(259),

		TX_CLIENT(261),

		TX_CLIENT_RESPONSE(262)
		;

		private final int number;

		PacketType(int t) {
			this.number = t;
		}

		public int getInt() {
			return number;
		}

		/**
		 * 
		 */
		public static final IntegerPacketTypeMap<PacketType> intToType = new IntegerPacketTypeMap<PacketType>(
				PacketType.values());
	}

	public TXResult response;
	/* The tuple <txid, initiator> is used to detect conflicting txids chosen by
	 * different initiating nodes. */
	public String txid;

	long requestId;

	final String leader;

    public  static enum Keys {
		TXID, LOCKID, INITIATOR,REQUESTID,LEADER,
	}

	static Random random=new Random();
	/**
	 * @param t
	 * @param txid 
	 */
	public TXPacket(IntegerPacketType t, String txid,String leader) {
		super(t);
		this.txid = txid;
		this.requestId=random.nextLong();
		this.leader = leader;
	}


	/**
	 * @param json
	 * @throws JSONException
	 */
	public TXPacket(JSONObject json) throws JSONException {
		super(json);
		this.txid = json.getString(Keys.TXID.toString());
		this.requestId=json.getLong(Keys.REQUESTID.toString());
		this.leader = json.getString(Keys.LEADER.toString());
	}

	@Override
	public IntegerPacketType getRequestType() {
		return PacketType.intToType.get(type);
	}


	@Override
	public String getServiceName() {
		// FIXME: This works for fixed groups only
		return leader;
	}

	@Override
	public long getRequestID() {
//		Unique Identifier for each request
//		This ensures callbacks
		return requestId;
	}

	@Override
	public ClientRequest getResponse() {
		return response;
	}

	@Override
	public boolean needsCoordination() {
		return true;
	}

	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		return new JSONObject();
	}



	/**
	 * @return The ID of the transaction to which this packet corresponds. The
	 *         initiator is set only after a transaction request has been
	 *         received by the entry server, so until then, the ID only has the
	 *         transaction number.
	 */
	public String getTXID() {
		return this.txid;
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject=super.toJSONObject();
		jsonObject.put(Keys.TXID.toString(),txid);
		jsonObject.put(Keys.REQUESTID.toString(),this.requestId);
		jsonObject.put(Keys.LEADER.toString(),leader);
		return jsonObject;
	}

	public PacketType getTXPacketType(){
		return (PacketType)getRequestType();
	}

	@Override
	public Object getType() {
		return getTXPacketType();
	}

	@Override
	public Object getMessage() {
		return this;
	}

	@Override
	public void setKey(Object key) {
		throw new RuntimeException("set Key event should never be called");
	}

	public String getLeader(){return  leader;}
}
