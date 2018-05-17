package edu.umass.cs.txn.txpackets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.txn.interfaces.TxOp;
import redis.clients.jedis.Client;

import java.util.Random;

/**
 * @author arun
 *
 */
public class TxOpRequest extends TXPacket  {
	private static enum Keys {
		REQUEST,OPID
	}

	public ClientRequest request;


	public long opId=-10000;
	/**
	 * @param txid
	 * @param request
	 */
	public TxOpRequest(String txid, ClientRequest request,String leader) {
		super(TXPacket.PacketType.TX_OP_REQUEST, txid,leader);
		this.request = request;
		this.opId = request.getRequestID();
	}

	/**
	 * The parser should be able to convert a serialized request to either an
	 * app request or client reconfiguration packet as appropriate.
	 * 
	 * @param json
	 * @param parser
	 * @throws JSONException
	 * @throws RequestParseException
	 */
	public TxOpRequest(JSONObject json, AppRequestParser parser)
			throws JSONException, RequestParseException {
//		FIXME App Request parser should be fixed
		super(json);

		this.request =(ClientRequest) parser
				.getRequest(json.getString(Keys.REQUEST.toString()));
		this.opId	 =  json.getLong(Keys.OPID.toString());
	}


	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject=super.toJSONObject();

		if((request !=null) && (request instanceof JSONPacket)){
			jsonObject.put(Keys.REQUEST.toString(),((JSONPacket)request).toJSONObject());
//			jsonObject.put(Keys.REQUEST.toString(),((AppRequest)request).toJSONObject());
		}else{
			throw new RuntimeException("TxOpRequests must be JSON objects");
		}
	 	jsonObject.put(Keys.OPID.toString(),opId);
		return jsonObject;
	}

	public String getServiceName(){
		return this.request.getServiceName();
	}


	@Override
	public Object getKey() {
		return txid;
	}


}
