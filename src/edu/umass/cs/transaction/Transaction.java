package edu.umass.cs.transaction;

import java.net.InetSocketAddress;

import java.security.Key;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.transaction.interfaces.TxOp;
import edu.umass.cs.utils.GCConcurrentHashMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.Client;

/**
 * @author arun
 *
 * 
 *         A transaction is an indivisible sequence of operations that satisfy
 *         ACID (atomicity, consistency, isolation, and durability) properties.
 * 
 *         <p>
 * 
 *         The list of individual {@link TxOp} operations must either be a
 *         {@link ClientRequest} or {@link ClientReconfigurationPacket}. If any
 *         of the TxOp operations is a {@link DeleteServiceName} request, the
 *         deletion(s) need to be implemented as an in-memory pre-delete
 *         operation at both reconfigurators and active replicas so that they
 *         can be reverted if necessary.
 *
 */
public class Transaction extends JSONObject {

	protected static enum Keys {
		OPS, TXID, DELETES,NODEID,REQUESTID,CLIENTADDR,ENTRYSERVER,LEADER
	}


	// a transaction number chosen to be unique at each client
	// not appended by the IP address of the internal server used only internally
	private final String txnId;

	// the server issuing the transaction
	public InetSocketAddress entryServer;

	public InetSocketAddress clientAddr;
	public long requestId=-1;
	// Current Leader nodeID
	String nodeId;

	ArrayList<ClientRequest> requests;
	/**
	 * @param entryServer
	 *
	 *
	 */

	final String leader;

	public Transaction(InetSocketAddress entryServer ,ArrayList<ClientRequest> requests,String nodeId,InetSocketAddress clientAddr,long requestId,String leader) {
		this.txnId = Long.toString(requestId);
//				getNewTxid(entryServer);
		this.entryServer = entryServer;
//		FIXME: Should this really be a request
		this.requests = requests;
		this.nodeId=nodeId;
		this.clientAddr = clientAddr;
		this.requestId = requestId;
		this.leader = leader;
	}

	/**
	 * @return The set of lock identifiers needed for this transaction in
	 *         lexicographic order. The lock identifiers are in general a subset
	 *         of participant groups in the transaction as name creation and
	 *         deletion requests are excluded from this list.
	 */
	public TreeSet<String> getLockList() {
//		FIXME: The return has to lexicographic to avoid deadlock
		TreeSet<String> set=new TreeSet<>();
		for (Request request : requests) {
			set.add(request.getServiceName());
		}
		return set;
	}

	public ArrayList<ClientRequest> getRequests() {
		return requests;
	}

	/**
	 * @return An ID for this transaction created by concatenating the issuer's
	 *         address and the long transaction number; for safety, this ID must
	 *         be unique across all transactions in the system, so the same
	 *         issuer must not issue different transactions with the same
	 *         transaction number.
	 */

	public String getTXID() {
		return this.txnId;
	}



	private static final long DEFAULT_TIMEOUT = 3600 * 1000;
	private static GCConcurrentHashMap<Long, String> txids = new GCConcurrentHashMap<Long, String>(
			DEFAULT_TIMEOUT);

	private synchronized static String getNewTxid(InetSocketAddress initiator) {
		String init=initiator.getAddress().getHostAddress() + ":"
				+ initiator.getPort();
		long txid = (long) (Math.random() * Long.MAX_VALUE);
		while (txids.contains(txid))
			txid = (long) (Math.random() * Long.MAX_VALUE);
		txids.put(txid, init);
		return init+Long.toString(txid);
	}


	protected synchronized static String releaseTxid(long txid) {
//		FIXME:: Build functionality to recycle Ids
		return txids.remove(txid);
	}



	public JSONObject toJSONObject() throws JSONException{
		JSONObject jsonObject=new JSONObject();
		jsonObject.put(Keys.TXID.toString(),txnId);
		ArrayList<JSONObject> j=new ArrayList<>();
		for(Request request:requests){
			j.add(((JSONPacket)request).toJSONObject());
		}
		jsonObject.put(Keys.OPS.toString(),j);
		jsonObject.put(Keys.NODEID.toString(),nodeId);
		if(requestId!=-1){jsonObject.put(Keys.REQUESTID.toString(),requestId);}
		jsonObject.put(Keys.CLIENTADDR.toString(),clientAddr);
		jsonObject.put(Keys.ENTRYSERVER.toString(),entryServer);
		jsonObject.put(Keys.LEADER.toString(),leader);
		return jsonObject;
	}

	private static InetSocketAddress getSocketAddrFromString(String str){
		str = str.replace("/","");
		String[] a= str.split(":");
		return new InetSocketAddress(a[0],Integer.parseInt(a[1]));
	}


	public Transaction(JSONObject jsonObject, AppRequestParser appRequestParser) throws JSONException {
		txnId = jsonObject.getString(Keys.TXID.toString());
		JSONArray ops = jsonObject.getJSONArray(Keys.OPS.toString());
		requests = new ArrayList<>();
		for (int i = 0; i < ops.length(); i++) {
			try{
			requests.add((ClientRequest)appRequestParser.getRequest(ops.get(i).toString()));
				}catch (RequestParseException rpe){
				throw new JSONException("Could understand this request");
				}
			}
		nodeId = jsonObject.getString(Keys.NODEID.toString());
		requestId = jsonObject.getLong(Keys.REQUESTID.toString());
		clientAddr = getSocketAddrFromString(jsonObject.getString(Keys.CLIENTADDR.toString()));
		entryServer=getSocketAddrFromString(jsonObject.getString(Keys.ENTRYSERVER.toString()));
		leader = jsonObject.getString(Keys.LEADER.toString());
	}


	public String getLeader(){return  leader;}

	public long getRequestId() {
		return requestId;
	}
}


