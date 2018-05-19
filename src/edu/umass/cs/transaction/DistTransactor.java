package edu.umass.cs.transaction;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync.ReconfigurationException;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import edu.umass.cs.transaction.exceptions.ResponseCode;
import edu.umass.cs.transaction.exceptions.TxnState;
import edu.umass.cs.transaction.interfaces.TXLocker;
import edu.umass.cs.transaction.protocol.*;
import edu.umass.cs.transaction.txpackets.*;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 * 
 *         This class is used at a node pushing forward the transactions steps,
 *         which is normally the primary designate in the transaction group. If
 *         the primary crashes, a secondary might use this class for the same
 *         purpose.
 *         Dist Transactor is a wrapper class around AbstractReplicaCoordinator
 * @param <NodeIDType>
 */
public class DistTransactor<NodeIDType> extends AbstractTransactor<NodeIDType>
		 {

	/**
	 * A distributed transaction processor needs a client to submit transaction
	 * operations as well as to acquire and release locks.
	 */
	 public  ReconfigurableAppClientAsync<Request> gpClient;
	
	 ProtocolExecutor<NodeIDType,TXPacket.PacketType,String> protocolExecutor;

//	A Storage structure for locks
	private  TXLocker txLocker;
//  A wrapper around coordMessenger to send TxPackets
	private TxMessenger txMessenger;

	private Messenger coordMessenger;

	private static final Logger log = Logger
			.getLogger(DistTransactor.class.getName());

	HashMap<String,LeaderState> leaderStateHashMap = new HashMap<>();

	public DistTransactor(){}

	@SuppressWarnings("unchecked")
	@Override
	public void setCoordinator(AbstractReplicaCoordinator<NodeIDType> coordinator,Messenger messenger)
		{
		super.setCoordinator(coordinator,messenger);
		try {
			this.gpClient = TXUtils.getGPClient(this);
		}catch (IOException e){
			log.log(Level.SEVERE,"Gpclient could not be initialized, Major error.");
		}
		this.txLocker = new TXLockerMap(coordinator);
		coordMessenger = messenger;
		txMessenger=new TxMessenger(this.gpClient,messenger,coordinator.getMyID());
		protocolExecutor=new ProtocolExecutor<>(txMessenger);
		txMessenger.setProtocolExecutor(protocolExecutor);

	}

	@Override
	public Request getRequest(String str) throws RequestParseException {
		try {
			JSONObject jsonObject = new JSONObject(str);
			TXPacket.PacketType packetId = TXPacket.PacketType.intToType.get(jsonObject.getInt("type"));
			if (packetId != null) {
				switch (packetId) {
					case TX_INIT:
						return new TXInitRequest(jsonObject, this.getCoordinator());
					case LOCK_REQUEST:
						return new LockRequest(jsonObject);
					case TX_OP_REQUEST:
						return new TxOpRequest(jsonObject, this.getCoordinator());
					case UNLOCK_REQUEST:
						return new UnlockRequest(jsonObject);
					case RESULT:
						return new TXResult(jsonObject);
					case TX_STATE_REQUEST:
						return new TxStateRequest(jsonObject);
					case TX_CLIENT:
						return new TxClientRequest(jsonObject, this.getCoordinator());
					case TX_CLIENT_RESPONSE:
						return new TxClientResult(jsonObject);

					default:
						throw new RuntimeException("Forgot handling some TX packet");
				}
			}
		} catch (JSONException e) {
			throw new RequestParseException(e);
		}
		return null;
	}

	public  Request getRequest(byte[] bytes, NIOHeader header)
			throws RequestParseException{
//		FIXME: These methods are highly specific
		try{
			String str=new String(bytes, NIOHeader.CHARSET);
			Request request=getRequest(str);
			if(request instanceof TxClientRequest){
				TxClientRequest t=(TxClientRequest) request;
				t.clientAddr=header.sndr;
				t.recvrAddr= header.rcvr;
			}
			return request;
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	boolean isFixedTXGroupCreated = false;

	@SuppressWarnings("unchecked")
	void initialization(){
		if((TxConstants.fixedGroups && !isFixedTXGroupCreated)){
			Set<InetSocketAddress> addresses = new HashSet<>();
			addresses.addAll(PaxosConfig.getActives().values());

			AbstractReplicaCoordinator<NodeIDType> appCoordinator = this.getCoordinator();
//			FixMe: Slightly patchy fix this later.
			for(int i=0;i<TxConstants.noFixedGroups;i++){
				String groupName = TxConstants.coordGroupPrefix+i;
				if(appCoordinator.getReplicaGroup(groupName)==null){
					try {
						this.gpClient.sendRequest(new CreateServiceName(groupName, "0", addresses));
					}catch (IOException|ReconfigurationException rfe){
							rfe.printStackTrace();
						}
					}
			isFixedTXGroupCreated = true;
			}
		}
	}

	Random random = new Random();

	String getLeader(TxClientRequest txClientRequest){
		if(TxConstants.fixedGroups){
			return TxConstants.coordGroupPrefix+random.nextInt(TxConstants.noFixedGroups);
		}
		return txClientRequest.getRequests().get(0).getServiceName();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean preExecuted(Request request) {
		initialization();
		if(request==null){return false;}
		if(request instanceof TxClientResult) {
			TxClientResult txClientResult = (TxClientResult) request;
			try {
				((JSONMessenger)this.coordMessenger).sendClient(txClientResult.getClientAddr(),txClientResult,txClientResult.getServerAddr());
			} catch (JSONException|IOException e) {
				System.out.println("Unable to send to client");
			}
			return true;
		}

		if(request instanceof TxClientRequest) {
			TxClientRequest txClientRequest=(TxClientRequest)request;
			String leader = getLeader(txClientRequest);
			log.log(Level.INFO,"Recieved txClient Request "+txClientRequest.getRequestID()+"Recieved");
			Transaction transaction = new Transaction(txClientRequest.recvrAddr,
					((TxClientRequest) request).getRequests(), (String) getMyID(),
					txClientRequest.clientAddr,txClientRequest.getRequestID(),leader);
			log.log(Level.INFO,"Mapping  a txID "+transaction.getTXID());

			try {

				this.gpClient.sendRequest(new TXInitRequest(transaction), (RequestCallback) null);

			}catch (IOException ex){
				log.log(Level.INFO,"Exception:"+ex.getMessage());
//					throw new RuntimeException("Unable to send Transaction to Fixed Groups");
				}
			return true;
		}

//		FIXME: Minor Redundancy why is nodeID being stored both at the transaction
//		FIXME: and inside the secondary transaction protocol
		if(request instanceof TXInitRequest){
			TXInitRequest trx=(TXInitRequest)request;
			/*In case of fixed transaction groups, allowing unlimited transactions to happen would increase the state
			* that has to be stored per transaction. I am not sure what is the upper limit but for now I am
			* restricting it . If on stress testing this doesnt seem to be a bottle neck delete this clause*/
			if((leaderStateHashMap.get(trx.transaction.leader)!=null)&&(leaderStateHashMap.get(trx.transaction.leader).ongoingTxnHashMap.size()>TxConstants.MAX_CONCURRENT_TXN)){
				if(!trx.transaction.nodeId.equals(this.getMyID()))return true;
				txMessenger.sendObject(new TxClientResult(trx.transaction,ResponseCode.OVERLOAD,null));
				return true;
			}
//			These are wierd fixes when participant groups behave as leaders
			Set<String> leaderActives = (Set<String>)this.getCoordinator().getReplicaGroup(trx.transaction.getLeader());
			boolean flag = true;
			if(trx.transaction.nodeId.equals(this.getMyID())){
				if(TxConstants.fixedGroups){
					this.protocolExecutor.spawnIfNotRunning(new TxLockProtocolTask<NodeIDType>(trx.transaction, protocolExecutor,
							leaderActives));
				}else {
						if (!txLocker.isLocked(trx.transaction.leader)) {
							/* When a participant which is acting as a leader is checkpointed
							it state which is altered with the ongoing Txn and these
							details are  captured in the JSON . This is nonsensical
							 Quick solution was to lock it and checkpoint before adding additional details
							 of ongoing Transaction*/
							txLocker.lock(trx.transaction.getLeader(), trx.getTXID(), leaderActives);
							this.protocolExecutor.spawnIfNotRunning(new TxLockProtocolTask<NodeIDType>(trx.transaction, protocolExecutor,
									leaderActives));
						} else {
	//						If leader which is a participant is locked, transaction would anyway be aborted
							log.log(Level.INFO, "leader already locked" + trx.transaction.getTXID());
							leaderActives = txLocker.getStateMap(trx.transaction.leader).getLeaderQuorum();
							txMessenger.sendObject(new TxClientResult(trx.transaction, ResponseCode.LOCK_FAILURE, leaderActives));
							return true;
						}
					}
			}else {
				if (TxConstants.fixedGroups) {
					this.protocolExecutor.spawnIfNotRunning(
							new TxSecondaryProtocolTask<>
									(trx.transaction, TxState.INIT, protocolExecutor,
											leaderActives));
				} else {
					if (!txLocker.isLocked(trx.transaction.leader)) {
						txLocker.lock(trx.transaction.getLeader(), trx.getTXID(), leaderActives);
						this.protocolExecutor.spawnIfNotRunning(
								new TxSecondaryProtocolTask<>
										(trx.transaction, TxState.INIT, protocolExecutor,
												leaderActives));
					} else {
						log.log(Level.INFO, "leader already locked" + trx.transaction.getTXID());
						return true;
					}
				}
			}
			String leader_name=trx.transaction.getLeader();
			LeaderState leaderState;
			if((leaderState = leaderStateHashMap.get(leader_name))==null){
				leaderState = new LeaderState(leader_name);
			}
			leaderState.insertNewTransaction(new OngoingTxn(trx.transaction,TxState.INIT));
			leaderStateHashMap.put(leader_name,leaderState);
			return true;
		}

		if(request instanceof LockRequest){
			LockRequest lockRequest=(LockRequest)request;
			boolean success=txLocker.lock(lockRequest.getServiceName(),lockRequest.txid,lockRequest.getLeaderActives());
			TXResult result=
					new TXResult(lockRequest.txid,lockRequest.getTXPacketType(),
							success,(String) lockRequest.getKey(),lockRequest.getServiceName(),lockRequest.getLeader());
			result.setRequestId(lockRequest.getRequestID());
			lockRequest.response=result;
			if(!success && txLocker.isLocked(lockRequest.getServiceName())){
				result.setActivesOfPreviousLeader(txLocker.getStateMap(lockRequest.getServiceName()).getLeaderQuorum());
			}
			if(success){
				log.log(Level.INFO,"Recieved type: LOCKOK "+((LockRequest) request).getTXID());
			}else{
				log.log(Level.INFO,"Recieved type: LOCKFAIL "+((LockRequest) request).getTXID());
			}
			return true;
		}

		if(request instanceof UnlockRequest){
			UnlockRequest unlockRequest=(UnlockRequest)request ;
			if(txLocker.isLocked(unlockRequest.getServiceName())) {
				if (txLocker.isLockedByTxn(unlockRequest.getServiceName(), unlockRequest.getLockID())) {
					if (!unlockRequest.isCommited()) {
						restore(unlockRequest.getServiceName(), txLocker.getStateMap(unlockRequest.getServiceName()).state);
					}
					if(unlockRequest.isCommited()){
						log.log(Level.INFO,"Participant: COMMIT "+ unlockRequest.getServiceName().hashCode());
					}else{
						log.log(Level.INFO,"Participant: ABORT "+unlockRequest.getServiceName().hashCode());
					}
					txLocker.unlock(unlockRequest.getServiceName(), unlockRequest.txid);
				} else {
//					throw new RuntimeException("How can you attempt to unlock a lock not held by you");
//					Could be an old lock message, if group is not locked by txn
				}
			}
//			This operation always succeeds
			TXResult txResult= new TXResult(unlockRequest.txid,unlockRequest.getTXPacketType(),
					true,(String) unlockRequest.getKey(),unlockRequest.getServiceName(),unlockRequest.getLeader());;
			txResult.setRequestId(unlockRequest.getRequestID());
			unlockRequest.response=txResult;
/*If 2 unlock requests where sent by the same co-ordinator and response is reordered
* it would wait for one of the 2 responses */
			return true;
		}

		if(request instanceof TxOpRequest){
//			FIXME: Op ID on TXOP
			TxOpRequest txOpRequest=(TxOpRequest) request;
			boolean success=txLocker.isLockedByTxn(txOpRequest.getServiceName(),txOpRequest.getTXID());
			if(success){
				log.log(Level.INFO,"Recieved type: TXOP "+((TxOpRequest) request).getTXID());
				boolean handled=txLocker.allowRequest(txOpRequest.request.getRequestID(),txOpRequest.txid,txOpRequest.getServiceName());
				if(!handled) this.execute(txOpRequest.request,true,null);
			}
			TXResult result=new TXResult(txOpRequest.txid,txOpRequest.getTXPacketType(),success
							,(String) txOpRequest.getKey(),Long.toString(txOpRequest.opId),txOpRequest.getLeader());
			result.setRequestId(txOpRequest.getRequestID());
			txOpRequest.response=result;
			return true;
			}

		if(request instanceof TxStateRequest){
			TransactionProtocolTask protocolTask=(TransactionProtocolTask) protocolExecutor.getTask(((TxStateRequest) request).getTXID());
			if(protocolTask!=null){
//				logic to not change state from COMMIT/ABORT is inside each protocol task
				protocolTask.onStateChange((TxStateRequest) request);
				LeaderState state= leaderStateHashMap.get(request.getServiceName());
				state.updateTransaction(((TxStateRequest) request).getTXID(),((TxStateRequest) request).getState());
				if(state.isEmpty()){
					leaderStateHashMap.remove(request.getServiceName());
				}
			}
			log.log(Level.INFO,"Recieved type: "+((TxStateRequest) request).getState()+" "+((TxStateRequest) request).getTXID());

//			Similar logic to not change state from INIT or COMMIT is inside leaderState
			return true;
		}
		if((request instanceof ClientRequest)){
			if(txLocker.isAllowedRequest((ClientRequest) request)){
				log.log(Level.INFO,"Recieved request with ID"+((ClientRequest) request).getRequestID());
				return false;
				}

//			FixMe: Can do some Exception handling here
			System.out.println("DROPPING REQUEST. SYSTEM BUSY");
			return true;
		}

		return false;
	}



	public synchronized String preCheckpoint(String name) {
		/*The main goal of this method is to capture the state of a transaction
		* at the participants and coordinators . */
		if(!txLocker.isLocked(name)){return null;}
		JSONObject jsonObject= new JSONObject();
		try {
			if (txLocker.isLocked(name)) {
				jsonObject.put("txLocker", txLocker.getStateMap(name).toJSONObject());
			}
			if(leaderStateHashMap.containsKey(name)){
				jsonObject.put("leader",leaderStateHashMap.get(name).toJSONObject(name));
			}

		return jsonObject.toString();
		}catch(JSONException ex){
			throw new RuntimeException("Conversion to JSON is flawed");

		}
	}

	@SuppressWarnings("unchecked")
	public synchronized boolean preRestore(String name, String state) {
		try {
//			System.out.println("Attempting to prerestore" + name+" "+state);
			JSONObject jsonObject = new JSONObject(state);
			if(!(jsonObject.has("txLocker") ||(jsonObject.has("leader")))){
				return false;
			}
		}catch (JSONException ex){
			try{
				Integer.parseInt(state);
//				Just for testing
			}catch (NumberFormatException nfe){
				System.out.println("Could not prerestore"+name+" "+state);
			}
			return false;
		}

		try {
//			System.out.println("Attempting to restore"+name+"	: "+state);
			JSONObject jsonObject = new JSONObject(state);
			if(jsonObject.has("txLocker")){
				TxnState txnState=new TxnState(jsonObject.getJSONObject("txLocker"));
				this.getCoordinator().restore(name,txnState.state);
				txLocker.updateStateMap(name,txnState);
				for(String req_string:txnState.getRequests()){
					Request request=this.getCoordinator().getRequest(req_string);
					this.getCoordinator().execute(request,true);
				}
			}
			if(jsonObject.has("leader")){
//				FixMe: Repeat some code for a quick fix
//				FixMe:VERY DANGEROUS FIX THINK THROUGH THIS
				/*
				If a group is both a group both a participant and leader
				on rollback executing this function would change the state
				* */
				if(leaderStateHashMap.containsKey(name))return true;
				LeaderState leaderState = new LeaderState(jsonObject.getJSONObject("leader"),this.getCoordinator());
				leaderStateHashMap.put(name,leaderState);
				for(OngoingTxn ongoingTxn:leaderState.ongoingTxnHashMap.values()){
					Transaction transaction = ongoingTxn.transaction;
					Set<String > leaderActives =(Set<String>) this.getCoordinator().getReplicaGroup(transaction.getLeader());
					if(protocolExecutor.isRunning(transaction.getTXID())){continue;}
					switch(ongoingTxn.txState){
						case INIT:	if(transaction.nodeId.equals(getMyID())){
										this.protocolExecutor.spawnIfNotRunning(new TxLockProtocolTask<NodeIDType>(transaction,protocolExecutor,
												leaderActives));
									}else{
										this.protocolExecutor.spawnIfNotRunning(
												new TxSecondaryProtocolTask<>
														(transaction,TxState.INIT,protocolExecutor,leaderActives));
									}
									break;
						case COMMITTED:	protocolExecutor.spawnIfNotRunning(new TxCommitProtocolTask<>(ongoingTxn.transaction,protocolExecutor));
										break;
						case ABORTED:	// FixME: Major approximation that we are not counting transactions where machines have woken up.
										protocolExecutor.spawnIfNotRunning(new TxAbortProtocolTask<>(ongoingTxn.transaction,protocolExecutor,null, ResponseCode.TIMEOUT));
										break;
						case COMPLETE: throw new RuntimeException("If it was complete why would it be recorded");
					}
				}

			}
			return true;
		}catch(JSONException j){
			j.printStackTrace();
			System.out.println("not a jsonObject" +state);
		}catch(RequestParseException rpe){
			System.out.println("not a request");
		}
//		System.out.println("Flowing into the system "+name+":"+state);
		return false;
	}



	 @Override
	 public void executed(Request request, boolean handled) {
		return;
	 }

 }