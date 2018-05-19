package edu.umass.cs.transaction;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.transaction.exceptions.TXException;
import edu.umass.cs.transaction.exceptions.TxnState;
import edu.umass.cs.transaction.interfaces.TXLocker;
import org.omg.SendingContext.RunTime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author arun
 *
 */
public class TXLockerMap implements TXLocker {

	/**
	 * A blocking call that returns upon successfully locking {@code lockID} or
	 * throws a {@link TXException}. Locking a group involves synchronously
	 * checkpointing its state and maintaining in memory its locked status.
	 * 
	 * @param lockID
	 * @throws TXException
	 */

	private AbstractReplicaCoordinator app;

	TXLockerMap(AbstractReplicaCoordinator abstractReplicaCoordinator){
		app = abstractReplicaCoordinator;
	}


	private static final Logger log = Logger
			.getLogger(DistTransactor.class.getName());

//	<Service name, Tx ID>
//	Service is locked iff present in the map with corresponding locking transaction
	private HashMap<String,String> txMap=new HashMap<>();
//	<Service name , State string>
//	the string is the checkpointed state stored at the start of the transaction
//	this string is used to restore back state on abort
	private HashMap<String,String> stateMap=new HashMap<>();
//	FIXME: Get clearance from Arun
//	<Service name, request ID>
//	request ID is added to this list after recieving a valid request
//	Set of allowed requests
	/*
	* To handle the execute,
	* The distTransactor recieves the TxExecute Packet
	* checks if allowed and puts into the request queue
	* and recursively calls itself with the unwrapped request.
	* The distTransactor checks if this request is allowed
	* and lets it flow into the underlying app
	* */
	private HashMap<String,HashSet<Long>> allowedRequests = new HashMap<>();
// 	ServiceName, TxnState
	private HashMap<String,TxnState> txnStateHashMap = new HashMap<>();

	@Override
	public boolean lock(String serviceName,String lockID,Set<String> leaderQuorum){
		if(txMap.containsKey(serviceName)){
			String currlockId=txMap.get(serviceName);
//			Lock requests are idempotent
			return currlockId.equals(lockID);
		}
		if(!(txMap.containsKey(serviceName))){
			String state=app.checkpoint(serviceName);
			log.log(Level.INFO,"Participant: LOCK "+serviceName.hashCode());
			txMap.put(serviceName,lockID);
			stateMap.put(serviceName,state);
			txnStateHashMap.put(serviceName,new TxnState(lockID,state,leaderQuorum));
			return true;
		}
		return false;
	}


	/**
	 * A blocking call that returns upon successfully release {@code lockID} or
	 * throws a {@link TXException} .
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	public boolean unlock(String serviceName,String lockID)  {
//		System.out.println("Unlocking"+serviceName+":	"+lockID);
		if((txMap.containsKey(serviceName))){
			String lckID=txMap.get(serviceName);
			if(lckID.equals(lockID)){
				stateMap.remove(serviceName);
				txMap.remove(serviceName);
				if(allowedRequests.containsKey(serviceName)){
					allowedRequests.remove(serviceName);
				}
				txnStateHashMap.remove(serviceName);
				return true;
			}else{
//				Could happen on recieving a delayed message
				log.log(Level.INFO,"Investigate attempting to unlock "+serviceName+" not held by "+lockID);
//				throw new RuntimeException("Attempting to unlock lock not held by you");
			}
		}
//		unlock requests are idempotent
//		System.out.println("Couldnt find my lock");
		return true;
	}
/* Returns
* 	handled: false if this is a new request
* 	and true if the request is already handled
* 	FIXME: allowRequest should be a void not a boolean return
*   should throw an exception when a repeated request is given
*
*/
	public boolean allowRequest(long requestId,String txID,String serviceName){
		if(txMap.containsKey(serviceName) && txMap.get(serviceName).equals(txID)){
			if(!allowedRequests.containsKey(serviceName)){
				HashSet<Long> set = new HashSet<>();
				set.add(new Long(requestId));
				allowedRequests.put(serviceName,set);
				return false;
			}else{
				HashSet<Long> set= allowedRequests.get(serviceName);
				if(set.contains(requestId)){return  true;}
				set.add(requestId);
				return false;
			}
		}
//		Could be a delayed message
		log.log(Level.INFO,"Investigate: Recieved a txExecute with no matching locks txID"+txID+" serviceName "+serviceName);
		return true;
		//		throw new RuntimeException("Only a locked group recieves TxOP request");
	}

	public boolean isAllowedRequest(ClientRequest clientRequest){
		String serviceName=clientRequest.getServiceName();
		if(!isLocked(serviceName)){return true;}
		if(allowedRequests.containsKey(serviceName)&& allowedRequests.get(serviceName).contains(clientRequest.getRequestID())){
			TxnState state = txnStateHashMap.get(serviceName);
			state.add_request(clientRequest);
			return true;
		}
		return false;
	}

	/*Is the service name locked by any transaction
	* used to filter incoming requests */
	public boolean isLocked(String serviceName){
		if(txMap.containsKey(serviceName)){
			if(!txnStateHashMap.containsKey(serviceName)){
				log.log(Level.INFO,"Investigate locked but not txnState for "+serviceName);
			}
			assert stateMap.containsKey(serviceName);
			assert txnStateHashMap.containsKey(serviceName);
			return true;
		}
		return false;
	}

	/*Is this */
	public boolean isLockedByTxn(String serviceName, String txId){
		if(txMap.containsKey(serviceName)){
			String _txId=txMap.get(serviceName);
			return txId.equals(_txId);
		}
		return false;
	}

	public void updateStateMap( String serviceName,TxnState state){
		txnStateHashMap.put(serviceName,state);
		stateMap.put(serviceName,state.state);
		txMap.put(serviceName,state.txId);
		HashSet<Long> test = new HashSet<>();
		for(Long i: state.getRequestId()){
			test.add(i);
		}
		allowedRequests.put(serviceName,test);
	}

	public TxnState getStateMap(String serviceName){
		return txnStateHashMap.get(serviceName);
	}


	public void printStats(){
		System.out.println("Tx Map "+txMap.size());
		System.out.println("Tx Map "+txnStateHashMap.size());
	}
}
