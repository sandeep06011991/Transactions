package edu.umass.cs.txn;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.CoordinatorCallback;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.utils.Config;

/**
 * @author arun
 * Deleted:
 * A lot of code was doing the same as is done in super class
 * this.coordinator was doing what this.app does in parent class
 * Not required as app=coordinator
 * @param <NodeIDType>
 */


public  abstract class AbstractTransactor<NodeIDType> implements CoordinatorCallback {

	private AbstractReplicaCoordinator<NodeIDType> coordinator;

	protected  AbstractTransactor(){}

	public NodeIDType getMyID() {
		return coordinator.getMyID();
	}

	@Override
	public void setCoordinator(AbstractReplicaCoordinator coordinator) {
		this.coordinator = coordinator;
		assert (coordinator instanceof PaxosReplicaCoordinator);
		this.coordinator.setCallback(this);
	}


	private static final boolean ENABLE_TRANSACTIONS = Config
			.getGlobalBoolean(RC.ENABLE_TRANSACTIONS);


	private Set<IntegerPacketType> cachedRequestTypes = null;

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		if (cachedRequestTypes != null)
			return cachedRequestTypes;
		Set<IntegerPacketType> types = new HashSet<IntegerPacketType>();
		types.addAll(TXPacket.PacketType.intToType.values());
		return cachedRequestTypes = types;
	}


//	Rigging methods to underlying coordinator
//	AbstractReplicaCoordinator takes in coordinator but stores the app
//	directly calling the parent class for the methods below imply
//	that the paxos cordinator is by passed
	/*Intercept execute requests here*/
	public boolean execute(Request request, boolean noReplyToClient){
//		FixMe: Avoid this
		return this.coordinator.execute(request,noReplyToClient);
	}


	public AbstractReplicaCoordinator<NodeIDType> getCoordinator() {
		return coordinator;
	}

	public boolean execute(Request request) {
		return this.coordinator.execute(request);
	}


	public boolean execute(Request request, boolean noReplyToClient,
						   ExecutedCallback requestCallback) {
		return this.coordinator.execute(request, noReplyToClient, requestCallback);
	}

	public boolean restore(String name, String state) {
		return this.coordinator.restore(name,state);
	}

	public String checkpoint(String name) {
		return this.coordinator.checkpoint(name);
	}


}
