package edu.umass.cs.transaction;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.GigaPaxosClient;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.transaction.txpackets.TXPacket;
import edu.umass.cs.transaction.txpackets.TxStateRequest;

/**
 * @author arun
 * 
 *         A class with static utility methods related to transactions.
 *  	  Deleted:
 *  	  This class contained method to finish batch of TxRequests. Performs each in
 *  	  async but blocks till all finish.Protocol Executor does the same, therefore delete.
 *
 */
public class TXUtils  {

	/**
	 * Creates an async gigapaxos client without checking connectivity (because
	 * the reconfigurators may not yet have been initialized.
	 * 
	 * @param app
	 * @return The created client.
	 * @throws IOException
	 */

	protected static ReconfigurableAppClientAsync<Request> getGPClient(AppRequestParser app)
			throws IOException {
		return new ReconfigurableAppClientAsync<Request>(
				ReconfigurationConfig.getReconfiguratorAddresses(), false) {

			@Override
			public Request getRequest(String stringified)
					throws RequestParseException {
				return app.getRequest(stringified);
			}

			@Override
			public Request getRequest(byte[] message, NIOHeader header)
					throws RequestParseException {
				return app.getRequest(message, header);
			}

			@Override
			public Set<IntegerPacketType> getRequestTypes() {
				Set<IntegerPacketType> set;
				set=app.getRequestTypes();
				set.addAll(TXPacket.PacketType.intToType.values());
				return set;
//				return app.getRequestTypes().addAll(TXPacket.PacketType.intToType.values());
			}
			
			public String getLabel() {
				return TXUtils.class.getSimpleName() + "."+GigaPaxosClient.class.getSimpleName();
			}
		};
	}


}
