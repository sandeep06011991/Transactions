/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */
package edu.umass.cs.txn.testing.app;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.txn.DistTransactor;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.AppRequest.ResponseCodes;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.mapdb.TxEngine;
import org.omg.SendingContext.RunTime;

/**
 * @author Sandeep
 * 
 *         A calculator application to test transactions
 *         because 1*2+3 != 1*3+2
 */
public class CalculatorTX extends AbstractReconfigurablePaxosApp<String> implements
		Replicable, ClientMessenger, AppRequestParserBytes {

	private static final int DEFAULT_INIT_STATE = 0;
	// total number of reconfigurations across all records
	private int numReconfigurationsSinceRecovery = -1;
	private boolean verbose =true;

	private static final Logger log = Logger
			.getLogger(DistTransactor.class.getName());

	private class AppData {
		final String name;
		int state = DEFAULT_INIT_STATE;

		AppData(String name, int state) {
			this.name = name;
			this.state = state;
		}

		void  setState(int state) {
			this.state = state;
		}

		int getState() {
			return this.state;
		}

		void operate(OperateRequest.Operation operation,int obj){
			switch (operation){
				case add:
					state = state + obj;
					break;
				case multiply:
					state = state *obj;
					break;
			}

		}
	}

	private String myID; // used only for pretty printing
	private final HashMap<String, AppData> appData = new HashMap<String, AppData>();
	// only address based communication needed in app
	private SSLMessenger<?, JSONObject> messenger;

	/**
	 * Constructor used to create app replica via reflection. A reconfigurable
	 * app must support a constructor with a single String[] as an argument.
	 * 
	 * @param args
	 */
	public CalculatorTX(String[] args) {
	}

	// Need a messenger mainly to send back responses to the client.
	@Override
	public void setClientMessenger(SSLMessenger<?, JSONObject> msgr) {
		this.messenger = msgr;
		this.myID = msgr.getMyID().toString();
	}

	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		log.log(Level.INFO,"Participant: EXECUTE "+request.getServiceName().hashCode());

		if (request.toString().equals(Request.NO_OP))
			return true;

		if(request instanceof GetRequest){
//			System.out.println("processing get request");
			GetRequest getRequest = (GetRequest)request;
			String name= getRequest.getServiceName();
			int res= appData.get(name).state;
			getRequest.response = new ResultRequest(getRequest.getServiceName(),res,getRequest.requestID);
			log.log(Level.INFO,"operating on request ID "+((GetRequest) request).getRequestID());
					return true;
		}
		if(request instanceof OperateRequest){
			OperateRequest operateRequest = (OperateRequest)request;
			String name=operateRequest.getServiceName();
			AppData ap=appData.get(name);
			if(ap==null){
				System.out.println("High load error");
				ap = new AppData(name,0);
				appData.put(name,ap);
			}
			ap.operate(operateRequest.operation,operateRequest.object);
			return true;
		}
		System.out.println("Request Unimpelemnted"+request.toString());
		throw new RuntimeException("unimplemented");
	}

	private static final boolean DELEGATE_RESPONSE_MESSAGING = true;



	// no-op
	private boolean processStopRequest(AppRequest request) {
		return true;
	}

	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		try {
			return staticGetRequest(stringified);
		} catch (JSONException je) {
			ReconfigurationConfig.getLogger().fine(
					"App-" + myID + " unable to parse request " + stringified);
			throw new RequestParseException(je);
		}
	}

	/**
	 * We use this method also at the client, so it is static.
	 * 
	 * @param stringified
	 * @return App request
	 * @throws RequestParseException
	 * @throws JSONException
	 */
	public static Request staticGetRequest(String stringified)
			throws RequestParseException, JSONException {
		JSONObject jsonObject = new JSONObject(stringified);
		TxAppRequest.TxRequestType requestType= TxAppRequest.TxRequestType.getPacketTypeFromInt(jsonObject.getInt("type"));
		switch(requestType){
			case GET: return new GetRequest(jsonObject);
			case PUT:return new PutRequest(jsonObject);
			case RESULT:return new ResultRequest(jsonObject);
			case OPERATE:return new OperateRequest(jsonObject);

		}
		throw new RuntimeException("Request could not be parsed");
	}
	
	@Override
	public Request getRequest(byte[] bytes, NIOHeader header)
			throws RequestParseException {
		return staticGetRequest(bytes, header);
	}
	/**
	 * @param bytes
	 * @param header
	 * @return Request constructed from bytes.
	 * @throws RequestParseException
	 */
	public static Request staticGetRequest(byte[] bytes, NIOHeader header)
			throws RequestParseException {
		try {
			return staticGetRequest(new String(bytes, NIOHeader.CHARSET));
		} catch (UnsupportedEncodingException | JSONException e) {
			throw new RequestParseException(e);
		}
	}

	private static IntegerPacketType[] types = TxAppRequest.TxRequestType.values();

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return staticGetRequestTypes();
	}

	/**
	 * We use this method also at the client, so it is static.
	 * 
	 * @return App request types.
	 */
	public static Set<IntegerPacketType> staticGetRequestTypes() {
		return new HashSet<IntegerPacketType>(Arrays.asList(types));
	}

	@Override
	public boolean execute(Request request) {
		return this.execute(request, false);
	}

	@Override
	public String checkpoint(String name) {
		AppData data = this.appData.get(name);
		return data != null ? String.valueOf(data.getState()) : null;
	}

	@Override
	public boolean restore(String name, String state) {
		AppData data = this.appData.get(name);
		int number;
		try{
			number = Integer.parseInt(state);
		}catch(NumberFormatException nfe){
			number = 0;
			if(!state.equals("{}"))
				throw new RuntimeException("This should never happen" +state);
		}

		// if no previous state, this is a creation epoch.
		if (data == null && state != null) {
			data = new AppData(name, number);
			if (verbose)
				System.out.println(">>>App-" + myID + " creating " + name
						+ " with state " + state);
			numReconfigurationsSinceRecovery++;
		}
		// if state==null => end of epoch
		else if (state == null) {
			if (data != null)
				if (verbose)
					System.out.println("App-" + myID + " deleting " + name
							+ " with final state " + data.state);
			this.appData.remove(name);
			assert (this.appData.get(name) == null);
		} 
		// typical reconfiguration or epoch change
		else if (data != null && state != null) {
			System.out.println("App-" + myID + " updating " + name
					+ " with state " + state);
			data.state = number;
		} 
		else
			// do nothing when data==null && state==null
			;
		if (state != null)
			this.appData.put(name, data);

		return true;
	}

	public String toString() {
		return CalculatorTX.class.getSimpleName();
	}
}
