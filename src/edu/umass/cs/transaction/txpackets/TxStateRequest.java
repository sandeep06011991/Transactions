package edu.umass.cs.txn.txpackets;

import edu.umass.cs.txn.exceptions.ResponseCode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.Key;
import java.util.HashSet;
import java.util.Set;


/*Should Also contain the reason for committing or aborting*/
public class TxStateRequest extends TXPacket {

	enum KEYS{
		STATE,
		RPE,
		PREVACTIVES,
	};
	TxState state;

	Set<String> previousActives;

	ResponseCode rpe;


	public TxStateRequest(String txId, TxState state, String leader, ResponseCode rpe,Set<String> previousActives) {
		super(TXPacket.PacketType.TX_STATE_REQUEST, txId,leader);
		this.state=state;
		this.previousActives = previousActives;
		this.rpe = rpe;
		assert !(rpe == ResponseCode.LOCK_FAILURE && previousActives == null);
	}

	public boolean needsCoordination(){return true;}

	public TxStateRequest(JSONObject json) throws JSONException {
		super(json);
		state=TxState.valueOf(json.getString(KEYS.STATE.toString()));
		if(json.has(KEYS.PREVACTIVES.toString())){
			JSONArray jsonArray = json.getJSONArray(KEYS.PREVACTIVES.toString());
			previousActives = new HashSet<>();
			for (int i = 0;i<jsonArray.length();i++){
				previousActives.add(jsonArray.getString(i));
			}
		}
		rpe = ResponseCode.getResponseCodeFromInt(json.getInt(KEYS.RPE.toString()));
		assert !(rpe == ResponseCode.LOCK_FAILURE && previousActives == null);
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject jsonObject = super.toJSONObject();
		jsonObject.put(KEYS.STATE.toString(),state);
		if(previousActives!=null){
			jsonObject.put(KEYS.PREVACTIVES.toString(),previousActives);
		}
		jsonObject.put(KEYS.RPE.toString(),rpe.getInt());
		return jsonObject;
	}

	public Set<String> getPreviousActives(){return previousActives;}

	public ResponseCode getRpe() {
		return rpe;
	}

	public TxState getState() {
		return this.state;
	}

	@Override
	public Object getKey() {
		return this.txid;
	}
}
