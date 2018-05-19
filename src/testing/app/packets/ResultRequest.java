package testing.app.packets;

import org.json.JSONException;
import org.json.JSONObject;

public class ResultRequest extends TxAppRequest {

    enum keys{
        RESULT
    }

    int result;



    public ResultRequest( String serviceName, int result,long requestID) {
        super(TxRequestType.RESULT, serviceName);
        this.requestID = requestID;
        this.result = result;

    }

    public ResultRequest(JSONObject json) throws JSONException {
        super(json);
        result = json.getInt(keys.RESULT.toString());

    }

    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject= super.toJSONObject();
        jsonObject.put(keys.RESULT.toString(),result);
        return jsonObject;
    }

    public int getResult() {
       return result;
    }
}
