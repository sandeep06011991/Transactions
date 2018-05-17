package edu.umass.cs.transaction.testing.app;

import org.json.JSONException;
import org.json.JSONObject;

public class GetRequest extends TxAppRequest{

    public GetRequest(String serviceName) {
        super(TxRequestType.GET, serviceName);
    }

    public GetRequest(JSONObject json) throws JSONException {
        super(json);
    }

    @Override
    public JSONObject toJSONObject() throws JSONException {
        return super.toJSONObject();
    }

}
