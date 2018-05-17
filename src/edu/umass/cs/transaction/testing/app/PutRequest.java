package edu.umass.cs.txn.testing.app;

import org.json.JSONException;
import org.json.JSONObject;

public class  PutRequest extends TxAppRequest{

    public PutRequest(String serviceName) {
        super(TxRequestType.PUT, serviceName);
    }

    public PutRequest(JSONObject json) throws JSONException {
        super(json);
    }
}
