package testing.app;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.transaction.BaseTxnClient;
import org.json.JSONException;
import org.json.JSONObject;
import testing.app.packets.ResultRequest;

import java.io.IOException;
import java.util.Set;

public class CalculatorTXClient extends BaseTxnClient {

    public CalculatorTXClient() throws IOException{
        super();
    }


    @Override
    public Request getRequest(String stringified) {
        Request r = super.getRequest(stringified);
        if(r !=null) return r;
        try {
/*      DEBUG tip: If requests are not being recieved debug here */
            JSONObject jsonObject=new JSONObject(stringified);
            if(jsonObject.getInt("type")==4){
                return new ResultRequest(jsonObject);
            }
        } catch ( JSONException e) {
            // Assume that all requests are
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> set = super.getRequestTypes();
        set.addAll(CalculatorTX.staticGetRequestTypes());
        return set;
    }


}
