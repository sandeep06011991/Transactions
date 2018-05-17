package edu.umass.cs.transaction.testing.app;

import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.Key;
import java.util.HashMap;

public class OperateRequest extends TxAppRequest {



    enum keys {
        OPERATION,OBJECT;
    }
    public enum Operation{
        add(1),

        multiply(2);

        public  int id;

        static HashMap<Integer,Operation> map=new HashMap<>();

        static{
            for(Operation op:Operation.values()){
                map.put(new Integer(op.id),op);
            }
        }

        Operation(int i){this.id = i;}

        public static Operation getOperationFromInt(int id){
            return map.get(id);
        }
    }

    public int object;

    public Operation operation;

    public OperateRequest(String serviceName,int object,Operation operation) {
        super(TxRequestType.OPERATE, serviceName);
        this.object = object;
        this.operation = operation;
    }

    public OperateRequest(JSONObject jsonObject) throws JSONException{
        super(jsonObject);
        this.object = jsonObject.getInt(keys.OBJECT.toString());
        this.operation = Operation.getOperationFromInt(jsonObject.getInt(keys.OPERATION.toString()));

    }


    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject = super.toJSONObject();
        jsonObject.put(keys.OPERATION.toString(),operation.id);
        jsonObject.put(keys.OBJECT.toString(),object);
        return jsonObject;
    }
}
