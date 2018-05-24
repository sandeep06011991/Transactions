package testing;

import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import testing.app.*;
import edu.umass.cs.transaction.txpackets.TXPacket;
import edu.umass.cs.transaction.txpackets.TxClientRequest;
import edu.umass.cs.transaction.txpackets.TxClientResult;
import org.json.JSONException;
import org.json.JSONObject;
import testing.app.packets.GetRequest;
import testing.app.packets.OperateRequest;
import testing.app.packets.ResultRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Sandeep
 *
 */
public class BasicTest extends CalculatorTXClient {

    static BasicTest client;

    static boolean created = false;

    long startTime;

    static void createSomething(){
        if(!created){
            try {
                client = new BasicTest();
                client.sendRequest(new CreateServiceName("name3", "0"));
                client.sendRequest(new CreateServiceName("name4","1"));
                TimeUnit.SECONDS.sleep(5);
            }catch(Exception e){
                e.printStackTrace();
                throw new RuntimeException("Unable to create");
            }
        }
        created = true;
    }

    void testGetRequest(int finalValue) throws IOException{
        sendRequest(new GetRequest("name3"), new RequestCallback() {
            @Override
            public void handleResponse(Request response) {
                ResultRequest rr= (ResultRequest)response;
                System.out.println("Result :"+rr.getResult());
                assert  rr.getResult() == finalValue;
                System.out.println("Get Test complete");

            }
        });
    }


    void testMultiLineTxn(){
        createSomething();
        InetSocketAddress entryServer=new InetSocketAddress("127.0.0.1",2100);
        ArrayList<ClientRequest> requests = new ArrayList<>();
        requests.add(new OperateRequest("name0", 10, OperateRequest.Operation.add));
        requests.add(new OperateRequest("name0", 20, OperateRequest.Operation.multiply));
        requests.add(new OperateRequest("name0", 10, OperateRequest.Operation.add));


        try{
            TxClientRequest txClientRequest = new TxClientRequest(requests);
            RequestFuture rd= sendRequest(txClientRequest,entryServer, new RequestCallback() {
                @Override
                public void handleResponse(Request response) {
                    if(response instanceof TxClientResult){
                        try {
                            testGetRequest(210);
                            System.out.println("Transaction test complete");
                        }catch(Exception e){
                            throw new RuntimeException("Transaction failed");
                        }
                    }
                }
            });

        }catch (Exception e){
            e.printStackTrace();
        }


    }

    void testAborting() throws IOException{
        createSomething();

        InetSocketAddress entryServer=new InetSocketAddress("127.0.0.1",2100);
        ArrayList<ClientRequest> requests = new ArrayList<>();
        requests.add(new OperateRequest("name0", 10, OperateRequest.Operation.add));
        requests.add(new OperateRequest("name1", 20, OperateRequest.Operation.add));
        TxClientRequest txClientRequest = new TxClientRequest(requests);

        ArrayList<ClientRequest> requests1 = new ArrayList<>();
        requests1.add(new GetRequest("name0"));
        TxClientRequest txClientRequest1 = new TxClientRequest(requests1);

        RequestFuture rd= sendRequest(txClientRequest,entryServer, new RequestCallback() {
            @Override
            public void handleResponse(Request response) {
                if(response instanceof TxClientResult){
                    try {
                        System.out.println("Transaction status "+((TxClientResult) response).getRpe());
                        testGetRequest(10);
                        System.out.println("Transaction test complete");
                    }catch(Exception e){
                        throw new RuntimeException("Transaction failed");
                    }
                }
            }

        });

        sendRequest(txClientRequest1, entryServer, new TimeoutRequestCallback() {
            @Override
            public long getTimeout() {
                return 600*1000;
            }


            @Override
            public void handleResponse(Request response) {
                assert response instanceof TxClientResult;
                TxClientResult txClientResult=(TxClientResult)response;
            }
        });
    }

    static HashMap<Long,Long> init = new HashMap<>();

    void testBasicCommit() throws IOException{
        createSomething();
        for(int i=0;i<2;i++) {
            try {
                System.out.println("Attempt Request "+i);
                ArrayList<ClientRequest> requests = new ArrayList<>();
                requests.add(new OperateRequest("name3", 10, OperateRequest.Operation.add));
                requests.add(new OperateRequest("name4", 20, OperateRequest.Operation.add));
                TxClientRequest txClientRequest = new TxClientRequest(requests);
                startTime = new Date().getTime();
                init.put(new Long(txClientRequest.getRequestID()),new Long(new Date().getTime()));
//                InetSocketAddress ient= PaxosConfig.getActives().get("arun_a0");
                sendTransaction(txClientRequest, new RequestCallback() {
                    @Override
                    public void handleResponse(Request response) {
                        TxClientResult t = (TxClientResult) response;
                        long recvTime = new Date().getTime();
                        long startTime = init.get(new Long(t.getRequestID()));
                        long timeTaken = (recvTime - startTime);
                        System.out.println("Req Status:" + t.getRpe() + " with " + timeTaken);

                    }
                });
                TimeUnit.SECONDS.sleep(1);
//
//                ResultRequest rr= (ResultRequest) sendRequest(new GetRequest("name3"));
//                System.out.println("Result "+i+":"+rr.getResult());


            }catch(Exception e){
                e.printStackTrace();
                System.out.println("Some Exception");
            }

        }


    }

    /**
     * @throws IOException
     */
    public BasicTest() throws IOException {
        super();
    }




    public static void main(String args[]) throws  IOException{
        createSomething();
//        client.testGetRequest(10);
//        client = new TxnClient();
        client.testBasicCommit();
//          client.testMultiLineTxn();
//          client.testAborting();
    }


}
