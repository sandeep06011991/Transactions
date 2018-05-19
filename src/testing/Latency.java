package testing;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import testing.app.packets.GetRequest;
import testing.app.packets.OperateRequest;
import edu.umass.cs.transaction.txpackets.TxClientRequest;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import testing.app.CalculatorTX;
import testing.app.packets.ResultRequest;
import edu.umass.cs.transaction.txpackets.TXPacket;
import edu.umass.cs.transaction.txpackets.TxClientResult;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Latency extends ReconfigurableAppClientAsync<Request>{

    protected static String DEFAULT_RECONFIGURATOR_PREFIX = "active.";

    static Map<String, InetSocketAddress> actives = new HashMap<String, InetSocketAddress>();

    static Latency client;

    static int maxGroups = 10;

    static int recieved = 0;

    static{
        Properties config = PaxosConfig.getAsProperties();

        Set<String> keys = config.stringPropertyNames();
        for (String key : keys) {
            if (key.trim().startsWith(DEFAULT_RECONFIGURATOR_PREFIX)) {
                actives .put(key.replaceFirst(DEFAULT_RECONFIGURATOR_PREFIX, ""),
                        Util.getInetSocketAddressFromString(config
                                .getProperty(key)));
            }
        }

    }

    static  void createSomething(){
        try {
            client = new Latency();
            Set<InetSocketAddress> quorum = new HashSet<>();
            for (String key : actives.keySet()) {
                quorum.add(actives.get(key));
            }
            Object something = new Object();
            client.sendRequest(new CreateServiceName("Service_name_txn", "0", quorum));
            for (int i = 1; i <= maxGroups; i++) {
                client.sendRequest(new CreateServiceName("name" + i, Integer.toString(i)), new RequestCallback() {
                    @Override
                    public void handleResponse(Request response) {
                        synchronized(something) {
                            recieved++;
                            if (recieved == maxGroups) notify();
                        }
                    }
                });
            }
            synchronized(something){
                something.wait(60000);
            }
            System.out.println("created a total of "+recieved+"groups");

        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("Unable to create");
        }
    }


    Latency() throws  IOException{

    }

    @Override
    public Request getRequest(String stringified) {

        try {
/*      DEBUG tip: If requests are not being recieved debug here */
            JSONObject jsonObject=new JSONObject(stringified);
            if(jsonObject.getInt("type")==4){
                return new ResultRequest(jsonObject);
            }
            if(jsonObject.getInt("type")==262){
                return new TxClientResult(jsonObject);
            }
        } catch ( JSONException e) {
            // do nothing by designSys
            e.printStackTrace();
        }
        return null;
    }

    private static final Logger log = Logger
            .getLogger(Latency.class.getName());


    public static void testTxnLatency() throws IOException{
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for(int i=0;i<300;i++){
            Random random = new Random();
            int t = random.nextInt(maxGroups)+1;
            OperateRequest operateRequest =
                    new OperateRequest("name"+t,10, OperateRequest.Operation.add);
            ArrayList<ClientRequest> re = new ArrayList<>();
            re.add(operateRequest);
            long startTime = Calendar.getInstance().getTimeInMillis();
            TxClientRequest txClientRequest = new TxClientRequest(re);
            log.log(Level.INFO,"getRequest ID "+txClientRequest.getRequestID());
            Request response = client.sendRequestAnycast(txClientRequest,100000);
            if(response!=null){

                long endTime = Calendar.getInstance().getTimeInMillis();
                log.log(Level.INFO,"TxLatency "+ Long.toString(endTime - startTime));
                log.log(Level.INFO,response.toString());
                if(i>20)stats.addValue(new Long(endTime-startTime));
            }

        }
        log.log(Level.INFO, "Final Transaction statistics");
        log.log(Level.INFO,"GM "+stats.getGeometricMean());
        log.log(Level.INFO, "Variance"+stats.getVariance());
        log.log(Level.INFO, "SD"+stats.getStandardDeviation());
        log.log(Level.INFO,"Size"+stats.getN());

    }

    public static void testPaxosLatency() throws IOException{
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for(int i=0;i<300;i++){
            Random random = new Random();
            int t = random.nextInt(maxGroups)+1;
            GetRequest getRequest =
                   new GetRequest("name"+t);
            long startTime = Calendar.getInstance().getTimeInMillis();
            log.log(Level.INFO,"RequestID "+getRequest.getRequestID());
            Request response = client.sendRequest(getRequest,5000);

            if(response!=null){
//                System.out.println(response.toString());
                long endTime = Calendar.getInstance().getTimeInMillis();
//                log.log(Level.INFO,"Latency "+ Long.toString(endTime - startTime));
//                log.log(Level.INFO,response.toString());
              if(i>20)stats.addValue(new Long(endTime-startTime));
            }


        }
        log.log(Level.INFO, "Final Paxos statistics");
        log.log(Level.INFO,"GM "+stats.getGeometricMean());
        log.log(Level.INFO, "Variance"+stats.getVariance());
        log.log(Level.INFO, "SD"+stats.getStandardDeviation());
        log.log(Level.INFO,"Size"+stats.getN());
        //        stats.addValue();
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> set = CalculatorTX.staticGetRequestTypes();
        set.add(TXPacket.PacketType.TX_CLIENT_RESPONSE);
        return set;
    }
    public static void  main(String args[]) throws IOException{
        createSomething();
        testPaxosLatency();
        testTxnLatency();
        System.out.println("I am all done !!!");
    }
}
