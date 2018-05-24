package testing;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.transaction.BaseTxnClient;
import edu.umass.cs.transaction.exceptions.ResponseCode;
import testing.app.CalculatorTX;
import testing.app.CalculatorTXClient;
import testing.app.packets.OperateRequest;
import testing.app.packets.ResultRequest;
import edu.umass.cs.transaction.txpackets.TXPacket;
import edu.umass.cs.transaction.txpackets.TxClientRequest;
import edu.umass.cs.transaction.txpackets.TxClientResult;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Sandeep
 *
 */
public class Simulator extends CalculatorTXClient {

    /*Building the Simulator
    0. Test every kind of Tx Response Code
    1. Locally run bursts of requests - No Failure, No Contention, Try to pass as many as possible Measure throughput
    Run on Cluster -> Kind of a throughput experiment. No Failures.
    Objective is to have a reasonable transaction throughput
    2. Do the same with some contention and check if quorum details are being recieved
    3. Start Failining machines and do the same

    * */
    static int load =20;

//    static int noBursts = 10;
    static int noBursts = 10 * 120;
//    1 day is 10 sec
//  sets of transactions sent
//  1 day is 30 sec and running for 180 days

    static int maxGroups = 300;
            //210;
//max groups created
    static int recieved = 0;
//to wait till groups are complete

    static int txSize = 3;
//  TxSize is the size of the load

    static int stabilizeTime = 30;
//    Wait 2 minute after starting everythin
//    Atleast this much time between failures

    static BaseTxnClient client;

    static boolean created = false;

    static HashMap<ResponseCode,Integer> results = new HashMap<>();

   static  String cmdAppend1 = "./bin/gpServer.sh";
   static String cmdAppend2= "-DgigapaxosConfig=src/testing/gigapaxos.properties";
    private static final Logger log = Logger
            .getLogger(Simulator.class.getName());
    HashMap<String,Boolean> isAlive;
    static  void createSomething(){
            recieved = 0;
            try {
                client = new CalculatorTXClient();
                Set<InetSocketAddress> quorum = new HashSet<>();
                for (String key : actives.keySet()) {
                    quorum.add(actives.get(key));
                }
                Object something = new Object();
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



    TxClientRequest getRandomlyCreatedTxRequest(){
        Random random = new Random();
        ArrayList<ClientRequest> clientRequests=new ArrayList<>();
        Set<Integer> prev = new HashSet<>();
        int t;
        for(int i=0;i<txSize;i++){
            t = random.nextInt(maxGroups)+1;
            while(prev.contains(t)){
                t = random.nextInt(maxGroups)+1;
            }
            String name = "name"+t;
            clientRequests.add(new OperateRequest(name,10, OperateRequest.Operation.add));
            prev.add(t);
        }

        TxClientRequest txClientRequest = new TxClientRequest(clientRequests);
        return txClientRequest;
    }

    ResponseCode checkQuorumStatus(Set<String> actives,ResponseCode rpe) {
        if (rpe == ResponseCode.LOCK_FAILURE) {
            synchronized (isAlive) {
                int i = 0;
                for (String active : actives) {
                    if (isAlive.get(active)) i++;
                }
                if (i <= actives.size() / 2) {
                    return ResponseCode.TARGET;
                }
            }
        }
        return rpe;
    }

    synchronized static void updateRPE(ResponseCode rpe){
           if(!results.containsKey(rpe)){
            results.put(rpe,1);
        }else{
            results.put(rpe,results.get(rpe)+1);
        }
    }

    static Set<Long> requestIds = new HashSet<>();

    public float startload(int load,int totalRequests){
        createSomething();
        RateLimiter rateLimiter = new RateLimiter(load);
        System.out.println("Start load");
        Timer timer = new Timer();
        requestIds = new HashSet<>();
        for(int i=0;i<=totalRequests;i++){
            rateLimiter.record();
            TxClientRequest txClientRequest = getRandomlyCreatedTxRequest();

//            System.out.println("Request ID"+txClientRequest.getRequestID());
            try {
                synchronized (requestIds){
                    requestIds.add(txClientRequest.getRequestID());
                }
//                writer.write("Send rqID:"+txClientRequest.getRequestID()+"\n");
                sendRequestAnycast(txClientRequest,new TimeoutRequestCallback() {
                    @Override
                    public long getTimeout() {
                        return 50*60*2000;
                    }

                    @Override
                    public void handleResponse(Request response) {

                        if (response instanceof TxClientResult) {
                            TxClientResult t = (TxClientResult) response;
//                            try {
////                                writer.write("Recieved rqID" + t.getRequestID() + "\n");
//                            }catch (IOException e){
//
//                            }
                            synchronized (requestIds){
                                requestIds.remove(t.getRequestID());
                            }
//                            if(.success)checkQuorumStatus(t.getActivesPrevious());
                            updateRPE(checkQuorumStatus(t.getActivesPrevious(),t.getRpe()));
//                            if(t.getRpe()!= ResponseCode.COMMITTED)checkQuorumStatus(t.getActivesPrevious());
                        }
                    }
                });
                if(i%10000==0) {
                    synchronized (requestIds) {
                        writer.write("requests sent"+i+": Outstanding "+requestIds.size());
                        writer.newLine();
                    }
                }
            }catch (Exception ex){
                try {
                    writer.write(ex.getMessage());
                }catch(Exception e){

                }
                }


        }
        System.out.print("Begin Wait");
        try{
            TimeUnit.SECONDS.sleep(60);
            synchronized (requestIds){
                writer.write("Did not recieve\n");
                Object[] a=requestIds.toArray();
                for(int i=0;i<requestIds.size();i++){
                    writer.write(a[i]+"\n");
                }
            }
        }catch(Exception ex){

        }

         return 0;
    }

    /**
     * @throws IOException
     */

    public Simulator(HashMap<String,Boolean> isAlive) throws IOException {
        super();
        this.isAlive =isAlive;
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

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> set = CalculatorTX.staticGetRequestTypes();
        set.add(TXPacket.PacketType.TX_CLIENT_RESPONSE);
        return set;
    }



    /**
     * This simple client creates a bunch of names and sends a bunch of requests
     * to each of them. Refer to the parent class
     * {@link ReconfigurableAppClientAsync} for other utility methods available
     * to this method or to know how to write your own client.
     *w
     * @param args
     * @throws IOException
     */
    static Random rand = new Random();

    static long getRandom(int exp){
        return Math.round(Math.log(1- rand.nextDouble())*(exp)*-1);
    }

    synchronized static void runCommand(String[] cmd){
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                System.out.println(line);
            }
            input.close();
        } catch(IOException ex){
            ex.printStackTrace();
            System.out.println(cmd + "Could not execute");
        }
    }

    static class SimulateFailure extends Thread{

        final String activeID;

        final int ettf;

        HashMap<String,Boolean> isAlive;



        String[] getStartCommand(){

            return new String[]{cmdAppend1,cmdAppend2,"start",activeID};

        }

        String[] getStopCommand(){
            return new String[]{cmdAppend1,cmdAppend2,"stop",activeID};
        }

        SimulateFailure(String activeID,int ettf,HashMap<String,Boolean> isAlive ) {
            this.activeID = activeID;
            this.ettf = ettf;
            this.isAlive = isAlive;
        }

        public void run() {
            try{
            while(!isInterrupted()) {
                    long fail = getRandom(ettf);
                    Thread.sleep(fail*1000);
                    synchronized (isAlive){
                        isAlive.put(activeID,new Boolean(false));
                    }
                    runCommand(getStopCommand());
                    System.out.println("System " + activeID + " Killed");
                    Thread.sleep(30000);// MTTR
                    runCommand(getStartCommand());
                    Thread.sleep(stabilizeTime * 1000);
                    synchronized (isAlive) {
                        isAlive.put(activeID, new Boolean(true));
                    }

            }
            }catch(InterruptedException ex){
                System.out.println("Interrupted");
            }
        }
    }

    protected static String DEFAULT_RECONFIGURATOR_PREFIX = "active.";

    static Map<String, InetSocketAddress> actives = new HashMap<String, InetSocketAddress>();


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
    static BufferedWriter writer;

    public static void main(String args[]) throws  IOException,InterruptedException{
//      Start everything after, starting all replicas
        HashMap<String,Boolean> isAlive = new HashMap<>();
        HashMap<String,SimulateFailure> threads = new HashMap<>();
//        createSomething();
        writer = new BufferedWriter(new FileWriter("results"));
        writer.write("This is a test\n");
        writer.flush();
//        int ettfs[] = {15 days, 30 days, 3 months, 6 months, 1 year}
// ;
//        int ettfs[] = {150,300,900,1800,0};
        int ettfs[] = {150};
//        if etffs = 0 no failure
//                int[] ettfs={150};
        /*  TIME COMPRESSION
        * If a day is represented by 30 seconds,
        * MTTR -> 1 day = 30 sec
        * e_MTTF -> 365*30
        * Total run time is 6 months
        * or req/s * 180 * 30 total requests
        * Actual time per experiment : 90 min
        *
        *
        *
        * */
        for(int ettf:ettfs){
            //kill every machine
        recieved = 0;

        runCommand(new String[]{cmdAppend1 ,cmdAppend2 ,"stop","all"});
		TimeUnit.SECONDS.sleep(10);

		runCommand(new String[]{"make" ,"clear_everything"});
		TimeUnit.SECONDS.sleep(10);


            runCommand(new String[]{cmdAppend1,cmdAppend2,"start","all"});
		TimeUnit.SECONDS.sleep(120);
            
            results = new HashMap<>();
            isAlive = new HashMap<>();
            createSomething();
            // Start every machine: Stabilize
            for(String k:actives.keySet()){
                if(ettf!=0){
                 SimulateFailure s=new SimulateFailure(k,ettf,isAlive);s.start();
                threads.put(k,s);
                    }
                isAlive.put(k,true);
            }
//  Total requests is no. of days * (equivalent secodns in a day)
            float failurePercentage =new Simulator(isAlive).startload(load,load*noBursts);
            writer.write("ETTF:"+ettf+" Failure:"+failurePercentage+"\n");
            writer.write("Total Load :"+load*noBursts+"\n");
            for(ResponseCode rpc:results.keySet()){
                writer.write("STATS for"+rpc+"is "+results.get(rpc)+"\n");
            }
            writer.flush();

            log.log(Level.INFO,"ETTF:"+ettf+" Failure:"+failurePercentage+"\n");
            log.log(Level.INFO,"Total Load :"+load*noBursts+"\n");
            for(ResponseCode rpc:results.keySet()){
                log.log(Level.INFO,"STATS for"+rpc+"is "+results.get(rpc)+"\n");
            }


            for(String th:threads.keySet()){
                SimulateFailure f=threads.get(th);
                if(f.isAlive()){
                    f.interrupt();
                    f.join();
                }
            }
        }
        writer.write("The end");

        writer.close();
    }


}
