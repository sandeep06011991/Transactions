package edu.umass.cs.transaction.testing;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationConfig;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationMain;
import edu.umass.cs.transaction.TxnClient;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class SerializabilityTest extends DefaultTest {
/* FIXME: Target test 1: How many requests are needed to get requests to
* start failing */
    private static HashMap<String,InetSocketAddress> activesMap;
    private static HashMap<String,InetSocketAddress> reconfMap;
    static{

        activesMap=new HashMap<>();
        activesMap.put("arun_a0",new InetSocketAddress(2000));
        activesMap.put("arun_a1",new InetSocketAddress(2001));
        activesMap.put("arun_a2",new InetSocketAddress(2002));
        reconfMap=new HashMap<>();
        reconfMap.put("arun_r0",new InetSocketAddress(2700));
        reconfMap.put("arun_r1",new InetSocketAddress(2701));
        reconfMap.put("arun_r2",new InetSocketAddress(2702));

    }

        private static Set<ReconfigurableNode<?>> startActives(String[] args)
            throws IOException {
        Set<ReconfigurableNode<?>> createdNodes = new HashSet<ReconfigurableNode<?>>();
        System.out.print("Creating active(s) [ ");
        for (int i = 0; i < 1; i++) {
            createdNodes
                    .add(new ReconfigurableNode.DefaultReconfigurableNode(
                            "arun_a" + i,
                            // must use a different nodeConfig for each
                             new DefaultNodeConfig<String>(
                                    activesMap,reconfMap), args,
                            false));
            }
        return createdNodes;
    }

    @Test
    public void testSomething() throws IOException,InterruptedException{
//        FIXME: build a test framework and think of some tests
        String args[]={"edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp"};
        startActives(args);
        TxnClient.function();
    }

    public static void main(String[] args) throws IOException,
            InterruptedException {
        new SerializabilityTest().testSomething();
//        Result result = JUnitCore.runClasses(SerializabilityTest.class);
//        for (Failure failure : result.getFailures()) {
//            failure.getException().printStackTrace();
//        }
    }
}
