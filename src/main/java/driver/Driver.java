/*
 * Copyright by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package driver;

import client.AbstractClient;
import config.AbstractClientConfig;
import config.AsterixClientConfig;
import config.Constants;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class Driver {
    private static Map<String, String> env = System.getenv();
    public static String BIGFUN_HOME = env.get("BIGFUN_HOME") == null ? "/Users/shiva/bigfun_afterCB":env.get("BIGFUN_HOME") ;

    public static String workloadsFolder = BIGFUN_HOME +"/workloads/";
    public static HashMap<String,String> clientToRunningQueries = new HashMap<>();
    public static HashMap<String, Double> totalExecutionTime_perqidvid = new HashMap<>();
    public static HashMap<String, Double> totalElapsedTime_perqidvid = new HashMap<>();
    public static HashMap<String, Double> totalClientResponseTime_perqidvid = new HashMap<>();
    public static volatile int numberOfConcurrentThreads=0;
    public static AtomicInteger count_all_queries = new AtomicInteger(0);//sum for all users
    public static String outputFolder;

    public static void main(String[] args) throws IOException {

        Map<String,String> cmd = processCommandLineConfig(args);

        String confName = cmd.containsKey("conf")?cmd.get("conf"):"bigfun-conf_1node.json";
        String clientConfigFile = BIGFUN_HOME+"/conf/"+confName;



        AbstractClientConfig clientConfig = new AsterixClientConfig(clientConfigFile, cmd);
        clientConfig.parseConfigFile();
        ExecutorService executorService = Executors.newFixedThreadPool(clientConfig.getParams().size());
        Collections.shuffle(clientConfig.getParams());
        IntStream.range(0,clientConfig.getParams().size()).forEach(c ->
                executorService.submit(()-> {
                    Thread.currentThread().setName("USERID-"+clientConfig.getParamValue(Constants.USERID, c)+
                            "="+clientConfig.getParamValue(Constants.CLASS,c));
                    if (!clientConfig.isParamSet(Constants.CLIENT_TYPE, c)) {
                        System.err.println("The Client Type is not set to a valid value in the config file.");
                        return;
                    }
                    if (clientConfig.isParamSet(Constants.NUM_CONCURRENT_READERS, c)) {
                        if (clientConfig.getParamValue(Constants.NUM_CONCURRENT_READERS, c) instanceof  String)
                            numberOfConcurrentThreads =
                                    Integer.parseInt((String)clientConfig.getParamValue(Constants.NUM_CONCURRENT_READERS,
                                            c));
                        else
                            numberOfConcurrentThreads =
                                    (Integer)clientConfig.getParamValue(Constants.NUM_CONCURRENT_READERS, c);
                    }

                    String workload = Constants.WORKLOAD;
                    String numberOfThreads = Integer.toString(numberOfConcurrentThreads);
                    //                String workload = Constants.WORKLOAD;
                    //                String numberOfThreads = Integer.toString(numberOfConcurrentThreads);
                    if (clientConfig.isParamSet(Constants.NUM_CONCURRENT_READERS, c)) {
                        numberOfThreads =
                                Integer.toString(
                                        (Integer) clientConfig.getParams().get(c).get(Constants.NUM_CONCURRENT_READERS));
                    }
                    if (clientConfig.isParamSet(Constants.WORKLOAD, c)) {
                        workload = (String)clientConfig.getParams().get(c).get(Constants.WORKLOAD);
                    }

                    //Create output file
                    outputFolder = BIGFUN_HOME+"/files/output/"+confName.split(".json")[0]+"_"+workload.split(".txt")[0]+"_"+numberOfThreads+"users";
                    File dir = new File(outputFolder+"/avg");
                    if (!dir.exists()){
                        dir.mkdirs();
                    }

                    String clientTypeTag = (String) clientConfig.getParamValue(Constants.CLIENT_TYPE, c);
                    AbstractClient client = null;
                    switch (clientTypeTag) {
                        case Constants.ASTX_RANDOM_CLIENT_TAG:
                            client = clientConfig.readReadOnlyClientConfig(BIGFUN_HOME, c);
                            break;
                        case Constants.ASTX_UPDATE_CLIENT_TAG:
                            client = clientConfig.readUpdateClientConfig(BIGFUN_HOME, c);
                            break;

                        default:
                            System.err.println("Unknown/Invalid client type:\t" + clientTypeTag);
                    }
                    client.bigFunHome = BIGFUN_HOME;
                    try {
                        client.execute();
                        System.out.println("Exited");
                        return;
                        //client.generateReport();
                    } catch (Exception e) {
                        e.printStackTrace();
                        // server.stop();
                    }

                }));
//        try {
//            System.out.println("Killing all");
////             server.stop();
//            executorService.shutdownNow();
//            executorService.awaitTermination(100, TimeUnit.SECONDS);
//
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        finally {
//            printGlobalStats();
//            if (!executorService.isTerminated()) {
//                System.out.println("canceling all pending tasks");
//            }
//            executorService.shutdownNow();
//        }
    }
    private static Map<String, String> processCommandLineConfig(String[] args) {
        Map<String, String> commandLineConfig = new HashMap<>();
        if (args != null) {
            for (String arg: args) {
                if (arg.contains("=")) {
                    commandLineConfig.put(arg.substring(0, arg.indexOf("=")).toLowerCase(),
                            arg.substring(arg.indexOf("=")+1));
                }
            }
        }
        return commandLineConfig;
    }

    private static void printGlobalStats() throws FileNotFoundException {
        PrintWriter globalPW = new PrintWriter(outputFolder+"/globalStats.json");
        globalPW.println("{\"concurrent_users\":"+numberOfConcurrentThreads+",");
        globalPW.println("\"total_queries(sum_for_all_users)\":"+count_all_queries+",");
        int count = 0;
        for(String qidvid:totalElapsedTime_perqidvid.keySet()) {
            if (count > 0)
                globalPW.println(",");
            globalPW.println("\""+qidvid+"\":{");
            globalPW.println("\"total_avg_elapsed(sec)\":" + (double) ((totalElapsedTime_perqidvid.get(qidvid) / 1000 * 1.0) / (numberOfConcurrentThreads * 1.0)) + ",");
            globalPW.println("\"total_avg_execution(sec)\":" + (double) ((totalExecutionTime_perqidvid.get(qidvid) / 1000 * 1.0) / (numberOfConcurrentThreads * 1.0)) + ",");
            globalPW.println("\"total_avg_client_response_time(sec)\":" + (double) ((totalClientResponseTime_perqidvid.get(qidvid) / 1000 * 1.0) / (numberOfConcurrentThreads * 1.0)) + ",");
            globalPW.println("\"throughput(elapsed_time)(query/sec)\":" + (double) (count_all_queries.get()/numberOfConcurrentThreads * 1.0) / (totalElapsedTime_perqidvid.get(qidvid) / 1000 * 1.0) + ",");
            globalPW.println("\"throughput(execution_time)(query/sec)\":" + (double) (count_all_queries.get()/numberOfConcurrentThreads * 1.0) / (totalExecutionTime_perqidvid.get(qidvid) / 1000 * 1.0) + ",");
            globalPW.println("\"throughput(client_time)(query/sec)\":" + (double) (count_all_queries.get()/numberOfConcurrentThreads * 1.0) / (totalClientResponseTime_perqidvid.get(qidvid) / 1000 * 1.0));
            globalPW.println("}\n");
            count++;
        }
        globalPW.println(",\"total_avg_elapsed(sec)\":" + (double) ((totalElapsedTime_perqidvid.values().stream().reduce((double)0,Double::sum) / 1000 * 1.0) / (count_all_queries.get() * 1.0)) + ",");
        globalPW.println("\"total_avg_execution(sec)\":" + (double) ((totalExecutionTime_perqidvid.values().stream().reduce((double)0,Double::sum) / 1000 * 1.0) / (count_all_queries.get() * 1.0)) + ",");
        globalPW.println("\"total_avg_client_response_time(sec)\":" + (double) ((totalClientResponseTime_perqidvid.values().stream().reduce((double)0,Double::sum) / 1000 * 1.0) / (count_all_queries.get() * 1.0)) + ",");
        globalPW.println("\"throughput(elapsed_time)(query/sec)\":" + (double) (count_all_queries.get() * 1.0) / (totalElapsedTime_perqidvid.values().stream().reduce((double)0,Double::sum) / 1000 * 1.0)+ ",");
        globalPW.println("\"throughput(execution_time)(query/sec)\":" + (double) (count_all_queries.get()* 1.0) / (totalExecutionTime_perqidvid.values().stream().reduce((double)0,Double::sum) / 1000 * 1.0) + ",");
        globalPW.println("\"throughput(client_time)(query/sec)\":" + (double) (count_all_queries.get() * 1.0) / (totalClientResponseTime_perqidvid.values().stream().reduce((double)0,Double::sum) / 1000 * 1.0));
        globalPW.println("}");
        globalPW.close();
    }
}
