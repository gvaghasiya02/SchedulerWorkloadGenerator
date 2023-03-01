package asterixReadOnlyClient;

import client.AbstractReadOnlyClientUtility;
import driver.Driver;
import structure.Pair;
import structure.Query;
import workloadGenerator.ReadOnlyWorkloadGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class AsterixConcurrentReadOnlyWorkload extends AsterixClientReadOnlyWorkload {

    private ExecutorService executorService;


    private static Map<Integer, ReadOnlyWorkloadGenerator> rwgMap;

    private static Map<Integer, AbstractReadOnlyClientUtility> clUtilMap;

    private int numReaders;

    private List<Long> readerSeeds;

    private String class_;

    public AsterixConcurrentReadOnlyWorkload(String cc, String dvName, int iter, String qGenConfigFile, String
            qIxFile, String statsFile, int ignore, String qSeqFile, String resDumpFile, long seed, long minUserId,long maxUsrId,
            int numReaders, String server, int thinking_min_ms, int thinking_max_ms, String class_) {
        super();
        this.ccUrl = cc;
        this.dvName = dvName;
        this.iterations = iter;
        this.numReaders = numReaders;
        this.clUtilMap = new ConcurrentHashMap<>();
        this.rwgMap = new ConcurrentHashMap<>();
        initReaderSeeds(seed);
        setClientUtil(qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile, resDumpFile, server, thinking_min_ms, thinking_max_ms);
        initReadOnlyWorkloadGen(seed, minUserId,maxUsrId);
        execQuery = true;
        this.class_ = class_;
        //super(cc, dvName, iter, qGenConfigFile, qIxFile, statsFile, ignore, qSeqFile, resDumpFile, seed, maxUsrId);
    }

    private void shutDownExecutors() {
        try {
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS); // TODO: Is this necessary?

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            if (!executorService.isTerminated()) {
                System.out.println("canceling all pending tasks");
            }

            executorService.shutdownNow();
        }
    }

    private void initReaderSeeds(long seed) {
        readerSeeds = new ArrayList<>();
        Random rand = new Random(seed);
        IntStream.range(0, numReaders).forEach(x -> {
            readerSeeds.add(rand.nextLong());
        });
    }

    @Override
    public void initReadOnlyWorkloadGen(long seed, long minUserId,long maxUsrId) {
        IntStream.range(0, numReaders).forEach(x -> {
            rwgMap.put(x, new ReadOnlyWorkloadGenerator(clUtilMap.get(x).getQIxFile(), clUtilMap.get(x)
                    .getQGenConfigFile(), readerSeeds.get(x), minUserId,maxUsrId));

        });
    }

    @Override
    public void setClientUtil(String qIxFile, String qGenConfigFile, String statsFile, int ignore,
            String qSeqFile, String resultsFile, String server, int thinking_min_ms, int thinking_max_ms) {
        //TODO: Append the result and other stat files with threadIds.
        IntStream.range(0, numReaders).forEach(x -> {
            String statsF = statsFile.contains(".json")?statsFile.split(".json")[0]+x+".json":statsFile+x+".json";
            String resF = resultsFile.contains(".txt")?resultsFile.split(".txt")[0]+x+".txt":resultsFile+x+".txt";
            try {
                clUtilMap.put(x, new AsterixReadOnlyClientUtility(ccUrl, qIxFile, qGenConfigFile, statsF, ignore, qSeqFile,
                        resF, server, thinking_min_ms, thinking_max_ms));
            } catch (IOException e) {
                e.printStackTrace();
            }
            clUtilMap.get(x).init();
        });
    }

    @Override
    public void execute() {
        long starttime = System.currentTimeMillis();
        System.out.println("started at : " +starttime);
        for (int i = 0; i < numReaders; i++ ){
            int readerId= i;
            new Thread("" + i){
                public void run(){
                        long iteration_start = 0l;
                        long iteration_end = 0l;
                        Random rand = new Random();
                        for (int i = 0; i < iterations; i++) {
                            Thread.currentThread().setName("Client-" + readerId);
                            System.out.println("\nAsterixDB Client - Read-Only Workload - Starting Iteration " + i + " in "
                                    + "thread: " + readerId + " (" + Thread.currentThread().getName() + "class: "+class_+")");
                            iteration_start = System.currentTimeMillis();
                            for (Pair qvPair : clUtilMap.get(readerId).qvids) {
                                int qid = qvPair.getQId();
                                int vid = qvPair.getVId();
                                ReadOnlyWorkloadGenerator ro = rwgMap.get(readerId);
                                Query q = ro.nextQuery(qid, vid);
                                if (q == null) {
                                    continue; //do not break, if one query is not found
                                }
                                long q_start = -1;

                                if (execQuery) {
                                    try {
                                        int thinking_min_ms = ((AsterixReadOnlyClientUtility) clUtilMap.get(readerId)).getThinking_min_ms();
                                        int thinking_max_ms = ((AsterixReadOnlyClientUtility) clUtilMap.get(readerId)).getThinking_max_ms();
                                        if (thinking_max_ms > 0 && thinking_min_ms > 0) {
                                            int sleepTime = rand.nextInt(thinking_max_ms - thinking_min_ms) + thinking_min_ms;
                                            Thread.sleep(sleepTime);
                                        }
                                        q_start = System.currentTimeMillis();
                                        System.out.println(clUtilMap.get(readerId).executeQuery(qid, vid, q.aqlPrint(dvName)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                                long q_end = System.currentTimeMillis();
                                System.out.println(
                                        "Iteration " + i + " Thread " + Thread.currentThread().getName() + " Q" + qvPair
                                                .getQId() + " version " + qvPair.getVId() + "\t" + (q_end - q_start));
                                int diff = (int) (q_end - q_start);
                                Driver.count_all_queries.addAndGet(1);
                            }
                            iteration_end = System.currentTimeMillis();

                            System.out.println("Total time for iteration " + i + " :\t" + (iteration_end - iteration_start)
                                    + " ms in thread: " + readerId + " (" + Thread.currentThread().getName() + ")");
//                            if (System.currentTimeMillis() >= starttime+ (3*60*60*1000)) {
//                                System.out.println("Exiting Client-"+ Thread.currentThread().getName());
//                                break;
//                            }
                        }
                        clUtilMap.get(readerId).terminate();
                    }
            }.start();
        }

    }

    @Override
    public void setDumpResults(boolean b) {
        IntStream.range(0, numReaders).forEach(x -> {

        });
    }

    @Override
    public void generateReport() {
        IntStream.range(0, numReaders).forEach(x -> clUtilMap.get(x).generateReport());
    }
    public void setxyMap(){

    }
}
