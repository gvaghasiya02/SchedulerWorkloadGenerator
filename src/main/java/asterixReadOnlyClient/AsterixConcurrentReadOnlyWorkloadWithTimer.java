package asterixReadOnlyClient;

import client.AbstractReadOnlyClientUtility;
import workloadGenerator.ReadOnlyWorkloadGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class AsterixConcurrentReadOnlyWorkloadWithTimer extends AsterixClientReadOnlyWorkload {
    Timer timer = new Timer();
    public static volatile int threadID;

    public static Map<Integer, ReadOnlyWorkloadGenerator> rwgMap;

    public static Map<Integer, AbstractReadOnlyClientUtility> clUtilMap;

    public int numReaders;

    public List<Long> readerSeeds;
    public ScheduledExecutorService executors;

    public AsterixConcurrentReadOnlyWorkloadWithTimer(String cc, String dvName, int iter, String qGenConfigFile, String
            qIxFile, String statsFile, int ignore, String qSeqFile, String resDumpFile, long seed, long minUserId,long maxUsrId,
            int numReaders, String server, int thinking_min_ms, int thinking_max_ms) {
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
        threadID=0;
        this.executors= Executors.newScheduledThreadPool(numReaders);
    }

    public void execute(){
        timer.schedule(new ShutDownTask(),3*60*60*1000);
        for (int i=0; i<numReaders;i++) {
             executors.submit(new AsterixConcurrentReadUtil(this));
        }

    }

    class ShutDownTask extends TimerTask {
        public void run() {
            System.out.println("Shutting Down...");
            executors.shutdownNow();
            timer.cancel(); //Terminate the timer thread
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
    public void setDumpResults(boolean b) {
        IntStream.range(0, numReaders).forEach(x -> {

        });
    }

    @Override
    public void generateReport() {
        IntStream.range(0, numReaders).forEach(x -> clUtilMap.get(x).generateReport());
    }
}
