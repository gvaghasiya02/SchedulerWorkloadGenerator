package asterixReadOnlyClient;

import driver.Driver;
import structure.Pair;
import structure.Query;
import workloadGenerator.ReadOnlyWorkloadGenerator;

import java.util.Random;

public class AsterixConcurrentReadUtil implements Runnable{
        int readerID;
    AsterixConcurrentReadOnlyWorkloadWithTimer AWL;

        public  AsterixConcurrentReadUtil(AsterixConcurrentReadOnlyWorkloadWithTimer AWL) {
            this.AWL = AWL;
            this.readerID = AWL.threadID++;
        }

        @Override
        public void run() {
            long iteration_start = 0l;
            long iteration_end = 0l;
            Random rand = new Random();
            for (int i = 0; i < AWL.iterations; i++) {
                Thread.currentThread().setName("Client-" + this.readerID);
                System.out.println(
                        "\nAsterixDB Client - Read-Only Workload - Starting Iteration " + i + " in " + "thread: " + this.readerID + " (" + Thread.currentThread().getName() + ")");
                iteration_start = System.currentTimeMillis();
                for (Pair qvPair : AWL.clUtilMap.get(this.readerID).qvids) {
                    int qid = qvPair.getQId();
                    int vid = qvPair.getVId();
                    ReadOnlyWorkloadGenerator ro = AWL.rwgMap.get(this.readerID);
                    Query q = ro.nextQuery(qid, vid);
                    if (q == null) {
                        continue; //do not break, if one query is not found
                    }
                    long q_start = -1;
                    if (AWL.execQuery) {
                        try {
                            q_start = System.currentTimeMillis();
                            System.out.println(AWL.clUtilMap.get(this.readerID).executeQuery(qid, vid,
                                    q.aqlPrint(AWL.dvName)));
                        } catch (Exception e) {
                            return;
                        }
                    }
                    long q_end = System.currentTimeMillis();
                    System.out.println(
                            "Iteration " + i + " Thread " + Thread.currentThread().getName() + " Q" + qvPair.getQId() + " version " + qvPair.getVId() + "\t" + (q_end - q_start));
                    int diff = (int) (q_end - q_start);
                    Driver.count_all_queries.addAndGet(1);
                }
                iteration_end = System.currentTimeMillis();

                System.out.println("Total time for iteration " + i + " :\t" + (iteration_end - iteration_start) + " ms in thread: "
                        + this.readerID + " (" + Thread.currentThread().getName() + ")");
            }
        }


        public void shutdown(){
            AWL.clUtilMap.get(this.readerID).terminate();
        }
    }
