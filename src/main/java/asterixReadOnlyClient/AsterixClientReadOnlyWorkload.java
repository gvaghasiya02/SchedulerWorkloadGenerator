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
package asterixReadOnlyClient;

import client.AbstractReadOnlyClient;
import driver.Driver;
import structure.Pair;
import structure.Query;
import workloadGenerator.ReadOnlyWorkloadGenerator;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;

public class AsterixClientReadOnlyWorkload extends AbstractReadOnlyClient {

    String ccUrl;
    String dvName;
    int iterations;
    ReadOnlyWorkloadGenerator rwg;
    public AsterixClientReadOnlyWorkload() {};

    public AsterixClientReadOnlyWorkload(String cc, String dvName, int iter, String qGenConfigFile, String qIxFile,
            String statsFile, int ignore, String qSeqFile, String resDumpFile, long seed, long minUserId,
            long maxUsrId, String server, int thinking_min_ms, int thinking_max_ms ) {
        super();
        this.ccUrl = cc;
        this.dvName = dvName;
        this.iterations = iter;
        setClientUtil(qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile, resDumpFile, server, thinking_min_ms, thinking_max_ms);
        clUtil.init();
        initReadOnlyWorkloadGen(seed,minUserId, maxUsrId);
        execQuery = true;
    }

    @Override
    protected void initReadOnlyWorkloadGen(long seed, long minUserId,long maxUsrId) {
        this.rwg = new ReadOnlyWorkloadGenerator(clUtil.getQIxFile(), clUtil.getQGenConfigFile(), seed, minUserId,maxUsrId);
    }

    @Override
    public void execute() throws Exception {
        long starttime=System.currentTimeMillis();
        System.out.println("started at : " +new Timestamp(starttime));
        long iteration_start;
        long iteration_end;
        int iterationCount = 0;
        System.out.print("[");
        boolean condition = true;
        while(condition){
            Driver.count_all_queries.addAndGet(1);
            if (iterationCount > 0) {
                System.out.println(",");
            }
            System.out.println("\n{ \"Iteration\":" + iterationCount+
                    ",");
            iteration_start = System.currentTimeMillis();
            System.out.println("\"queries\":[");
            int loopCount = 0;
            for (Pair qvPair : clUtil.qvids) {
                if (loopCount > 0) {
                    System.out.print(",");
                }
                int qid = qvPair.getQId();
                int vid = qvPair.getVId();
                Query q = rwg.nextQuery(qid, vid);
                if (q == null) {
                    continue; //do not break, if one query is not found
                }
                if (execQuery) {
                    System.out.println(clUtil.executeQuery(qid, vid, q.aqlPrint(dvName)));
                }
                loopCount++;
            }
            iteration_end = System.currentTimeMillis();
            System.out.print("],\n\"TotalTime " + iterationCount + "\" :" + (iteration_end - iteration_start) + "\n}");
            iterationCount++;
            if (iterations > 0) {
                condition = iterationCount < iterations;
            }
            //            if (System.currentTimeMillis() >= starttime+ (3*60*60*1000)) {//3hrs
            //                System.out.println("Exiting Client-"+ Thread.currentThread().getName());
            //                break;
            //            }
        }
        System.out.print("]");
        System.out.println("Finished at: "+new Timestamp(System.currentTimeMillis()));
        clUtil.terminate();

    }

    @Override
    public void setClientUtil(String qIxFile, String qGenConfigFile, String statsFile, int ignore, String qSeqFile,
            String resultsFile, String server, int thinking_min_ms, int thinking_max_ms) {
        try {
            clUtil = new AsterixReadOnlyClientUtility(ccUrl, qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile,
                    resultsFile, server, thinking_min_ms, thinking_max_ms);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
