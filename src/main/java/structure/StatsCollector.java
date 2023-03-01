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
package structure;

import driver.Driver;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * @author pouria
 */
public class StatsCollector {

    int ignore; //number of beginning iteration to ignore in average report
    HashMap<Pair, QueryStat> qvToStat;
    String statsFile; //the file to eventually dump final results into
    int counter; //for tracing purpose only
    HashMap<Pair,Double> qvToAvgtimeX;

    public StatsCollector(String statsFile, int ignore) {
        this.qvToStat = new HashMap<>();
        this.qvToAvgtimeX = new HashMap<>();
        this.statsFile = statsFile;
        this.ignore = ignore;
        this.counter = 0;
    }
    public void setqvToAvgTimeX(){
        double i=0;
        Set<Pair> keys = qvToStat.keySet();
        Pair[] qvs = keys.toArray(new Pair[keys.size()]);
        Arrays.sort(qvs);
       for(Pair qv: qvs){
           qvToAvgtimeX.put(qv,i);
           i++;
       }
    }

    public void updateStat(int qid, int vid, double clientTime, double elapsedTime, double executionTime) {
        Pair p = new Pair(qid, vid);
        if (!qvToStat.containsKey(p)) {
            qvToStat.put(p, new QueryStat(qid));
        }
        qvToStat.get(p).addToClientTimes(clientTime);
        qvToStat.get(p).addToElapsedTimes(elapsedTime);
        qvToStat.get(p).addToExecutionTimes(executionTime);
    }

    public void report() {
        generateReport(0);
    }

    private void generateReport(int startRound) {
        try {
            setqvToAvgTimeX();
            PrintWriter pw =
                    new PrintWriter(new File(Driver.outputFolder+"/user_"+statsFile));
            PrintWriter avgpw = new PrintWriter(new File(Driver.outputFolder+"/avg/avg_"+statsFile));
            if (startRound != 0) {
                ignore = -1;
            }
            StringBuffer tsb = new StringBuffer();
            StringBuffer avgsb = new StringBuffer();
            Set<Pair> keys = qvToStat.keySet();
            Pair[] qvs = keys.toArray(new Pair[keys.size()]);
            Arrays.sort(qvs);
            avgsb.append("[");
            tsb.append("[");
            int resCount = 0;
            for (Pair p : qvs) {
                if (resCount > 0){
                    tsb.append(",");
                    avgsb.append(",");
                }
                    QueryStat qs = qvToStat.get(p);
                    tsb.append("{\n \"qidvid\":\""+ p.toString()).append("\", \n")
                            .append("\"iterations\":[").append(qs.getIterations(qs.clientTimes)).append("],\n")
                            .append("\"Client Side Response Times\":[").append(qs.getTimesForChart(qs.clientTimes)).append("],\n")
                            .append("\"Elapsed Times: \" :[").append(qs.getTimesForChart(qs.elapsedTimes)).append("],\n")
                            .append("\"Execution Times: \" :[").append(qs.getTimesForChart(qs.executionTimes)).append("]\n } \n");
                    double clientSideResponseTimeAvg = qs.getAverageRT(ignore, qs.clientTimes);
                    double clientSideResponseTimeSTD = qs.getSTD(ignore,qs.clientTimes);
                    double elapsedTimeAvg = qs.getAverageRT(ignore, qs.elapsedTimes);
                    double elapsedTimeSTD = qs.getSTD(ignore,qs.elapsedTimes);
                    double executionTimeAvg = qs.getAverageRT(ignore, qs.executionTimes);
                    double executionTimeSTD = qs.getSTD(ignore,qs.executionTimes);
                    avgsb.append("\n{\n \"qidvid\":\""+ p.toString()).append("\", \n")
                            .append("\"clientSideResponseTimeAvg\":").append(clientSideResponseTimeAvg).append(",\n")
                            .append("\"clientSideResponseTimeSTD\":" ).append(clientSideResponseTimeSTD).append(",\n")
                    .append("\"elapsedTimeAvg\":").append(elapsedTimeAvg).append(",\n")
                        .append("\"elapsedTimeSTD\":" ).append(elapsedTimeSTD).append(",\n")
                            .append("\"executionTimeAvg\":").append(executionTimeAvg).append(",\n")
                            .append("\"executionTimeSTD\":" ).append(executionTimeSTD).append("\n}\n");
                    if(!Driver.totalElapsedTime_perqidvid.containsKey(p.toString())){
                        Driver.totalElapsedTime_perqidvid.put(p.toString(), (double) 0);
                        Driver.totalExecutionTime_perqidvid.put(p.toString(), (double) 0);
                        Driver.totalClientResponseTime_perqidvid.put(p.toString(), (double) 0);
                    }
                    Driver.totalElapsedTime_perqidvid.put(p.toString(),Driver.totalElapsedTime_perqidvid.get(p.toString())+elapsedTimeAvg);
                    Driver.totalExecutionTime_perqidvid.put(p.toString(),Driver.totalExecutionTime_perqidvid.get(p.toString())+executionTimeAvg);
                    Driver.totalClientResponseTime_perqidvid.put(p.toString(),Driver.totalClientResponseTime_perqidvid.get(p.toString())+clientSideResponseTimeAvg);
                    resCount++;
            }
            tsb.append("]");
            avgsb.append("]");
            avgpw.println(avgsb.toString());
            pw.println(tsb.toString());
            pw.close();
            avgpw.close();

        } catch (IOException e) {
            System.err.println("Problem in creating report in StatsCollector !");
            e.printStackTrace();
        }
    }
}
