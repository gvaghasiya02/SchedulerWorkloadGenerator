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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

import config.Constants;

/**
 * @author pouria
 */
public class QueryStat {

    int qid;
    ArrayList<Double> clientTimes;
    ArrayList<Double> elapsedTimes;
    ArrayList<Double> executionTimes;

    public QueryStat(int qId) {
        qid = qId;
        clientTimes = new ArrayList<>();
        elapsedTimes = new ArrayList<>();
        executionTimes = new ArrayList<>();
    }

    public void getSumOfTimes(ArrayList<Double> times) {
        double sum = times.stream()
                .mapToDouble(a -> a)
                .sum();
    }

    public void setQid(int qId) {
        qid = qId;
    }

    public void addToClientTimes(double time) {
        clientTimes.add(time);
    }

    public void addToElapsedTimes(double time) {
        elapsedTimes.add(time);
    }

    public void addToExecutionTimes(double time) {
        executionTimes.add(time);
    }
    public String getTimesForChart(ArrayList<Double> times){
        StringBuffer sb = new StringBuffer();
        int index=0;
        for (Double t : times) {
            if(index>0)
                sb.append(",");
            sb.append(t);
            index++;
        }
        return sb.toString();
    }

    public String getIterations(ArrayList<Double> times) {
        int i=1;
        String result="";
        for(Double t: times) {
            if(i > 1)
                result = result+",";
            result = result+i;
            i++;
        }
        return result;
    }

    public double getAverageRT(int ignore, ArrayList<Double> times) {
        if (ignore >= times.size()) {
            return Constants.INVALID_TIME;
        }
        double sum = 0;
        double count = 0;
        int skip = 0;
        Iterator<Double> it = times.iterator();
        while (it.hasNext()) {
            double d = it.next();
            if ((++skip) > ignore) {
                sum += d;
                count++;
            }
        }
        return sum / count;
    }

    public double getSTD(int ignore, ArrayList<Double> times){
        double standardDeviation = 0;
        double count = 0;
        int skip = 0;
        double avg = getAverageRT(ignore, times);
        Iterator<Double> it = times.iterator();
        while (it.hasNext()) {
            double d = it.next();
            if ((++skip) > ignore) {
                standardDeviation += Math.pow(d - avg, 2);
                count++;
            }
        }
        return Math.sqrt(standardDeviation/count);
    }
}
