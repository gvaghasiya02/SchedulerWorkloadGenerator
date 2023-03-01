package Prometheus;

import driver.Driver;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BigFunCollector extends Collector {
    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<>();
        GaugeMetricFamily gauges = new GaugeMetricFamily("bigfun_running_queries", "Current Running Queries in Bigfun",Arrays.asList("lables"));
        for (String key : Driver.clientToRunningQueries.keySet()){
            gauges.addMetric(Arrays.asList(key + ":qid-vid"+Driver.clientToRunningQueries.get(key)),Integer.valueOf(Driver.clientToRunningQueries.get(key).split("-")[0]));
            mfs.add(gauges);
        }
        mfs.add(gauges);
        return mfs;
    }
}
