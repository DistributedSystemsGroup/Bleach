package storm.dataclean.metrics;

import backtype.storm.StormSubmitter;
//import backtype.storm.generated.ClusterSummary;
//import backtype.storm.generated.Nimbus;
//import backtype.storm.generated.SupervisorSummary;
//import backtype.storm.generated.TopologySummary;
//import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by yongchao on 11/4/15.
 */
public class TopologyMetrics {

    public static int TimeInterval = 30;
    public static int TimeTotal = 300;

    private static class MetricsState {
        long transferred = 0;
        int slotsUsed = 0;
        long lastTime = 0;
    }

    public TopologyMetrics() {
        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        //            metrics(client, _messageSize, _pollFreqSec, _testRunTimeSec);
        try {
            metrics(client, TimeInterval, TimeTotal);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void metrics(Nimbus.Client client, int poll, int total) throws Exception {
        System.out.println("status\ttopologies\ttotalSlots\tslotsUsed\ttotalExecutors\texecutorsWithMetrics\ttime\ttime-diff ms\ttransferred\ttotal Failed");
        MetricsState state = new MetricsState();
        long pollMs = poll * 1000;
        long now = System.currentTimeMillis();
        state.lastTime = now;
        long startTime = now;
        long cycle = 0;
        long sleepTime;
        long wakeupTime;
        while (metrics(client, now, state, "WAITING")) {
            now = System.currentTimeMillis();
            cycle = (now - startTime) / pollMs;
            wakeupTime = startTime + (pollMs * (cycle + 1));
            sleepTime = wakeupTime - now;
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
            now = System.currentTimeMillis();
        }

        now = System.currentTimeMillis();
        cycle = (now - startTime) / pollMs;
        wakeupTime = startTime + (pollMs * (cycle + 1));
        sleepTime = wakeupTime - now;
        if (sleepTime > 0) {
            Thread.sleep(sleepTime);
        }
        now = System.currentTimeMillis();
        long end = now + (total * 1000);
        do {
            metrics(client, now, state, "RUNNING");
            now = System.currentTimeMillis();
            cycle = (now - startTime) / pollMs;
            wakeupTime = startTime + (pollMs * (cycle + 1));
            sleepTime = wakeupTime - now;
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
            now = System.currentTimeMillis();
        } while (now < end);
    }

    public boolean metrics(Nimbus.Client client, long now, MetricsState state, String message) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        long time = now - state.lastTime;
        state.lastTime = now;
        int numSupervisors = summary.get_supervisors_size();
        int totalSlots = 0;
        int totalUsedSlots = 0;
        for (SupervisorSummary sup : summary.get_supervisors()) {
            totalSlots += sup.get_num_workers();
            totalUsedSlots += sup.get_num_used_workers();
        }
        int slotsUsedDiff = totalUsedSlots - state.slotsUsed;
        state.slotsUsed = totalUsedSlots;

        int numTopologies = summary.get_topologies_size();
        long totalTransferred = 0;
        int totalExecutors = 0;
        int executorsWithMetrics = 0;
        int totalFailed = 0;
        for (TopologySummary ts : summary.get_topologies()) {
            String id = ts.get_id();
            TopologyInfo info = client.getTopologyInfo(id);
            for (ExecutorSummary es : info.get_executors()) {


//                es.get_component_id()
                ExecutorStats stats = es.get_stats();

//                stats.

                totalExecutors++;
                if (stats != null) {
                    if (stats.get_specific().is_set_spout()) {
                        SpoutStats ss = stats.get_specific().get_spout();
//                        ss.get
                        Map<String, Long> failedMap = ss.get_failed().get(":all-time");
                        if (failedMap != null) {
                            for (String key : failedMap.keySet()) {
                                Long tmp = failedMap.get(key);
                                if (tmp != null) {
                                    totalFailed += tmp;
                                }
                            }
                        }
                    }

                    Map<String, Map<String, Long>> transferred = stats.get_transferred();
                    if (transferred != null) {
                        Map<String, Long> e2 = transferred.get(":all-time");
                        if (e2 != null) {
                            executorsWithMetrics++;
                            //The SOL messages are always on the default stream, so just count those
                            Long dflt = e2.get("default");
                            if (dflt != null) {
                                totalTransferred += dflt;
                            }
                        }
                    }
                }
            }
        }
        long transferredDiff = totalTransferred - state.transferred;
        state.transferred = totalTransferred;
//        double throughput = (transferredDiff == 0 || time == 0) ? 0.0 : (transferredDiff * size)/(1024.0 * 1024.0)/(time/1000.0);
        System.out.println(message + "\t" + numTopologies + "\t" + totalSlots + "\t" + totalUsedSlots + "\t" + totalExecutors + "\t" + executorsWithMetrics + "\t" + now + "\t" + time + "\t" + transferredDiff + "\t" + totalFailed);
        if ("WAITING".equals(message)) {
            //System.err.println(" !("+totalUsedSlots+" > 0 && "+slotsUsedDiff+" == 0 && "+totalExecutors+" > 0 && "+executorsWithMetrics+" >= "+totalExecutors+")");
        }
        return !(totalUsedSlots > 0 && slotsUsedDiff == 0 && totalExecutors > 0 && executorsWithMetrics >= totalExecutors);
    }

}
