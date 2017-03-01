package storm.dataclean.TestTopology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.auxiliary.base.*;
import storm.dataclean.auxiliary.detect.DataTuple;
import storm.dataclean.auxiliary.repair.RepairProposal;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposal;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposalGroup;
import storm.dataclean.component.bolt.anchor.StartEndBolt;
import storm.dataclean.component.bolt.detect.DetectEgressRouterBolt;
import storm.dataclean.component.bolt.detect.DetectIngressRouterBolt;
import storm.dataclean.component.bolt.detect.DetectWorkerBolt;
import storm.dataclean.component.bolt.repair.*;
import storm.dataclean.component.spout.KafkaSpoutBuilder;
import storm.dataclean.metrics.TopologyMetrics;
import storm.dataclean.util.BleachConfig;

/**
 * Created by yongchao on 11/16/15.
 */
public class TestRepairNoControl {

    public static int repair_way;

    public static BleachConfig genConfig(String[] args) {
        String configfile = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-config")) {
                configfile = args[++i];
            }
//            if (args[i].equals("-repair")) {
//                repair_way = Integer.parseInt(args[++i]);
//            }

        }

        if (configfile.equals("")) {
            System.out.println("Usage: -config <configfile>");
            System.exit(0);
        }
//        if(repair_way <= 0 || repair_way > 3){
//            System.out.println("There are only 3 repair methods");
//            System.exit(0);
//        }

        BleachConfig config = BleachConfig.parse(configfile);
        return config;
    }

    public static void registerSerialization(Config conf){
        conf.registerSerialization(Violation.class);
        conf.registerSerialization(Violation.NullViolation.class);
        conf.registerSerialization(ViolationCause.class);
        conf.registerSerialization(RepairProposal.class);
        conf.registerSerialization(ViolationGroup.class);
        conf.registerSerialization(MergeEQClassProposalGroup.class);
        conf.registerSerialization(MergeEQClassProposal.class);
        conf.registerSerialization(BasicSuperCell.class);
        conf.registerSerialization(WinSuperCell.class);
        conf.registerSerialization(ComWinSuperCell.class);
        conf.registerSerialization(CircularFifoQueue.class);
        conf.registerSerialization(DataTuple.class);
    }


    public static void main(String[] args) throws Exception {

        BleachConfig bconfig = genConfig(args);
        TopologyBuilder builder = new TopologyBuilder();

        RepairWorkerBolt rw;
        repair_way = bconfig.getRW();
        if(repair_way == 1){
            rw = new BasicRepairWorkerBolt(bconfig);
        } else if(repair_way == 2){
            rw = new BleachIRRepairWorkerBolt(bconfig);
        } else {
            rw = new BleachDRRepairWorkerBolt(bconfig);
        }

        builder.setSpout(bconfig.get(BleachConfig.SPOUT_DATA_ID), KafkaSpoutBuilder.buildKafkaDataSpout(bconfig), Integer.parseInt(bconfig.get(BleachConfig.SPOUT_NUM)));

//        builder.setSpout(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), KafkaSpoutBuilder.buildKafkaControlSpout(bconfig),1);

        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_REPAIR_EGRESS_ID), new StartEndBolt(bconfig),
                Integer.parseInt(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_REPAIR_EGRESS_NUM))).
                fieldsGrouping(bconfig.get(BleachConfig.SPOUT_DATA_ID), new Fields("kid")).
                fieldsGrouping(bconfig.get(BleachConfig.BOLT_REPAIR_AGGREGATOR_ID), GlobalConstant.REPAIRAGG_START_STREAM, new Fields("kid")).
                fieldsGrouping(bconfig.get(BleachConfig.BOLT_DETECT_EGRESS_ID), GlobalConstant.DETECTEGRESS_START_STREAM, new Fields("kid"));
        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_ID), new DetectIngressRouterBolt(bconfig), 1).
                shuffleGrouping(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_REPAIR_EGRESS_ID), GlobalConstant.START_DETECTINGRESS_STREAM);
//                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID));
//                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.CONTROL_STREAM);

        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID), new DetectWorkerBolt(bconfig), Integer.parseInt(bconfig.get(BleachConfig.BOLT_DETECT_WORKER_NUM)))
                .directGrouping(bconfig.get(BleachConfig.BOLT_DETECT_INGRESS_ID), GlobalConstant.DETECTINGRESS_DETECTWORKER_STREAM);

//                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.CONTROL_STREAM);

        builder.setBolt(bconfig.get(BleachConfig.BOLT_DETECT_EGRESS_ID), new DetectEgressRouterBolt(bconfig),  Integer.parseInt(bconfig.get(BleachConfig.BOLT_DETECT_EGRESS_NUM))).
                fieldsGrouping(bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID), GlobalConstant.DETECTWORKER_DETECTEGRESS_STREAM, new Fields("tid"));
//                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.CONTROL_STREAM);
//                shuffleGrouping(bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID), GlobalConstant.DETECT_DATA_STREAM);
        builder.setBolt(bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID), rw,
                Integer.parseInt(bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_NUM))).
                allGrouping(bconfig.get(BleachConfig.BOLT_DETECT_EGRESS_ID), GlobalConstant.DETECTEGRESS_REPAIRWORKER_STREAM).
                allGrouping(bconfig.get(BleachConfig.BOLT_REPAIR_COORDINATOR_ID), GlobalConstant.REPAIR_COORDINATION_STREAM);

//                allGrouping(bconfig.get(BleachConfig.SPOUT_CONTROL_ID), GlobalConstant.REPAIR_CONTROL_STREAM);

        builder.setBolt(bconfig.get(BleachConfig.BOLT_REPAIR_AGGREGATOR_ID), new RepairAggregatorBolt(bconfig), Integer.parseInt(bconfig.get(BleachConfig.BOLT_REPAIR_AGGREGATOR_NUM))).
                fieldsGrouping(bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID), GlobalConstant.REPAIRWORKER_REPAIRAGG_STREAM, new Fields("kid"));
        builder.setBolt(bconfig.get(BleachConfig.BOLT_REPAIR_COORDINATOR_ID), new RepairCoordinatorBolt(bconfig), 1).
                shuffleGrouping(bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID), GlobalConstant.REPAIR_COORDINATION_STREAM);
        Config conf = new Config();



        registerSerialization(conf);
        conf.setNumWorkers(Integer.parseInt(bconfig.get(BleachConfig.NUMPROCESS)));
        conf.setDebug(false);
        if(bconfig.getNumAcker() != 0)
        {
            conf.setNumAckers(bconfig.getNumAcker());
        }
        conf.setFallBackOnJavaSerialization(false);
        if (bconfig.getMaxspoutpending() > 0) {
            conf.setMaxSpoutPending(bconfig.getMaxspoutpending());
        }
        StormSubmitter.submitTopologyWithProgressBar(bconfig.getTopologyname(), conf, builder.createTopology());
        TopologyMetrics tm = new TopologyMetrics();

    }

}
