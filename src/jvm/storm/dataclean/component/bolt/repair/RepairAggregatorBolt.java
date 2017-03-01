package storm.dataclean.component.bolt.repair;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.guava.collect.ArrayListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.auxiliary.repair.RepairProposal;
import storm.dataclean.util.BleachConfig;

import java.util.*;

/**
 * Created by tian on 14/04/2016.
 */
public class RepairAggregatorBolt extends BaseRichBolt {

    public OutputCollector _collector;
    private static final Logger LOG = LoggerFactory.getLogger(RepairAggregatorBolt.class);
    public static String KID = "kid";
    public static String PARTIAL_PROPOSE = "partial-propose";
    public static String MERGED_PROPOSE = "merged-propose";
    public ArrayListMultimap<Integer, RepairProposal> tid_repairproposal_map;
    public int numrepairworker;
    public String repairworkerid;

    public String REPAIR_PROPOSAL_STREAM_ID;  //  receive from RepairWorkerBolt as RepairEgressRouter
    public String REPAIR_CONTROL_STREAM_ID;


    public boolean started;
    public int num;

    public RepairAggregatorBolt(BleachConfig bconfig) {
//        REPAIR_PROPOSAL_STREAM_ID = GlobalConstant.REPAIR_DATA_STREAM;
        REPAIR_PROPOSAL_STREAM_ID = GlobalConstant.REPAIRAGG_START_STREAM;
        REPAIR_CONTROL_STREAM_ID = GlobalConstant.CONTROL_STREAM;
        repairworkerid = bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID);
        started = false;
        num = 0;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        tid_repairproposal_map = ArrayListMultimap.create();
        numrepairworker = topologyContext.getComponentTasks(repairworkerid).size();
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(REPAIR_CONTROL_STREAM_ID)) {   // receive from repair_controller
            process_control(tuple);
            return;
        }
        int kid = tuple.getIntegerByField(KID);
        RepairProposal rp = (RepairProposal) tuple.getValueByField(PARTIAL_PROPOSE);
        tid_repairproposal_map.put(kid, rp);
        List<RepairProposal> rplist = tid_repairproposal_map.get(kid);
        if (rplist.size() == numrepairworker) {
            RepairProposal merged_rp = rplist.get(0);
            for(int i = 1; i < rplist.size(); i++){
                merged_rp.merge(rplist.get(i));
            }
            tid_repairproposal_map.removeAll(kid);
            _collector.emit(REPAIR_PROPOSAL_STREAM_ID, tuple, new Values(kid, merged_rp));
        }
        _collector.ack(tuple);
    }

    public void process_control(Tuple tuple){
        System.out.println("RA receive control message");
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(REPAIR_PROPOSAL_STREAM_ID, new Fields(KID,MERGED_PROPOSE));
    }
}
