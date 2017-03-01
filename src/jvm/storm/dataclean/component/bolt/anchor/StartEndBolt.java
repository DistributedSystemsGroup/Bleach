package storm.dataclean.component.bolt.anchor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.guava.collect.ArrayListMultimap;
import org.apache.storm.guava.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dataclean.auxiliary.*;
import storm.dataclean.auxiliary.repair.RepairProposal;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.BufferedTupleNotFoundException;
import storm.dataclean.util.BleachConfig;

import java.util.*;

/**
 * Created by tian on 26/11/2015.
 */
public class StartEndBolt extends BaseRichBolt {

    public OutputCollector _collector;
    private static final Logger LOG = LoggerFactory.getLogger(StartEndBolt.class);
    public static String KID = "kid"; // original tuple id
    public static String MERGED_PROPOSE = "merged-propose";


    public String schemestring;
    public List<String> attributes;
    public HashMap<Integer, List<Object>> kid_tuple_map;
    public Multimap<Integer, RepairProposal> tid_repairproposal_map;
    public int numrepairworker;
    public String repairworkerid;

    public String DETECT_TUPLESOURCE_STREAM_ID;  // receive from KafkaSpout as DetectIngressRouter
    public String DETECT_CONTROL_STREAM_ID; //  receive from DetectController(not implemented) as DetectIngressRouter
    public String DETECT_DATA_STREAM_ID;    //  send to DetectWorker as DetectIngressRouter

    public String REPAIR_PROPOSAL_STREAM_ID;  //  receive from RepairWorkerBolt as RepairEgressRouter
    public String REPAIR_CLEANNOTIFICATION_STREAM_ID;   //  receive from DetectEgressRouterBolt/RepairIngressRouter as RepairEgressRouter

    public String DETECT_WORKER_BOLT_ID;

    public static String output_prefix_repaired = "DATA_OUTPUT repaired: ";
    public static String output_prefix_norepaired = "DATA_OUTPUT no repaired: ";

    public String[] predefined_rulestrings;

    public boolean started;
    public int num;

    public StartEndBolt(BleachConfig bconfig) {
        predefined_rulestrings = bconfig.getRules();
        DETECT_TUPLESOURCE_STREAM_ID = GlobalConstant.REPAIR_ORIGINAL_DATA_STREAM;
        DETECT_CONTROL_STREAM_ID = GlobalConstant.CONTROL_STREAM;
//        DETECT_DATA_STREAM_ID = GlobalConstant.DETECT_DATA_STREAM;
        DETECT_DATA_STREAM_ID = GlobalConstant.START_DETECTINGRESS_STREAM;
//        REPAIR_PROPOSAL_STREAM_ID = GlobalConstant.REPAIR_DATA_STREAM;
        REPAIR_PROPOSAL_STREAM_ID = GlobalConstant.REPAIRAGG_START_STREAM;
//        REPAIR_CLEANNOTIFICATION_STREAM_ID = GlobalConstant.REPAIR_CLEAN_NOTIFICATION_STREAM;
        REPAIR_CLEANNOTIFICATION_STREAM_ID = GlobalConstant.DETECTEGRESS_START_STREAM;

        DETECT_WORKER_BOLT_ID = bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID);
        repairworkerid = bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID);
        schemestring = bconfig.get(BleachConfig.SCHEMA);
        started = false;
        num = 0;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        kid_tuple_map = new HashMap();
        tid_repairproposal_map = ArrayListMultimap.create();
        numrepairworker = topologyContext.getComponentTasks(repairworkerid).size();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (tuple.getSourceStreamId().equals(DETECT_TUPLESOURCE_STREAM_ID)) {   // receive from kafka-spout
                receive_original_data(tuple);
            } else if (tuple.getSourceStreamId().equals(DETECT_CONTROL_STREAM_ID)) {    // receive from detect-controller
                process_control(tuple);
            } else if (tuple.getSourceStreamId().equals(REPAIR_CLEANNOTIFICATION_STREAM_ID)) {  // receive from detect-egress
                output_clean_data(tuple);
            } else if (tuple.getSourceStreamId().equals(REPAIR_PROPOSAL_STREAM_ID)) { // receive from repair-aggregator
                repair_output_data(tuple);
            }
            _collector.ack(tuple);
        } catch (BufferedTupleNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void process_control(Tuple tuple) {

    }

    public void receive_original_data(Tuple tuple) {
        _collector.emit(DETECT_DATA_STREAM_ID, tuple, new Values(tuple.getValues().toArray()));
        store_original_data(tuple);
    }

    public void store_original_data(Tuple tuple) {
        kid_tuple_map.put(tuple.getIntegerByField(KID), tuple.getValues());
    }

    public void output_clean_data(Tuple tuple) throws BufferedTupleNotFoundException {




        int kid = tuple.getIntegerByField(KID);
//        System.out.println("receive clean tuple, kid = " + kid);
        List<Object> buffered_tuple = kid_tuple_map.get(kid);
        if (buffered_tuple == null) {
            throw new BufferedTupleNotFoundException(kid);
        }
        write_tuple_log(false, buffered_tuple);
        kid_tuple_map.remove(kid);
    }

    public void repair_output_data(Tuple tuple) throws BufferedTupleNotFoundException {
        /**
         * TODO
         */
        int kid = tuple.getIntegerByField(KID);
        RepairProposal rp = (RepairProposal) tuple.getValueByField(MERGED_PROPOSE);
        List<Object> original_tuple = kid_tuple_map.get(kid);

//        System.out.println("kid=" + kid);


        if (original_tuple == null) {
            throw new BufferedTupleNotFoundException(kid);
        }
        output_cleaned_data(original_tuple, rp);
        kid_tuple_map.remove(kid);
    }

    public void output_cleaned_data(List<Object> original_tuple, RepairProposal rp) {
        try {
            List<Object> repaired_tuple = rp.getFixedTuple(original_tuple);
            write_tuple_log(true, repaired_tuple);
        } catch (BleachException e) {
            e.printStackTrace();
        }

    }

    private void write_tuple_log(boolean repaired, List<Object> tuple) {
        num++;

//        System.out.println("startend tuple: " + tuple);

        if (repaired) {
            LOG.info(output_prefix_repaired + tuple.toString());
        } else {
            LOG.info(output_prefix_norepaired + tuple.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> fields = new ArrayList();
        attributes = Arrays.asList(schemestring.split(","));
        fields.addAll(attributes);
        outputFieldsDeclarer.declareStream(DETECT_DATA_STREAM_ID, new Fields(fields));
    }
}
