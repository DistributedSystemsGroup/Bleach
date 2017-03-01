package storm.dataclean.component.bolt.detect;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.guava.collect.HashMultimap;
import org.apache.storm.guava.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.auxiliary.detect.DataTuple;
import storm.dataclean.auxiliary.rule.Rule;
import storm.dataclean.auxiliary.rule.RuleGenerator;
import storm.dataclean.exceptions.RuleDefinitionException;
import storm.dataclean.util.BleachConfig;

import java.util.List;
import java.util.Map;

/**
 * Created by yongchao on 11/13/15.
 * Should not be used. It is old version.
 */

public class DetectIngressRouterBolt extends BaseRichBolt {

    private OutputCollector _collector;

    public static String KID = "kid";
    public static String TID = "tid";
    public static String SUBTUPLE = "subtuple";

    public String DETECT_CONTROL_STREAM_ID;
    public String DETECT_DATA_STREAM_ID;
    public String DETECT_WORKER_BOLT_ID;


    public String schema;

    public Multimap<Integer, Rule> rwtable; // rule - worker mapping table
    public Multimap<Integer, String> dw_attrs_table; // dw attributes table


    public List<Integer> workerids;

    public int tid; // a new id, important
    public String[] predefined_rulestrings;
    public int num_dw;

    private static final Logger LOG = LoggerFactory.getLogger(DetectIngressRouterBolt.class);

    public DetectIngressRouterBolt(BleachConfig bconfig) {
        schema = bconfig.getSchema();
        predefined_rulestrings = bconfig.getRules();
        DETECT_CONTROL_STREAM_ID = GlobalConstant.CONTROL_STREAM;
//        DETECT_DATA_STREAM_ID = GlobalConstant.DETECT_DATA_STREAM;
        DETECT_DATA_STREAM_ID = GlobalConstant.DETECTINGRESS_DETECTWORKER_STREAM;
        DETECT_WORKER_BOLT_ID = bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID);
        tid = 0;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        rwtable = HashMultimap.create();
        dw_attrs_table = HashMultimap.create();
        workerids = context.getComponentTasks(DETECT_WORKER_BOLT_ID);
        num_dw = workerids.size();
        try {
            for (int i = 0; i < predefined_rulestrings.length; i++){
                Rule r = RuleGenerator.parse(i, predefined_rulestrings[i], schema);
                rwtable.put(workerids.get(i % num_dw), r);
                dw_attrs_table.putAll(workerids.get(i % num_dw), r.getAttrs());
            }
        } catch (RuleDefinitionException e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals("bleach-control")){
            process_control(tuple);
        } else {
            process_data(tuple);
        }
        _collector.ack(tuple);
    }


    public void remove_rule(int rid){
        int workerid = -1;
        Rule r = null;
        for(Map.Entry<Integer, Rule> entry : rwtable.entries()){
            r = entry.getValue();
            System.out.println("rule:"+r);
            if(rid == r.getRid()){
                workerid = entry.getKey();
                break;
            }
        }
        if(workerid < 0 || r == null){
            System.err.println("rule "+ rid + " not found");
        }

        rwtable.remove(workerid, r);
        dw_attrs_table.removeAll(workerid);
        for(Rule remaining_rule : rwtable.get(workerid)){
            dw_attrs_table.putAll(workerid,remaining_rule.getAttrs());
        }
    }

    public void add_rule(int rid, String rulestring){
        try {
            Rule r = RuleGenerator.parse(rid, rulestring, schema);
            rwtable.put(workerids.get(rid % num_dw), r);
            dw_attrs_table.putAll(workerids.get(rid % num_dw), r.getAttrs());
        } catch (RuleDefinitionException e) {
            e.printStackTrace();
        }
    }



    public void process_control(Tuple tuple){
        String control_id = tuple.getStringByField("control-msg-id");
        String control_msg = tuple.getStringByField("control-msg");
        System.out.println("DetectIngressRouterBolt: "+control_id+", "+control_msg);
        if(control_id.equals("del_rule")){
            remove_rule(Integer.parseInt(control_msg));
        } else if (control_id.equals("add_rule")){
            add_rule(Integer.parseInt(control_msg.split(";")[0]), control_msg.split(";")[1]);
        }


    }

    public void process_data(Tuple tuple){
        tid++;
        for(int i : rwtable.keySet()){
            Values v = new Values(tid, tuple.getValueByField(KID));
            DataTuple dt = new DataTuple();
            for(String attr: dw_attrs_table.get(i)){
                dt.add(attr, tuple.getValueByField(attr));
            }
            v.add(dt);
            _collector.emitDirect(i, DETECT_DATA_STREAM_ID, tuple, v);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(DETECT_DATA_STREAM_ID, true, new Fields(TID, KID, SUBTUPLE));
    }
}
