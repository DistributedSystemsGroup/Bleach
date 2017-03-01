package storm.dataclean.component.bolt.detect;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.base.ViolationGroup;
import storm.dataclean.auxiliary.rule.Rule;
import storm.dataclean.auxiliary.rule.RuleGenerator;
import storm.dataclean.exceptions.RuleDefinitionException;
import storm.dataclean.util.BleachConfig;

import java.util.*;

/**
 * Created by yongchao on 11/16/15.
 */
public class DetectEgressRouterBolt extends BaseRichBolt {

    private OutputCollector _collector;

    public static String TID = "tid"; // tuple id assigned by bleach
    public static String KID = "kid"; // original tuple id
    public static String VG = "vg";

    public String DETECT_CONTROL_STREAM_ID; // from detect_controller to detect_egress_router
    public String DETECT_DATA_STREAM_ID;    // from detect_worker to detect_egress_router and from egress_router to repair worker
    public String DETECT_NOVIO_SHORTCUT_STREAM_ID; // from detect_egress_router to repair_egress_router

    public HashSet<Rule> ruleSet;
    public String detectworkerid;
    public int numdetectworker;
    public HashMap<Integer, String> intrule_attr_map; // intersecting rule set (with common attributes)
    public HashMap<Integer, Collection<Tuple>> tuple_buffer;
    public HashSet<String> violated_attrs;

    public String[] predefined_rulestrings;
    public String schema;

    public DetectEgressRouterBolt(BleachConfig bconfig) {
        DETECT_CONTROL_STREAM_ID = GlobalConstant.CONTROL_STREAM;
//        DETECT_DATA_STREAM_ID = GlobalConstant.DETECT_DATA_STREAM;
        DETECT_DATA_STREAM_ID = GlobalConstant.DETECTEGRESS_REPAIRWORKER_STREAM;
//        DETECT_NOVIO_SHORTCUT_STREAM_ID = GlobalConstant.REPAIR_CLEAN_NOTIFICATION_STREAM;
        DETECT_NOVIO_SHORTCUT_STREAM_ID = GlobalConstant.DETECTEGRESS_START_STREAM;
        predefined_rulestrings = bconfig.getRules();
        schema = bconfig.getSchema();
        detectworkerid = bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        ruleSet = new HashSet();
        violated_attrs = new HashSet();
        tuple_buffer = new HashMap();
        numdetectworker = context.getComponentTasks(detectworkerid).size();
        System.err.println("numdetectworker="+numdetectworker);
        try{
            for(int i = 0; i < predefined_rulestrings.length; i++){
                Rule r = RuleGenerator.parse(i, predefined_rulestrings[i], schema);
                ruleSet.add(r);
            }
            build_intrule_attr_map();
            System.err.println("Bleach: DetectEgressRouterBolt: ruleset size=" + ruleSet.size());
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
    }

    public void process_control(Tuple tuple){
        String control_id = tuple.getStringByField("control-msg-id");
        String control_msg = tuple.getStringByField("control-msg");
        System.out.println("DetectEgressRouterBolt: "+control_id+", "+control_msg);
        if(control_id.equals("del_rule")){
            int rule_del_id = Integer.parseInt(control_msg);
            if(intrule_attr_map.containsKey(rule_del_id)){
                String attr = intrule_attr_map.get(rule_del_id);
                intrule_attr_map.remove(rule_del_id);
                int other_rule = -1;
                for(Map.Entry<Integer,String> entry : intrule_attr_map.entrySet()){
                    if(entry.getValue().equals(attr)){
                        if(other_rule < 0){
                            other_rule = entry.getKey();
                        } else {
                            other_rule = -1;
                            break;
                        }
                    }
                }
                if(other_rule >= 0){
                    intrule_attr_map.remove(other_rule);
                }
            }
        } else if(control_id.equals("add_rule")){
            int rule_id = Integer.parseInt(control_msg.split(";")[0]);
            String rulestring = control_msg.split(";")[1];
            try {
                ruleSet.add(RuleGenerator.parse(rule_id, rulestring, schema));
            } catch (RuleDefinitionException e) {
                e.printStackTrace();
            }
            build_intrule_attr_map();
            System.out.println("after adding, interrule_attr_map:"+intrule_attr_map);

        }
        _collector.ack(tuple);
    }

    public void process_data(Tuple tuple){
        int tid = tuple.getIntegerByField(TID);
        Collection<Tuple> tuples;
        if(tuple_buffer.containsKey(tid)){
            tuples = tuple_buffer.get(tid);
            tuples.add(tuple);
        } else {
            tuples = new ArrayList();
            tuples.add(tuple);
            tuple_buffer.put(tid, tuples);
        }
        if(tuples.size()==numdetectworker){
            int kid = -1;
            ViolationGroup vg = new ViolationGroup(tid);
            for(Tuple t_stored : tuples){
                Collection<Violation> v_list = (List<Violation>)t_stored.getValueByField("violation");
                for(Violation v: v_list){
                    if(kid < 0){
                        kid = v.getKid();
                    }
                    if(!(v instanceof Violation.NullViolation)){
                        vg.addViolation(v, intrule_attr_map.keySet());
                    }
                }
            }
            if(vg.isEmptyViolation()){
                _collector.emit(DETECT_NOVIO_SHORTCUT_STREAM_ID, tuples, new Values(kid));
            } else {
                vg.setKid(kid);
                _collector.emit(DETECT_DATA_STREAM_ID, tuples, new Values(vg));
            }
            for(Tuple t_stored : tuples)
            {
                _collector.ack(t_stored);
            }
            tuple_buffer.remove(tid);
        }
    }

    public void build_intrule_attr_map(){

        intrule_attr_map = new HashMap();
        for(Rule r : ruleSet){
            for(Rule r1 : ruleSet){
                if(r.getRid() == r1.getRid()){
                    continue;
                } else {
                    if(r.getValueAttr().equals(r1.getValueAttr())){
                        intrule_attr_map.put(r.getRid(), r.getValueAttr());
                    }
                }
            }
        }

        System.out.println("DetectEgressRouterBolt: intrule_attr_map: " + intrule_attr_map);


    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(DETECT_DATA_STREAM_ID, new Fields(VG));
        declarer.declareStream(DETECT_NOVIO_SHORTCUT_STREAM_ID, new Fields(KID));
    }




}
