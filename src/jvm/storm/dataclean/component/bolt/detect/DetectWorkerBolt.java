package storm.dataclean.component.bolt.detect;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.detect.DataTuple;
import storm.dataclean.auxiliary.rule.Rule;
import storm.dataclean.auxiliary.rule.RuleGenerator;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.RuleDefinitionException;
import storm.dataclean.util.BleachConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yongchao on 11/13/15.
 */
public class DetectWorkerBolt extends BaseRichBolt {

    private OutputCollector _collector;

    public static String KID = "kid";
    public static String TID = "tid";
    public static String SUBTUPLE = "subtuple";

    public String DETECT_CONTROL_STREAM_ID;
    public String DETECT_DATA_STREAM_ID;
    public String DETECT_WORKER_BOLT_ID;

    public List<Rule> rules;

    public String[] predefined_rulestrings;
    public String schema;
    public int dwnum;
    public int workerid;

    public int window; // 0 no window, 1 basic window, 2 bleach window
    public int window_size;

    private static final Logger LOG = LoggerFactory.getLogger(DetectWorkerBolt.class);
    public int mesure_counter;

    public DetectWorkerBolt(BleachConfig bconfig) {
        DETECT_CONTROL_STREAM_ID = GlobalConstant.CONTROL_STREAM;
//        DETECT_DATA_STREAM_ID = GlobalConstant.DETECT_DATA_STREAM;
        DETECT_DATA_STREAM_ID = GlobalConstant.DETECTWORKER_DETECTEGRESS_STREAM;
        predefined_rulestrings = bconfig.getRules();
        schema = bconfig.getSchema();
        mesure_counter = 0;
        window = bconfig.getWindow();
        window_size = bconfig.getWindowSize();
        DETECT_WORKER_BOLT_ID = bconfig.get(BleachConfig.BOLT_DETECT_WORKER_ID);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        System.err.println("errprint: detect work");
        LOG.info("log: detect work");
        _collector = collector;

        workerid = context.getThisTaskIndex();
        dwnum = context.getComponentTasks(DETECT_WORKER_BOLT_ID).size();

        rules = new ArrayList();
        try{
            for(int index = workerid; index < predefined_rulestrings.length; index += dwnum){
                rules.add(RuleGenerator.parse(index, predefined_rulestrings[index], schema, window, window_size));
                System.err.println("Bleach: detect worker: generated rule id=" + index);
            }
        } catch (RuleDefinitionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceComponent().equals("bleach-control")) {
            process_control(tuple);
        } else {
            process_data(tuple);
        }
        _collector.ack(tuple);

    }

    public void process_control(Tuple tuple) {
        String control_id = tuple.getStringByField("control-msg-id");
        String control_msg = tuple.getStringByField("control-msg");
        System.out.println("DetectWorkerBolt: "+control_id+", "+control_msg);


        if(control_id.equals("del_rule")){
            int rule_del_id = Integer.parseInt(control_msg);
            rules.removeIf(x->x.getRid()==rule_del_id);

        } else if(control_id.equals("dw_add_rule")){
            int rule_id = Integer.parseInt(control_msg.split(";")[0]);
            String rulestring = control_msg.split(";")[1];
            if(rule_id%dwnum == workerid) {
                try {
                    Rule r = RuleGenerator.parse(rule_id, rulestring, schema, window, window_size);
                    rules.add(r);
                    System.out.println("detectworker2: add a rule=" + r);
                } catch (RuleDefinitionException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public void process_data(Tuple tuple) {
        try {
            List<Violation> v_list = new ArrayList();
            DataTuple dt = (DataTuple)tuple.getValueByField(SUBTUPLE);
            int tid = tuple.getIntegerByField(TID);
            int kid = tuple.getIntegerByField(KID);
            for(Rule r: rules){
                Violation v = r.detect(tid, dt);
                v.setKid(kid);
                v_list.add(v);
            }
            _collector.emit(DETECT_DATA_STREAM_ID, tuple, new Values(tid, v_list));
        } catch (BleachException e) {
            e.printStackTrace();
        }
    }

    public void print_log(){
        System.err.println("print_log in detect worker not implemented");
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(DETECT_DATA_STREAM_ID, new Fields("tid", "violation"));
    }
}
