package storm.dataclean.component.bolt.repair;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
//import org.apache.storm.guava.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import storm.dataclean.auxiliary.*;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.base.ViolationCause;
import storm.dataclean.auxiliary.base.ViolationGroup;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposalGroup;
import storm.dataclean.auxiliary.repair.subgraph.AbstractSubGraph;
import storm.dataclean.auxiliary.repair.violationgraph.ViolationGraph;
import storm.dataclean.auxiliary.repair.violationgraph.BasicViolationGraph;
import storm.dataclean.auxiliary.repair.violationgraph.BasicWinViolationGraph;
import storm.dataclean.auxiliary.repair.violationgraph.BleachWinViolationGraph;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.util.BleachConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Created by yongchao on 12/29/15.
 */
public abstract class RepairWorkerBolt extends BaseRichBolt {

    protected OutputCollector _collector;
    public String REPAIR_COORDINATE_STREAM_ID;  // from repair_coordinator to repair_worker;
    public String REPAIR_DATA_STREAM_ID;    // from repair_ingress/detect_egress to repair worker and from repair_worker to repair_egress
    public String REPAIR_CONTROL_STREAM_ID; // from repair_controller to repair_worker
    public String REPAIR_TO_COORDINATE_STREAM_ID; // from repair_worker to repair_coordinator

    public String schema;
    public String[] attrs;
    public String repairworkerid;
    public int numrepairworker;
    public int taskindex;
    public int proposal_size;

    public Predicate<Integer> tid_predicate;
    public Predicate<Violation> violation_appendonly_predicate;
    public HashMap<Integer, String> intrule_attr_map; // intersecting rule set (with common attributes)
    public ViolationGraph fdrg; // violation_cause and partial eqclass table;

    public HashMap<Integer, ViolationGroup> buffered_tid_vlist_map;


    public int win; // 0 no window, 1 basic window, 2 bleach window
    public int win_size;

    protected static final Logger LOG = LoggerFactory.getLogger(RepairWorkerBolt.class);

    public RepairWorkerBolt(BleachConfig bconfig) {
        init_Streamid();
        schema = bconfig.getSchema();
        repairworkerid = bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID);
        proposal_size = Integer.parseInt(bconfig.get(BleachConfig.REPAIR_PROPOSAL_SIZE));
        win = bconfig.getWindow();
        win_size = bconfig.getWindowSize();
    }

//    public void init_Streamid(){
//        REPAIR_COORDINATE_STREAM_ID = GlobalConstant.REPAIR_FROM_COORDINATE_STREAM;
//        REPAIR_DATA_STREAM_ID = GlobalConstant.REPAIR_DATA_STREAM;
//        REPAIR_CONTROL_STREAM_ID = GlobalConstant.REPAIR_CONTROL_STREAM;
//        REPAIR_TO_COORDINATE_STREAM_ID = GlobalConstant.REPAIR_TO_COORDINATE_STREAM;
//    }

    public void init_Streamid(){
        REPAIR_COORDINATE_STREAM_ID = GlobalConstant.REPAIR_COORDINATION_STREAM;
        REPAIR_DATA_STREAM_ID = GlobalConstant.REPAIRWORKER_REPAIRAGG_STREAM;
        REPAIR_CONTROL_STREAM_ID = GlobalConstant.REPAIR_CONTROL_STREAM;
        REPAIR_TO_COORDINATE_STREAM_ID = GlobalConstant.REPAIR_COORDINATION_STREAM;

    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        attrs = schema.split(",");
        numrepairworker = context.getComponentTasks(repairworkerid).size();
        taskindex = context.getThisTaskIndex();
        tid_predicate = x -> x % numrepairworker == taskindex;

        //debug use
        System.err.println("Bleach: RepairWorkerBolt: taskindex =" + taskindex + ", numrepairworker = " + numrepairworker );

        violation_appendonly_predicate = violation -> !violation.isNewVio();
        intrule_attr_map = new HashMap();
        buffered_tid_vlist_map = new HashMap();
        if(win == 0)
        {
            fdrg = new BasicViolationGraph(proposal_size, tid_predicate, attrs);
        } else if(win == 1) {
            fdrg = new BasicWinViolationGraph(proposal_size, tid_predicate, attrs, 0, win_size);

        }
        else if(win == 2){
            fdrg = new BleachWinViolationGraph(proposal_size, tid_predicate, attrs, 0, win_size);
        } else {
            System.err.println("Bleach: not implemented");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (tuple.getSourceComponent().equals("bleach-control")) {   // receive from repair_controller
                process_control(tuple);
            } else if (tuple.getSourceStreamId().equals(REPAIR_COORDINATE_STREAM_ID)) {  // receive from repair_coordinator
                process_coordinate(tuple);
            } else {    // receive from repair_ingress/detect_egress
                process_data(tuple);
            }
        } catch (BleachException e) {
            e.printStackTrace();
        }
        _collector.ack(tuple);
    }

    public void process_control(Tuple tuple) {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~debug: repair worker receive control message~~~~~~~~~~~~~~~~~~~~~~~~~~");
        String control_id = tuple.getStringByField("control-msg-id");
        String control_msg = tuple.getStringByField("control-msg");
        System.out.println("RepairWorkerBolt: " + control_id + ", " + control_msg);
        if(control_id.equals("rw_del_rule")){
            int rule_del_id = Integer.parseInt(control_msg);
            System.out.println("before delete");
            fdrg.delete_rule(rule_del_id);
            System.out.println("after delete");
        }

    }

    public abstract void process_data(Tuple tuple) throws BleachException;

    public abstract void process_coordinate(Tuple tuple) throws BleachException;

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(REPAIR_TO_COORDINATE_STREAM_ID, new Fields("merge-propose"));
        declarer.declareStream(REPAIR_DATA_STREAM_ID, new Fields("kid", "partial-propose"));
    }

    public MergeEQClassProposalGroup getMergeEQClassProposal(int tid, int kid, ViolationGroup vg) throws BleachException {
        MergeEQClassProposalGroup mp = new MergeEQClassProposalGroup(tid, kid);
        for (int attr : vg.getAttrs()) {
            Collection<Violation> vios = vg.getViolations(attr);
            boolean found = false;
            for(Violation v : vios){
                if(!violation_appendonly_predicate.test(v)){
                    found = true;
                    break;
                }
            }
            if(!found){
                continue;
            }else{
                for (Violation v : vios) {
                    int attrid = v.getRattr_index();
                    if(tid_predicate.test(tid))
                    {
                        mp.add(attrid, tid, v.getVioCause());
                    }
                    if (v.isNewVio()) {
                        for (int oldtid : v.getOthervalue_tids().getTids()) {
                            if(tid_predicate.test(oldtid))
                            {
                                Collection<ViolationCause> vcs = fdrg.getVioCauses(attrid, oldtid);
                                mp.add(attrid, oldtid, vcs);
                            }
                        }
                    }
                }
            }
        }
        return mp;
    }

    public AbstractSubGraph getEQclassWithUpdate(int tid, Collection<Violation> vlist) throws BleachException {
        AbstractSubGraph merged_eqclass = null;
        HashSet<ViolationCause> mergecauses = new HashSet();
        for (Violation v : vlist) {
            mergecauses.add(v.getVioCause());
            if (merged_eqclass == null) {
                merged_eqclass = getEQclassWithUpdate(v);
            } else {
                AbstractSubGraph sg = getEQclassWithUpdate(v);
                if(sg.getVcCellGroupSize() > merged_eqclass.getVcCellGroupSize()){
                    if (sg.merge(merged_eqclass)) {
                        for( ViolationCause vc : merged_eqclass.getSubgraphID()){
                            fdrg.updateVcSubGraphFromMerge(vc, sg);
                        }
                    }
                    merged_eqclass = sg;
                }
                else {

                    if (merged_eqclass.merge(sg)) {
                        for (ViolationCause vc : sg.getSubgraphID()) {
                            fdrg.updateVcSubGraphFromMerge(vc, merged_eqclass);
                        }
                    }
                }
            }
        }
        if(mergecauses.size() > 1){ // subgraph merges
            merged_eqclass.addMergeCause(tid, mergecauses);
        }
        return merged_eqclass;
    }

    private AbstractSubGraph getEQclassWithUpdate(Violation v) throws BleachException {
        return fdrg.getSubGraphWithUpdate(v);
    }

    public void addCellVcFromViolation(Violation v){
        fdrg.addCellVcFromViolation(v);
    }

    public void addCellVcFromViolations(ViolationGroup vg){
        for(int aid : vg.getAttrs()){
            Collection<Violation> vios = vg.getViolations(aid);
            vios.forEach(v->addCellVcFromViolation(v));
        }
    }


}
