package storm.dataclean.component.bolt.repair;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.guava.collect.ArrayListMultimap;
import org.apache.storm.guava.collect.Multimap;
import storm.dataclean.auxiliary.GlobalConstant;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposalGroup;
import storm.dataclean.util.BleachConfig;

import java.util.Collection;
import java.util.Map;

/**
 * Created by tian on 19/11/2015.
 */
public class RepairCoordinatorBolt extends BaseRichBolt {

    public String REPAIR_COORDINATE_STREAM_ID;
    OutputCollector _collector;
    public int partial_level;
    public String rworker_id;

    public Multimap<Integer, MergeEQClassProposalGroup> states;

    public RepairCoordinatorBolt(BleachConfig bconfig){
        rworker_id = bconfig.get(BleachConfig.BOLT_REPAIR_WORKER_ID);
//        REPAIR_COORDINATE_STREAM_ID = GlobalConstant.REPAIR_FROM_COORDINATE_STREAM;
        REPAIR_COORDINATE_STREAM_ID = GlobalConstant.REPAIR_COORDINATION_STREAM;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        partial_level = context.getComponentTasks(rworker_id).size();
        states = ArrayListMultimap.create();
    }

    @Override
    public void execute(Tuple tuple) {
        MergeEQClassProposalGroup mp = (MergeEQClassProposalGroup) tuple.getValueByField("merge-propose");
        int tid = mp.getTid();
        states.put(tid, mp);
        Collection<MergeEQClassProposalGroup> mplist = states.get(tid);
        if(mplist.size() == partial_level){
            MergeEQClassProposalGroup merged = null;
            for(MergeEQClassProposalGroup m : mplist){
                if(merged == null){
                    merged = m;
                } else {
                    merged.merge(m);
                }
            }
            states.removeAll(tid);
            _collector.emit(REPAIR_COORDINATE_STREAM_ID, tuple, new Values(merged));
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(REPAIR_COORDINATE_STREAM_ID, new Fields("merged_propose"));
    }

}
