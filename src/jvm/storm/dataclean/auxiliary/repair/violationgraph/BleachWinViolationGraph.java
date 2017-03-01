package storm.dataclean.auxiliary.repair.violationgraph;

import storm.dataclean.auxiliary.base.DataInWindowRecursive;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.base.ViolationCause;
import storm.dataclean.auxiliary.base.Windowing;
//import storm.dataclean.auxiliary.repair.CellVcHistoryWin;
import storm.dataclean.auxiliary.repair.cellvchistory.WinCellVcHistory;
import storm.dataclean.auxiliary.repair.subgraph.AbstractSubGraph;
import storm.dataclean.auxiliary.repair.subgraph.BleachWinSubGraph;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.ImpossibleException;
import storm.dataclean.exceptions.ViolationOldRecordsLossException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Created by yongchao on 3/4/16.
 */
public class BleachWinViolationGraph extends ViolationGraph implements Windowing{

    private DataInWindowRecursive<ViolationCause, BleachWinSubGraph> history;

    public int win_cursor; // default
    public int win_step;

    public BleachWinViolationGraph(int psize, Predicate<Integer> tp, String[] attrs, int cursor, int step){
        history = new DataInWindowRecursive(cursor, step);
        proposal_size = psize;
        tid_predicate = tp;
        cell_vc_map = new WinCellVcHistory(attrs, cursor, step);
        win_cursor = cursor;
        win_step = step;
    }

    @Override
    protected boolean containVC(ViolationCause vc) {
        return history.containsKey(vc);
    }

    @Override
    public AbstractSubGraph getSubgraph(ViolationCause vc) {
        return history.get(vc);
    }

    @Override
    protected void put(ViolationCause vc, AbstractSubGraph sg) throws ImpossibleException {
        if(history.containsKey(vc)){
            throw new ImpossibleException("VC: " + vc + " should not be in violation graph");
        }
        history.put(vc, (BleachWinSubGraph)sg);
    }

    @Override
    public void updateVcSubGraph(ViolationCause vc, AbstractSubGraph sg) {
        if(!history.containsKey(vc)){
            System.err.println("Bleach: vg update a non-existing sg");
        }
        history.update(vc, (BleachWinSubGraph)sg);

    }

    @Override
    public void updateVcSubGraphFromMerge(ViolationCause vc, AbstractSubGraph sg) {
        if(!history.containsKey(vc)){
            System.err.println("Bleach: vg update a non-existing sg");
        }

        history.update_value(vc, (BleachWinSubGraph) sg);
    }

    @Override
    public AbstractSubGraph getSubGraphWithUpdate(Violation v) throws BleachException {
        updateWindow(v.getTid());
        ViolationCause vc = v.getVioCause();
        AbstractSubGraph sg;
        if(containVC(vc)){
            sg = getSubgraph(vc);
            sg.addViolation(v);
            updateVcSubGraph(vc, sg);
            return sg;
        } else {
            if (!v.isNewVio()) {
                throw new ViolationOldRecordsLossException(v);
            } else {
                sg = new BleachWinSubGraph(tid_predicate, proposal_size, cell_vc_map, v, win_cursor, win_step);
                put(vc, sg);
                return sg;
            }
        }
    }

    @Override
    public void delete_rule(int rid) {
        cell_vc_map.delete_rule(rid);
        HashMap<ViolationCause, BleachWinSubGraph> new_first_history = new HashMap<>();
        HashMap<ViolationCause, BleachWinSubGraph> new_second_history = new HashMap<>();
        for(Map.Entry<ViolationCause, BleachWinSubGraph> entry : history.getFirst_history().entrySet())
        {
            ViolationCause vc = entry.getKey();
            if(new_first_history.containsKey(vc)){
                continue;
            } else {
                if (vc.getRuleid() != rid) {
                    Collection<AbstractSubGraph> sgs = entry.getValue().delete_rule(rid);
                    if(sgs == null){
                        AbstractSubGraph sg = entry.getValue();
                        for (ViolationCause nvc : sg.getSubgraphID()) {
                            if(history.getSecond_history().containsKey(nvc))
                            {
                                new_second_history.put(nvc, (BleachWinSubGraph) sg);
                            } else
                            {
                                new_first_history.put(nvc, (BleachWinSubGraph) sg);
                            }
                        }
                        continue;
                    } else {
                        for(AbstractSubGraph sg : sgs){
                            for(ViolationCause nvc : sg.getSubgraphID()){
                                if(history.getSecond_history().containsKey(nvc))
                                {
                                    new_second_history.put(nvc, (BleachWinSubGraph) sg);
                                } else {
                                    new_first_history.put(nvc, (BleachWinSubGraph) sg);
                                }
                            }
                        }
                    }
                }
            }
        }

        for(Map.Entry<ViolationCause, BleachWinSubGraph> entry : history.getSecond_history().entrySet())
        {
            ViolationCause vc = entry.getKey();
            if(new_second_history.containsKey(vc)){
                continue;
            } else {
                if (vc.getRuleid() != rid) {
                    Collection<AbstractSubGraph> sgs = entry.getValue().delete_rule(rid);
                    if(sgs == null){
                        AbstractSubGraph sg = entry.getValue();
                        for (ViolationCause nvc : sg.getSubgraphID()) {
                            if(history.getFirst_history().containsKey(nvc)) {
                                new_first_history.put(nvc, (BleachWinSubGraph) sg);
                            }
                            else{
                                new_second_history.put(nvc, (BleachWinSubGraph) sg);
                            }
                        }
                        continue;
                    } else {
                        for(AbstractSubGraph sg : sgs){
                            for(ViolationCause nvc : sg.getSubgraphID()){
                                if(history.getFirst_history().containsKey(nvc))
                                {
                                    new_first_history.put(nvc, (BleachWinSubGraph) sg);
                                } else {
                                    new_second_history.put(nvc, (BleachWinSubGraph) sg);
                                }

                            }
                        }
                    }
                }
            }
        }
        history.resetFirst_history(new_first_history);
        history.resetSecond_history(new_second_history);
    }

    @Override
    public void print_log() {
        System.err.println(DEBUG_PREFIX + " violationgraphwinbleach has " + history.size() + " vcs, first size=" +history.first_history.size()+", second_size="+history.second_history.size());


        HashMap<Integer, Integer> rule_count = new HashMap<>();
        for(ViolationCause vc : history.keySet()){
            if(rule_count.containsKey(vc.getRuleid())){
                rule_count.put(vc.getRuleid(), rule_count.get(vc.getRuleid())+1);
            } else{
                rule_count.put(vc.getRuleid(),1);
            }
        }

        for(Map.Entry<Integer,Integer> entry : rule_count.entrySet()){
            System.out.println("rule "+entry.getKey()+", count="+entry.getValue());
        }

        HashSet<BleachWinSubGraph> sg_set = new HashSet(history.values());

        System.out.println("number of sgs in print_log2:" + sg_set.size());

        int tmp_count = 0;
        for(BleachWinSubGraph sg_tmp : sg_set){
            if(sg_tmp.isNevermerged()){
                tmp_count++;
            }
        }

        System.out.println("number of nevermerged sgs in print_log2:" + tmp_count);


//        System.err.println(DEBUG_PREFIX + " violationgraphwinbleach has " + (new HashSet(history.values())).size()+ " subgraphs");
//        System.err.println(DEBUG_PREFIX + " violationgraphwinbleach has at most " +
//                history.values().stream().mapToInt(x->x.getSuperCellNum()).max().getAsInt() + " super cells in a subgraph");
//        System.err.println(DEBUG_PREFIX + " violationgraphwinbleach has at most " +
//                history.values().stream().mapToInt(x->x.getSubgraphID().size()).max().getAsInt() + " vcs in a subgraph");

    }

    @Override
    public boolean updateWindow(int tid) {
        if(tid > win_cursor){
//            print_log();
//            long time_start = System.currentTimeMillis();
            win_cursor += win_step;
            System.err.println("Bleach: BasicWinViolationGraph: debug move window tid=" + tid);
            history.updateWindow(tid);
           ((WinCellVcHistory)cell_vc_map).updateWindow(tid);
            HashMap<ViolationCause, BleachWinSubGraph> new_first_history = new HashMap<>();
//            long time_split_start = System.currentTimeMillis();
            for (Map.Entry<ViolationCause, BleachWinSubGraph> entry : history.getFirst_history().entrySet()) {
                ViolationCause vc = entry.getKey();
                if (!new_first_history.containsKey(vc)) {
                    Collection<AbstractSubGraph> sgs = entry.getValue().split(false);
                    if (sgs == null) {
                        AbstractSubGraph sg = entry.getValue();
                        for (ViolationCause nvc : sg.getSubgraphID(true)) {
                            new_first_history.put(nvc, (BleachWinSubGraph) sg);
                        }
                    } else {
                        for (AbstractSubGraph sg : sgs) {
                            for (ViolationCause nvc : sg.getSubgraphID(true)) {
                                new_first_history.put(nvc, (BleachWinSubGraph) sg);
                            }
                        }

                    }
                }
            }
//            long time_split_end = System.currentTimeMillis();
            history.resetFirst_history(new_first_history);
//            long time_end = System.currentTimeMillis();
//            System.out.println("update window time: " + (time_end - time_start) + " ms");
//            System.out.println("update window, split sgs time: " + (time_split_end - time_split_start) + " ms");
            return true;
        }
        else {
            return false;

        }
    }
}
