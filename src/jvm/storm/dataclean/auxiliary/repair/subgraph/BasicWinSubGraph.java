package storm.dataclean.auxiliary.repair.subgraph;

import storm.dataclean.auxiliary.base.*;
import storm.dataclean.auxiliary.repair.cellvchistory.AbstractCellVcHistory;
import storm.dataclean.auxiliary.repair.mergeCausehistory.BasicWinMergeHistory;
import storm.dataclean.auxiliary.repair.mergeCausehistory.MergeHistory;
import storm.dataclean.exceptions.ImpossibleException;
import storm.dataclean.exceptions.ViolationOldRecordsLossException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Created by tian on 19/11/2015.
 * This is a version without dynamic processing (rule evolution).
 */
public class BasicWinSubGraph extends AbstractSubGraph implements Windowing{


    public DataInWindowRecursive<Object, WinSuperCell> value_cell_map;
    public DataInWindowRecursive<ViolationCause, DataInWindowRecursive<Object, WinSuperCell>> vc_cells_map;

    public int win_cursor;
    public int win_step;

    public boolean nevermerged; // if a sg is never merged, value_cell_map and vc_cells_map may share the same value-supercell map

    public BasicWinSubGraph(Predicate<Integer> tp, int p, AbstractCellVcHistory cvm, Violation v, int cursor, int step) throws ViolationOldRecordsLossException, ImpossibleException { // v must be a new violation where newVio is true
        if (!v.isNewVio()) {
            throw new ViolationOldRecordsLossException(v);
        }
        nevermerged = true;
        value_cell_map = new DataInWindowRecursive(win_cursor, win_step);
        vc_cells_map = new DataInWindowRecursive(win_cursor, win_step);
        vc_cells_map.put(v.getVioCause(), value_cell_map);
        tid_predicate = tp;
        proposal_size = p;
        cell_vc_map = cvm;
        win_cursor = cursor;
        win_step = step;
        topObject = new ObjectWithCount[proposal_size];
        attrid = v.getRattr_index();
        addCell(v.getValue(), v.getTid(), v.getVioCause());  // addFromCellVcHistory current tuple's id and its value
        addCells(v.getOthervalue(), v.getOthervalue_tids(), v.getVioCause()); // addFromCellVcHistory old tids which will be filterred
        mch = new BasicWinMergeHistory(win_cursor, win_step);
    }

    public BasicWinSubGraph(Predicate<Integer> tp, int p, AbstractCellVcHistory cvm, int aid,
                            DataInWindowRecursive<ViolationCause, DataInWindowRecursive<Object, WinSuperCell>> parent_vc_cells_map,
                            int cursor, int step,
                            Collection<ViolationCause> sid,
                            MergeHistory n_merge_causes){

        nevermerged = false;
        tid_predicate = tp;
        proposal_size = p;
        cell_vc_map = cvm;
        topObject = new ObjectWithCount[proposal_size];
        attrid = aid;
        win_cursor = cursor;
        win_step = step;
        vc_cells_map = new DataInWindowRecursive(win_cursor, win_step);
        sid.forEach(x -> vc_cells_map.put(x, parent_vc_cells_map.get(x), parent_vc_cells_map.containsKeyInFirst(x)));
        rebuild_value_cell_map();
        mch = n_merge_causes;
    }

    @Override
    public Collection<ViolationCause> getSubgraphID() {
        if(mch.size() == 0){
            return vc_cells_map.keySet();
        } else {
            return mch.getVcs();
        }
    }

    @Override
    public Collection<ViolationCause> getSubgraphID(boolean updating) {
        if(updating){
            HashSet<ViolationCause> ids = new HashSet<>();
            ids.addAll(vc_cells_map.keySet());
            ids.addAll(mch.getVcs());
            return ids;
        } else {
            return getSubgraphID();
        }
    }

    @Override
    public int getVcCellGroupSize() {
        return vc_cells_map.size();
    }

    @Override
    protected void addCell(Object value, int tid, ViolationCause vc) throws ImpossibleException {
        if(!nevermerged) {
            if (tid_predicate.test(tid)) {
                if (vc_cells_map.containsKey(vc)) {
                    DataInWindowRecursive<Object, WinSuperCell> cells = vc_cells_map.get(vc);
                    if (cells.containsKey(value)) {
                        WinSuperCell sc = cells.get(value);
                        sc.add(tid);
                        cells.update(value, sc);
                    } else {
                        cells.put(value, new WinSuperCell(tid, win_cursor, win_step));
                    }
                    vc_cells_map.update(vc, cells);
                } else {
                    DataInWindowRecursive<Object, WinSuperCell> cells = new DataInWindowRecursive(win_cursor, win_step);
                    cells.put(value, new WinSuperCell(tid, win_cursor, win_step));
                    vc_cells_map.put(vc, cells);
                }
                WinSuperCell sc;
                if (value_cell_map.containsKey(value)) {
                    sc = value_cell_map.get(value);
                    sc.add(tid);
                    value_cell_map.update(value, sc);
                } else {
                    sc = new WinSuperCell(tid, win_cursor, win_step);
                    value_cell_map.put(value, sc);
                }
                updateRank(value, sc);
            } else {
                if (!vc_cells_map.containsKey(vc)) {
                    vc_cells_map.put(vc, new DataInWindowRecursive(win_cursor, win_step));
                } else {
                    vc_cells_map.update(vc);
                }
            }
        } else { // here is very tricky that if there is only one vc-cell group, value_cell_map is just that vc-cell group!!!
            if (tid_predicate.test(tid)) {
                WinSuperCell sc;
                DataInWindowRecursive<Object, WinSuperCell> cells = value_cell_map;
                if (cells.containsKey(value)) {
                    sc = cells.get(value);
                    sc.add(tid);
                    cells.update(value, sc);
                } else {
                    sc = new WinSuperCell(tid, win_cursor, win_step);
                    cells.put(value, sc);
                }
                vc_cells_map.update_singleton();
                updateRank(value, sc);
            } else {
                vc_cells_map.update_singleton();

            }



        }
    }

    @Override
    protected void addCells(Object value, AbstractSuperCell tmp_tids, ViolationCause vc) throws ImpossibleException {
        if(!nevermerged) {
            AbstractSuperCell tids_vc = tmp_tids.copy();
            AbstractSuperCell tids_value = tmp_tids.copy();
            if (tids_vc.filter(tid_predicate)) {
                if (value_cell_map.containsKey(value)) {
                    WinSuperCell existing_tids = value_cell_map.get(value);
                    existing_tids.merge(tids_vc);
                    value_cell_map.update(value, existing_tids);
                    updateRank(value, existing_tids);
                } else {
                    value_cell_map.put(value, (WinSuperCell) tids_vc);
                    updateRank(value, tids_vc);
                }

                vc_cells_map.putIfAbsent(vc, new DataInWindowRecursive(win_cursor, win_step));

                DataInWindowRecursive<Object, WinSuperCell> vc_records = vc_cells_map.get(vc);
                if (vc_records.containsKey(value)) {
                    WinSuperCell ex_tids = vc_records.get(value);
                    ex_tids.merge(tids_value);
                    vc_records.update(value, ex_tids);
                } else {
                    vc_records.put(value, (WinSuperCell) tids_value);
                }
                vc_cells_map.update(vc, vc_records);

            } else {
                if (!vc_cells_map.containsKey(vc)) {
                    vc_cells_map.put(vc, new DataInWindowRecursive(win_cursor, win_step));
                } else {
                    vc_cells_map.update(vc);
                }
            }
        } else {
            AbstractSuperCell tids = tmp_tids.copy();
            if (tids.filter(tid_predicate)) {
                if(value_cell_map.containsKey(value)){
                    WinSuperCell ex_tids = value_cell_map.get(value);
                    ex_tids.merge(tids);
                    value_cell_map.update(value, ex_tids);
                    updateRank(value, ex_tids);
                } else {
                    value_cell_map.put(value, (WinSuperCell) tids);
                    updateRank(value, tids);
                }
                vc_cells_map.update_singleton();
            } else {
                vc_cells_map.update_singleton();
            }
        }

    }

    @Override
    public boolean merge(AbstractSubGraph p) {
        if (p != this && p != null) {
            if(nevermerged) {
                nevermerged = false;
                value_cell_map = copy_value_cell_map(); // avoid value_cell_map and vc_cell_group using the same map
            }

            merge_vc_cells_map(p);
            merge_value_cell_map(p);
            merge_mergehistory(p);
            return true;
        }
        return false;
    }

    private DataInWindowRecursive<Object, WinSuperCell> copy_value_cell_map(){
        DataInWindowRecursive<Object, WinSuperCell> other_value_cell_map = new DataInWindowRecursive(value_cell_map.getWin_cursor(), value_cell_map.getWin_step());
        Map<Object, WinSuperCell> firsthistory = value_cell_map.getFirst_history();
        Map<Object, WinSuperCell> secondhistory = value_cell_map.getSecond_history();
        HashMap<Object, WinSuperCell> new_firsthistory = new HashMap();
        HashMap<Object, WinSuperCell> new_secondhistory = new HashMap();
        for(Map.Entry<Object, WinSuperCell> entry : firsthistory.entrySet()){
            new_firsthistory.put(entry.getKey(), entry.getValue().copy(true));
        }
        for(Map.Entry<Object, WinSuperCell> entry : secondhistory.entrySet()){
            new_secondhistory.put(entry.getKey(), entry.getValue().copy(true));
        }
        other_value_cell_map.resetFirst_history(new_firsthistory);
        other_value_cell_map.resetSecond_history(new_secondhistory);
        return other_value_cell_map;
    }


    private DataInWindowRecursive<ViolationCause, DataInWindowRecursive<Object, WinSuperCell>> get_vc_cells_map(){
        return vc_cells_map;
    }

    private DataInWindowRecursive<Object, WinSuperCell> get_value_cell_map(){
        return value_cell_map;
    }

    @Override
    protected void merge_vc_cells_map(AbstractSubGraph p) {
        vc_cells_map.putAll(((BasicWinSubGraph) p).get_vc_cells_map());
    }

    @Override
    protected void merge_value_cell_map(AbstractSubGraph p) {
        DataInWindowRecursive<Object, WinSuperCell> other_value_cell_map = ((BasicWinSubGraph)p).get_value_cell_map();
        for(Map.Entry<Object, WinSuperCell> entry : other_value_cell_map.getFirst_history().entrySet() ){
            Object v = entry.getKey();
            WinSuperCell sc;
            if(value_cell_map.containsKey(v)){
                sc = value_cell_map.get(v);
                sc.merge(entry.getValue());
                value_cell_map.update_value(v, sc); // first is still first, second is still second
            } else {
                sc = entry.getValue();
                value_cell_map.putFirst(v, sc); // can only be first
            }
            updateRank(v, sc);
        }

        for(Map.Entry<Object, WinSuperCell> entry : other_value_cell_map.getSecond_history().entrySet() ){
            Object v = entry.getKey();
            WinSuperCell sc;
            if(value_cell_map.containsKey(v)){
                sc = value_cell_map.get(v);
                sc.merge(entry.getValue());
                value_cell_map.update(v, sc); // must be in second
            } else {
                sc = entry.getValue();
                value_cell_map.put(v, sc); // must be in second
            }
            updateRank(v, sc);
        }
    }

    @Override
    public boolean update_vc_cells_map(int deleted_rid) {
        boolean found = false;
        for(ViolationCause vc : vc_cells_map.keySet()){
            if(vc.getRuleid()==deleted_rid){
                found = true;
                break;
            }
        }
        if(found){
            Map<ViolationCause, DataInWindowRecursive<Object, WinSuperCell>> new_vc_cells_map = new HashMap();
            vc_cells_map.entrySet().stream().filter(entry -> entry.getKey().getRuleid() != deleted_rid).forEach(entry -> {
                new_vc_cells_map.put(entry.getKey(), entry.getValue());
            });
            vc_cells_map = new DataInWindowRecursive(win_cursor, win_step);
            vc_cells_map.putAll(new_vc_cells_map);
            return true;
        } else {
            return false;
        }

    }

    @Override
    protected AbstractSubGraph getSplitSubGraph(Collection<ViolationCause> sid, MergeHistory n_merge_causes) {
        return new BasicWinSubGraph(tid_predicate, proposal_size, cell_vc_map,
                attrid, vc_cells_map, win_cursor, win_step, sid, n_merge_causes);
    }

    @Override
    protected void rebuild_value_cell_map() {
        topObject = new ObjectWithCount[proposal_size];
        value_cell_map = new DataInWindowRecursive(win_cursor, win_step);

        for(DataInWindowRecursive<Object, WinSuperCell> map : vc_cells_map.getFirst_history().values()){
            for(Map.Entry<Object, WinSuperCell> entry : map.entrySet()){
                AbstractSuperCell sc;
                if(value_cell_map.containsKey(entry.getKey())){
                    sc = value_cell_map.get(entry.getKey());
                    sc.merge(entry.getValue());
                } else {
                    sc = entry.getValue().copy();
                    value_cell_map.putFirst(entry.getKey(), (WinSuperCell)sc);
                }
                updateRank(entry.getKey(), sc);
            }
        }
        for(DataInWindowRecursive<Object, WinSuperCell> map : vc_cells_map.getSecond_history().values()){
            for(Map.Entry<Object, WinSuperCell> entry : map.entrySet()){
                AbstractSuperCell sc;
                if(value_cell_map.containsKey(entry.getKey())){
                    sc = value_cell_map.get(entry.getKey());
                    sc.merge(entry.getValue());
                    value_cell_map.update(entry.getKey(), (WinSuperCell) sc);
                } else {
                    sc = entry.getValue().copy();
                    value_cell_map.put(entry.getKey(), (WinSuperCell) sc);
                }
                updateRank(entry.getKey(), sc);
            }
        }

    }

    @Override
    protected void rebuild_topObject() {
        topObject = new ObjectWithCount[proposal_size];
        value_cell_map.entrySet().forEach(x->updateRank(x.getKey(),x.getValue()));
    }


    @Override
    public int getSuperCellNum() {
        return value_cell_map.size();
    }


    /**
     * TODO?
     */
    @Override
    public boolean updateWindow(int tid) {
        if(tid > win_cursor) {
            value_cell_map.updateWindow(tid);
            vc_cells_map.updateWindow(tid);
            ((BasicWinMergeHistory) mch).updateWindow(tid);
            rebuild_topObject();
            win_cursor += win_step;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString(){
//        String result = "sg: " + getSubgraphID() + "\n first_history:" + vc_cells_map.getFirst_history() + "\n second_history:" + vc_cells_map.getSecond_history();
//        String result = "sg: " + getSubgraphID() + ", value candidates:";
//        for (ObjectWithCount objectWithCount : topObject) {
//            result += objectWithCount + ",";
//        }
//        return "sg: " + getSubgraphID() + ", value candidates:" + topObject;

        String result = "sg: " + getSubgraphID() + ", vc_cell_map: " + vc_cells_map;

        return result;
    }

}
