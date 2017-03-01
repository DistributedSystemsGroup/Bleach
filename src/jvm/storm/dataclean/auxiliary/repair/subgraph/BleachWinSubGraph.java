package storm.dataclean.auxiliary.repair.subgraph;


import storm.dataclean.auxiliary.base.*;
import storm.dataclean.auxiliary.repair.cellvchistory.AbstractCellVcHistory;
import storm.dataclean.auxiliary.repair.BleachVcCellGroup;
import storm.dataclean.auxiliary.repair.mergeCausehistory.MergeHistory;
import storm.dataclean.auxiliary.repair.mergeCausehistory.BleachWinMergeHistory;
import storm.dataclean.auxiliary.repair.overlap.BleachOverlpCells;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.ViolationOldRecordsLossException;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by tian on 19/11/2015.
 * This is a version without dynamic processing (rule evolution).
 */
public class BleachWinSubGraph extends AbstractSubGraph implements Windowing{

    public static int VALUE_LIMIT = 1000;

    public HashMap<Object, ComWinSuperCell> value_cell_map;

    public BleachVcCellGroup vc_cells_map;

    public int win_cursor;
    public int win_step;

    public BleachOverlpCells olcells;

    public boolean nevermerged;

    public BleachWinSubGraph(Predicate<Integer> tp, int p, AbstractCellVcHistory cvm, Violation v, int cursor, int step) throws BleachException { // v must be a new violation where newVio is true
        if (!v.isNewVio()) {
            throw new ViolationOldRecordsLossException(v);
        }
        nevermerged = true;
        value_cell_map = new HashMap();
        vc_cells_map = new BleachVcCellGroup(win_cursor, win_step);
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
        mch = new BleachWinMergeHistory();
        olcells = new BleachOverlpCells(win_cursor, win_step);

    }

    public BleachWinSubGraph(Predicate<Integer> tp, int p, AbstractCellVcHistory cvm, int aid,
                             BleachVcCellGroup parent_vc_cells_map,
                             int cursor, int step,
                             Collection<ViolationCause> sid,
                             MergeHistory n_merge_causes,
                             BleachOverlpCells n_olp_cell){
        tid_predicate = tp;
        proposal_size = p;
        cell_vc_map = cvm;
        topObject = new ObjectWithCount[proposal_size];
        attrid = aid;
        win_cursor = cursor;
        win_step = step;
        vc_cells_map = new BleachVcCellGroup(win_cursor, win_step);
        sid.forEach(x -> vc_cells_map.put(x, parent_vc_cells_map.get(x), parent_vc_cells_map.containsKeyInFirst(x)));
        olcells = n_olp_cell;
        mch = n_merge_causes;
        rebuild_value_cell_map();
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
    protected void addCell(Object value, int tid, ViolationCause vc) throws BleachException {

        if(!nevermerged) {
            if (tid_predicate.test(tid)) {
                if (vc_cells_map.containsKey(vc)) {
                    HashMap<Object, ComWinSuperCell> cells = vc_cells_map.get(vc);
                    if (cells.containsKey(value)) {
                        ComWinSuperCell sc = cells.get(value);
                        sc.add(tid);
                    } else {
                        cells.put(value, new ComWinSuperCell(tid, win_cursor, win_step));
                    }
                    vc_cells_map.update(vc, cells);
                } else {
                    HashMap<Object, ComWinSuperCell> cells = new HashMap<>();
                    cells.put(value, new ComWinSuperCell(tid, win_cursor, win_step));
                    vc_cells_map.put(vc, cells);
                }
                ComWinSuperCell sc;
                if (value_cell_map.containsKey(value)) {
                    sc = value_cell_map.get(value);
                    if (!sc.add(tid)) {
                        olcells.addFromCellVcHistory(tid, value, cell_vc_map.getVioCauses(attrid, tid));
                    }
                } else {
                    sc = new ComWinSuperCell(tid, win_cursor, win_step);
                    value_cell_map.put(value, sc);
                }

                updateRank(value, sc);
            } else {
                if (!vc_cells_map.containsKey(vc)) {
                    vc_cells_map.put(vc, new HashMap());
                } else {
                    vc_cells_map.update(vc);
                }
            }
        } else {
            if (tid_predicate.test(tid)) {
                ComWinSuperCell sc;
                if (value_cell_map.containsKey(value)) {
                    sc = value_cell_map.get(value);
                    sc.add(tid);
                } else {
                    sc = new ComWinSuperCell(tid, win_cursor, win_step);
                    value_cell_map.put(value, sc);
                }
                vc_cells_map.update_singleton();
                updateRank(value, sc);
            } else {
                vc_cells_map.update_singleton();
            }
        }

    }

    @Override
    protected void addCells(Object value, AbstractSuperCell tmp_tids, ViolationCause vc) throws BleachException {

        if(!nevermerged) {
            AbstractSuperCell tids_vc = tmp_tids.copy();
            AbstractSuperCell tids_value = tmp_tids.copy();

            if (tids_vc.filter(tid_predicate)) {
                tids_value.filter(tid_predicate);
                if (value_cell_map.containsKey(value)) {
                    ComWinSuperCell existing_tids = value_cell_map.get(value);
                    existing_tids.addAll((WinSuperCell)tids_vc);
                    updateRank(value, existing_tids);
                } else {
                    value_cell_map.put(value, new ComWinSuperCell(win_cursor, win_step, (WinSuperCell) tids_vc));
                    updateRank(value, tids_vc);
                }

                vc_cells_map.putIfAbsent(vc, new HashMap());

                HashMap<Object, ComWinSuperCell> vc_records = vc_cells_map.get(vc);
                if (vc_records.containsKey(value)) {
                    ComWinSuperCell ex_tids = vc_records.get(value);
                    ex_tids.addAll((WinSuperCell) tids_value);
                } else {
                    vc_records.put(value, new ComWinSuperCell(win_cursor, win_step, (WinSuperCell) tids_value));
                }
                vc_cells_map.update(vc, vc_records);

            } else {
                if (!vc_cells_map.containsKey(vc)) {
                    vc_cells_map.put(vc, new HashMap());
                } else {
                    vc_cells_map.update(vc);
                }
            }
        } else {
            AbstractSuperCell tids = tmp_tids.copy();
            if (tids.filter(tid_predicate)) {
                if(value_cell_map.containsKey(value)){
                    ComWinSuperCell ex_tids = value_cell_map.get(value);
                    ex_tids.addAll((WinSuperCell)tids);
                    updateRank(value, ex_tids);
                } else {
                    value_cell_map.put(value, new ComWinSuperCell(win_cursor, win_step, (WinSuperCell) tids));
                    updateRank(value, tids);
                }
                vc_cells_map.update_singleton();
            } else {
                vc_cells_map.update_singleton();
            }
        }
    }

    private BleachVcCellGroup get_vc_cells_map(){
        return vc_cells_map;
    }

    private HashMap<Object, ComWinSuperCell> get_value_cell_map(){
        return value_cell_map;
    }

    @Override
    public boolean merge(AbstractSubGraph p) {
        if (p != this && p != null) {
            if(nevermerged) {
                nevermerged = false;
                value_cell_map = copy_value_cell_map();
            }
            merge_vc_cells_map(p);
            merge_value_cell_map(p);
            merge_mergehistory(p);
            return true;
        }
        return false;
    }

    private HashMap<Object, ComWinSuperCell> copy_value_cell_map(){
        HashMap<Object, ComWinSuperCell> other_value_cell_map = new HashMap();
        for(Map.Entry<Object, ComWinSuperCell> entry : value_cell_map.entrySet()){
            other_value_cell_map.put(entry.getKey(), entry.getValue().copy(true));
        }
        return other_value_cell_map;
    }

    @Override
    protected void merge_vc_cells_map(AbstractSubGraph p) {
        vc_cells_map.putAll(((BleachWinSubGraph) p).get_vc_cells_map());
    }


    // Is this OK? not sure...
    @Override
    protected void merge_value_cell_map(AbstractSubGraph p) {
        HashMap<Object, ComWinSuperCell> other_value_cell_map = ((BleachWinSubGraph)p).get_value_cell_map();
        // merging existing overlap cells, but note that there will be new overlap cells to be discovered.
        olcells.merge(((BleachWinSubGraph)p).getOlcells());

        for(Map.Entry<Object, ComWinSuperCell> entry : other_value_cell_map.entrySet()){
            Object v = entry.getKey();
            ComWinSuperCell sc;
            if(value_cell_map.containsKey(v)){
                sc = value_cell_map.get(v);
                Collection<Integer> hinge_cell = sc.merge(entry.getValue());
                sc.reduceDuplicate(hinge_cell.size());
                for (int overlapping_tid : hinge_cell) {
                    olcells.addFromCellVcHistory(overlapping_tid, entry.getKey(), cell_vc_map.getVioCauses(attrid, overlapping_tid));
                }
            } else {
                sc = entry.getValue();
                value_cell_map.put(v, sc.copy()); // important. Here add a copied sc to prevent vc_cells_map and value_cell_map sharing the same scs.
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
            vc_cells_map.delete_rule(deleted_rid);
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected AbstractSubGraph getSplitSubGraph(Collection<ViolationCause> sid, MergeHistory n_merge_causes) {
        BleachOverlpCells n_olp_cells = olcells.getSubsetbySid(sid);
        return new BleachWinSubGraph(tid_predicate, proposal_size, cell_vc_map,
                attrid, vc_cells_map, win_cursor, win_step, sid, n_merge_causes, n_olp_cells);
    }

    @Override
    public Collection<AbstractSubGraph> delete_rule(int rid){
        mch.delete_rule(rid);
        olcells.delete_rule(rid);
        boolean affected = update_vc_cells_map(rid);
        if(affected)
        {
            return split(affected);
        } else {
            return null;
        }
    }

    @Override
    public Collection<AbstractSubGraph> split(boolean deleterule){
        ArrayList<Collection<ViolationCause>> sids = new ArrayList();
        Collection<ViolationCause> vcs = new HashSet(getSubgraphID(true));
        for(Collection<ViolationCause> overlap : mch.getMergeCauses()){
            Collection<ViolationCause> sid = new HashSet(overlap);
            sid.forEach(x->vcs.remove(x));
            ArrayList<Integer> deleted_index = new ArrayList();
            for(int index = 0; index < sids.size(); index++){
                Collection<ViolationCause> sub_sid = sids.get(index);
                if(!Collections.disjoint(sub_sid, sid)){
                    deleted_index.add(index);
                }
            }

            for(int index_index = (deleted_index.size()-1); index_index >= 0; index_index--){
                int index = deleted_index.get(index_index);
                Collection<ViolationCause> sub_sid = sids.get(index);
                if(sub_sid.size() > sid.size()){
                    sub_sid.addAll(sid);
                    sid = sub_sid;
                } else {
                    sid.addAll(sub_sid);
                }
                sids.remove(index);
            }
            sids.add(sid);

        }
        for (ViolationCause vc : vcs) {
            Collection<ViolationCause> sid = new HashSet();
            sid.add(vc);
            sids.add(sid);
        }

        if(sids.size() == 1) {
            if(deleterule)
            {
                rebuild_value_cell_map();
            }
            return null;
        } else if(sids.size() > 1){
            Collection<AbstractSubGraph> sgs = new ArrayList();
            for(Collection<ViolationCause> sid : sids){
                MergeHistory n_merge_causes = mch.getSubsetbySid(sid);
                sgs.add(getSplitSubGraph(sid, n_merge_causes));

            }
            return sgs;
        } else {
            return null;
        }
    }

    @Override
    protected void rebuild_value_cell_map() {
        topObject = new ObjectWithCount[proposal_size];
        value_cell_map = new HashMap();
        // accumulate from vc_cells_map
        for (HashMap<Object, ComWinSuperCell> map : vc_cells_map.values()) {
            for (Map.Entry<Object, ComWinSuperCell> entry : map.entrySet()) {
                ComWinSuperCell sc;
                if (value_cell_map.containsKey(entry.getKey())) {
                    sc = value_cell_map.get(entry.getKey());
                    sc.merge(entry.getValue());
                } else {
                    sc = entry.getValue().copy();
                    value_cell_map.put(entry.getKey(), sc);
                }
            }
        }

        // reduce from overlapping_cells
        for(Map.Entry<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> entry : olcells.getRecords().entrySet()){
            int duplicated_count = entry.getKey().size() - 1;
            for(Map.Entry<Object, ComWinSuperCell> entry2 : entry.getValue().entrySet()){
                Object v = entry2.getKey();
                value_cell_map.get(v).reduceDuplicate(duplicated_count * entry2.getValue().size());
            }
        }
        for(Map.Entry<Object, ComWinSuperCell> entry : value_cell_map.entrySet()){
            updateRank(entry.getKey(),entry.getValue());
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


    public BleachOverlpCells getOlcells(){
        return olcells;
    }


    @Override
    public boolean updateWindow(int tid) {
        if(tid > win_cursor) {
            Collection<ViolationCause> deleted_vcs = vc_cells_map.updateWindowWithReturn(tid);
            olcells.updateWindow(tid, deleted_vcs);
            ((BleachWinMergeHistory) mch).updateWindow(deleted_vcs);
            if (deleted_vcs.size() > 0) {
                rebuild_value_cell_map();
            }
            win_cursor += win_step;
            if(value_cell_map.size() > VALUE_LIMIT){
                reset();
            }
            rebuild_topObject();
            return true;
        } else {
            return false;
        }
    }

    // as time goes, when there are too many values in sg, we reset sg, only considering cells within the current window
    private void reset(){
        // debug use
        System.err.println("BleachWinSubGraph: reset because of too many values");
        vc_cells_map.reset();
        olcells.reset();
        rebuild_value_cell_map();
    }

    @Override
    public int hashCode(){
        return vc_cells_map.hashCode();
    }

    @Override
    public boolean equals(Object o){
        BleachWinSubGraph o2 = (BleachWinSubGraph)o;
        if(o2.get_vc_cells_map().equals(get_vc_cells_map())){
            return true;
        } else {
            return false;
        }

    }


    public boolean isNevermerged(){
        return nevermerged;
    }

}
