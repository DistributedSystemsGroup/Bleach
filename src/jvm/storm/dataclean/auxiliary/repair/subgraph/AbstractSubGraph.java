package storm.dataclean.auxiliary.repair.subgraph;

import storm.dataclean.auxiliary.base.AbstractSuperCell;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.base.ViolationCause;
import storm.dataclean.auxiliary.repair.cellvchistory.AbstractCellVcHistory;
import storm.dataclean.auxiliary.repair.mergeCausehistory.MergeHistory;
import storm.dataclean.exceptions.BleachException;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by yongchao on 3/3/16.
 */
public abstract class AbstractSubGraph {

    public int proposal_size;
    public ObjectWithCount[] topObject;
    public Predicate<Integer> tid_predicate;
    public AbstractCellVcHistory cell_vc_map;
    public int attrid;  // let's first make this running, When there are two streams/tables, things will be much different.

    public MergeHistory mch;

    public abstract Collection<ViolationCause> getSubgraphID();

    public abstract Collection<ViolationCause> getSubgraphID(boolean updating);

    public abstract int getVcCellGroupSize();

    @Override
    public String toString() {
        return "Bleach: AbstractSubGraph tostring to be defined";
    }

    public void addViolation(Violation v) throws BleachException {


        if (v.isNewVio()) {  // this happens when ex. win1(-1,-1),win2(1,1),win3(-1)...
            addCell(v.getValue(), v.getTid(), v.getVioCause());
            addCells(v.getOthervalue(), v.getOthervalue_tids(), v.getVioCause());
        } else
        {
            addCell(v.getValue(), v.getTid(), v.getVioCause());
        }
    }



    protected abstract void addCell(Object value, int tid, ViolationCause vc) throws BleachException;

    protected abstract void addCells(Object value, AbstractSuperCell tids, ViolationCause vc) throws BleachException;

    public int getAttrid() {
        return attrid;
    }


    protected MergeHistory getMergeCausesHistory(){
        return mch;
    }

    /**
     * Attention that after merge, mergecause must be added, by two addMergeCause method.
     * Otherwise, a subgraph can not be splittable.
     * @param p
     * @return true if they merge
     */
    public boolean merge(AbstractSubGraph p) {
        if (p != this && p != null) {
            merge_vc_cells_map(p);
            merge_value_cell_map(p);
            merge_mergehistory(p);
            return true;
        }
        return false;
    }

    protected abstract void merge_vc_cells_map(AbstractSubGraph sg);

    protected abstract void merge_value_cell_map(AbstractSubGraph sg);

    protected void merge_mergehistory(AbstractSubGraph sg){
        mch.merge(sg.getMergeCausesHistory());
    }

    public void addMergeCause(int tid, Collection<ViolationCause> mc){
        mch.add(tid, mc);
    }

    /**
     * Test is needed, especially to see if there is redundancy (e.g., the same hash value of different collections)
     * @param mcs collections of violation causes from merge decision.
     */
    public void addMergeCauses(HashMap<Integer, Collection<ViolationCause>> mcs){
        mch.addAll(mcs);
    }

    public Collection<AbstractSubGraph> delete_rule(int rid){
        mch.delete_rule(rid);
        boolean affected = update_vc_cells_map(rid);
        if(affected)
        {
            return split(affected);
        } else {
            return null;
        }
    }

    /**
     *
     * @param deleterule, true only if sg has vc belonging to the deleted rule. false also if it is coming from updateWindow
     * @return
     */
    public Collection<AbstractSubGraph> split(boolean deleterule){

        ArrayList<Collection<ViolationCause>> sids = new ArrayList();

        Collection<ViolationCause> vcs = new HashSet(getSubgraphID(true));

        for(Collection<ViolationCause> overlap : mch.getMergeCauses()){
            Collection<ViolationCause> sid = new HashSet(overlap);
            sid.forEach(x -> vcs.remove(x));

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
            // if it's coming from updateWindow, here should be nothing
            if(deleterule)
            {
                rebuild_value_cell_map();
            }
            return null;
        } else if(sids.size() > 1){
            Collection<AbstractSubGraph> sgs = new ArrayList();
            for(Collection<ViolationCause> sid : sids){
                MergeHistory n_merge_causes = mch.getSubsetbySid(sid);
                AbstractSubGraph split_sg = getSplitSubGraph(sid, n_merge_causes);
                sgs.add(split_sg);
            }
            return sgs;
        } else {
            return null;
        }
    }

    public abstract boolean update_vc_cells_map(int deleted_rid);

    protected abstract AbstractSubGraph getSplitSubGraph(Collection<ViolationCause> sid, MergeHistory n_merge_causes);

    protected abstract void rebuild_value_cell_map();

    protected abstract void rebuild_topObject();

    protected void updateRank(Object v, AbstractSuperCell tids) {
        updateRank(v, tids.size());
    }

    private void updateRank(Object v, int size) {
        ObjectWithCount oldv = null;
        int i = 0;
        for (; i < proposal_size; i++) {
            if (topObject[i] == null) {
                    topObject[i] = new ObjectWithCount(v, size);
                return;
            } else if (topObject[i].equalValue(v)) {
                topObject[i].updateCount(size);
                return;
            } else {
                if (size > topObject[i].getCount()) {
                    oldv = topObject[i];
                    topObject[i] = new ObjectWithCount(v, size);
                    break;
                }
            }
        }

        if(oldv != null){
            ObjectWithCount tmp;
            for (int j = i + 1; j < proposal_size; j++) {
                if (topObject[j] == null) {
                    topObject[j] = oldv;
                    return;
                } else {
                    if(topObject[j].equalValue(v)) {
                        topObject[j] = oldv;
                        break;
                    } else {
                        tmp = topObject[j];
                        topObject[j] = oldv;
                        oldv = tmp;
                    }
                }
            }
        }
    }

    //debug use
    public abstract int getSuperCellNum();

    // debug use
    public int getMergeCausesCount(){
        return mch.size();
    }

    public HashMap<Object, Integer> getTopObjectWithCount() {
        HashMap<Object, Integer> result = new HashMap();
        for (int i = 0; i < proposal_size; i++) {
            if (topObject[i] != null) {
                result.put(topObject[i].getValue(), topObject[i].getCount());
            } else {
                break;
            }
        }
        return result;
    }

    public class ObjectWithCount {
        public Object value;
        public int count;

        public ObjectWithCount(Object v, int c) {
            value = v;
            count = c;
        }

        public boolean equalValue(Object v) {
            return value.equals(v);
        }

        public void updateCount(int nc) {
            count = nc;
        }

        public int getCount() {
            return count;
        }

        public Object getValue() {
            return value;
        }

        public ObjectWithCount copy() {
            return new ObjectWithCount(value, count);
        }

        @Override
        public String toString(){
            return "value: " + value + ", count: " + count;
        }
    }
}
