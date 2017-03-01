package storm.dataclean.auxiliary.repair.subgraph;

//import org.apache.storm.guava.base.Predicate;
import storm.dataclean.auxiliary.base.*;
import storm.dataclean.auxiliary.repair.cellvchistory.AbstractCellVcHistory;
import storm.dataclean.auxiliary.repair.mergeCausehistory.MergeHistory;
import storm.dataclean.auxiliary.repair.mergeCausehistory.BasicMergeHistory;
import storm.dataclean.exceptions.ViolationOldRecordsLossException;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by tian on 19/11/2015.
 * This is a version without dynamic processing (rule evolution).
 */
public class BasicSubGraph extends AbstractSubGraph {

    public Map<Object, BasicSuperCell> value_cell_map;
    public Map<ViolationCause, HashMap<Object, BasicSuperCell>> vc_cells_map;

    public BasicSubGraph(Predicate<Integer> tp, int p, AbstractCellVcHistory cvm, Violation v) throws ViolationOldRecordsLossException { // v must be a new violation where newVio is true
        if (!v.isNewVio()) {
            throw new ViolationOldRecordsLossException(v);
        }
        value_cell_map = new HashMap();
        vc_cells_map = new HashMap();
        tid_predicate = tp;
        proposal_size = p;
        cell_vc_map = cvm;
        topObject = new ObjectWithCount[proposal_size];
        attrid = v.getRattr_index();
        addCell(v.getValue(), v.getTid(), v.getVioCause());  // addFromCellVcHistory current tuple's id and its value
        addCells(v.getOthervalue(), v.getOthervalue_tids(), v.getVioCause()); // addFromCellVcHistory old tids which will be filterred
        mch = new BasicMergeHistory();

    }

    public Collection<ViolationCause> getSubgraphID(){
        return vc_cells_map.keySet();
    }

    @Override
    public Collection<ViolationCause> getSubgraphID(boolean updating) {
        return getSubgraphID();
    }

    @Override
    public int getVcCellGroupSize() {
        return vc_cells_map.size();
    }

    @Override
    public String toString() {
        return "Bleach: BasicSubGraph tostring to be defined";
    }

    protected void addCell(Object value, int tid, ViolationCause vc) {
        if(tid_predicate.test(tid)){
            vc_cells_map.putIfAbsent(vc, new HashMap());
            HashMap<Object, BasicSuperCell> cells = vc_cells_map.get(vc);
            if(cells.containsKey(value)){
                cells.get(value).add(tid);
            } else {
                cells.put(value, new BasicSuperCell(tid));
            }
            BasicSuperCell sc;
            if(value_cell_map.containsKey(value)){
                sc = value_cell_map.get(value);
                sc.add(tid);
            } else {
                sc = new BasicSuperCell(tid);
                value_cell_map.put(value, sc);
            }
//            cell_vc_map.add(attrid, tid, vc);
            updateRank(value, sc);
        }
    }

    protected void addCells(Object value, AbstractSuperCell tids, ViolationCause vc) {
        if(tids.filter(tid_predicate)){
//            cell_vc_map.add(attrid, tids, vc);
            if(value_cell_map.containsKey(value)){
                System.err.println("Bleach: not possible in AbstractSubGraph class");


            } else {
                value_cell_map.put(value, (BasicSuperCell)tids);
            }
            vc_cells_map.putIfAbsent(vc, new HashMap<Object, BasicSuperCell>());
            vc_cells_map.get(vc).put(value, (BasicSuperCell)tids);
            updateRank(value, tids);
        }
    }

    private Map<ViolationCause, HashMap<Object, BasicSuperCell>> get_vc_cells_map(){
        return vc_cells_map;
    }

    private Map<Object, BasicSuperCell> get_value_cell_map(){
        return value_cell_map;
    }

    @Override
    protected void merge_vc_cells_map(AbstractSubGraph sg){
        vc_cells_map.putAll(((BasicSubGraph) sg).get_vc_cells_map());
    }

    @Override
    protected void merge_value_cell_map(AbstractSubGraph sg){
        Map<Object, BasicSuperCell> other_value_cell_map = ((BasicSubGraph)sg).get_value_cell_map();
        for(Map.Entry<Object, BasicSuperCell> entry : other_value_cell_map.entrySet()){
            Object v = entry.getKey();
            BasicSuperCell sc;// = entry.getValue();
            if(value_cell_map.containsKey(v)){
                sc = value_cell_map.get(v);
                sc.merge(entry.getValue());
//                value_cell_map.get(v).merge(sc);
            } else {
                sc = entry.getValue();
                value_cell_map.put(v, sc);
            }
            updateRank(v, sc);
        }
    }

    @Override
    public boolean update_vc_cells_map(int deleted_rid) {
        if(vc_cells_map.keySet().stream().allMatch(x->x.getRuleid()!=deleted_rid)){
            return false;
        } else {
            Map<ViolationCause, HashMap<Object, BasicSuperCell>> new_vc_cells_map = vc_cells_map.entrySet().stream().
                    filter(x -> x.getKey().getRuleid()!= deleted_rid).collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
            vc_cells_map = new_vc_cells_map;
            return true;
        }
    }

    @Override
    protected AbstractSubGraph getSplitSubGraph(Collection<ViolationCause> sid, MergeHistory n_merge_causes) {
        return new BasicSubGraph(tid_predicate, proposal_size, cell_vc_map, attrid, vc_cells_map, sid, n_merge_causes);
    }

    /**
     *  create an emtpy subgraph from splitting a subgraph,
     *  later cells and vcs will be added
     * @param tp
     * @param p
     * @param cvm
     * @param aid
     * @param parent_vc_cells_map
     * @param sid
     * @param n_merge_causes
     */
    public BasicSubGraph(Predicate<Integer> tp, int p, AbstractCellVcHistory cvm, int aid,
                         Map<ViolationCause, HashMap<Object, BasicSuperCell>> parent_vc_cells_map,
                         Collection<ViolationCause> sid,
                         MergeHistory n_merge_causes){
        tid_predicate = tp;
        proposal_size = p;
        cell_vc_map = cvm;
        topObject = new ObjectWithCount[proposal_size];
        attrid = aid;
        vc_cells_map = new HashMap();
        sid.stream().map(x -> vc_cells_map.put(x, parent_vc_cells_map.get(x)));
        rebuild_value_cell_map();
        mch = n_merge_causes;
    }

    protected void rebuild_value_cell_map(){
        topObject = new ObjectWithCount[proposal_size];
        value_cell_map = new HashMap();
        for(HashMap<Object, BasicSuperCell> map : vc_cells_map.values()){
            for(Map.Entry<Object, BasicSuperCell> entry : map.entrySet()){
                BasicSuperCell sc;
                if(value_cell_map.containsKey(entry.getKey())){
                    sc = value_cell_map.get(entry.getKey());
                    sc.merge(entry.getValue());
                } else {
                    sc = entry.getValue();
                    value_cell_map.put(entry.getKey(), sc);
                }
                updateRank(entry.getKey(), sc);
            }
        }
    }

    @Override
    protected void rebuild_topObject() {
        System.err.println("BasicSubGraph should not use rebuild_topObject function");
        topObject = new ObjectWithCount[proposal_size];
        value_cell_map.entrySet().forEach(x->updateRank(x.getKey(),x.getValue()));
    }

    @Override
    public int getSuperCellNum() {
        return value_cell_map.size();
    }
}
