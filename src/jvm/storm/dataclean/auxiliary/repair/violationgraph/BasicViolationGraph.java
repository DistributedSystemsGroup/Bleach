package storm.dataclean.auxiliary.repair.violationgraph;

//import org.apache.storm.guava.base.Predicate;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.base.ViolationCause;
import storm.dataclean.auxiliary.repair.cellvchistory.BasicCellVcHistory;
import storm.dataclean.auxiliary.repair.subgraph.AbstractSubGraph;
import storm.dataclean.auxiliary.repair.subgraph.BasicSubGraph;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.ImpossibleException;
import storm.dataclean.exceptions.ViolationOldRecordsLossException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Created by yongchao on 1/3/16.
 */
public class BasicViolationGraph extends ViolationGraph {

    private HashMap<ViolationCause, AbstractSubGraph> history; // violation_cause and partial eqclass table;

    public BasicViolationGraph(){}

    public BasicViolationGraph(int psize, Predicate<Integer> tp, String[] attrs){
        history = new HashMap();
        proposal_size = psize;
        tid_predicate = tp;
        cell_vc_map = new BasicCellVcHistory(attrs);
    }

    protected boolean containVC(ViolationCause vc){
        return history.containsKey(vc);
    }

    public AbstractSubGraph getSubgraph(ViolationCause vc){
        return history.get(vc);
    }

    protected void put(ViolationCause vc, AbstractSubGraph peqclass) throws ImpossibleException {
        if(history.containsKey(vc)){
            throw new ImpossibleException("VC: " + vc + " should not be in history");
        }
        history.put(vc, peqclass);
    }

    // better to let it be private, but ...
    public void updateVcSubGraph(ViolationCause vc, AbstractSubGraph peqclass){
        history.put(vc, peqclass);
    }

    @Override
    public void updateVcSubGraphFromMerge(ViolationCause vc, AbstractSubGraph peqclass) {
        history.put(vc, peqclass);
    }

    @Override
    public AbstractSubGraph getSubGraphWithUpdate(Violation v) throws BleachException {
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
                sg = new BasicSubGraph(tid_predicate, proposal_size, cell_vc_map, v);
                put(vc, sg);
                return sg;
            }
        }
    }

    public Collection<ViolationCause> getVioCauses(int attrid, int oldtid){
        return cell_vc_map.getVioCauses(attrid, oldtid);
    }

    public void delete_rule(int rid){
        System.err.println("Bleach: violation graph split is not finished");

        cell_vc_map.delete_rule(rid);
        HashMap<ViolationCause, AbstractSubGraph> new_vc_eq_table = new HashMap();
        for(Map.Entry<ViolationCause, AbstractSubGraph> entry : history.entrySet())
        {
            ViolationCause vc = entry.getKey();
            if(new_vc_eq_table.containsKey(vc)){
                continue;
            } else {
                if (vc.getRuleid() != rid) {
                    Collection<AbstractSubGraph> sgs = entry.getValue().delete_rule(rid);
                    if(sgs == null){
                        AbstractSubGraph sg = entry.getValue();
                        for(ViolationCause nvc : sg.getSubgraphID()){
                            new_vc_eq_table.put(nvc, sg);
                        }
                    } else {
                        for(AbstractSubGraph sg : sgs){
                            for(ViolationCause nvc : sg.getSubgraphID()){
                                new_vc_eq_table.put(nvc, sg);
                            }
                        }
                    }
                }
            }
        }
        history = new_vc_eq_table;
    }

    @Override
    public void print_log() {
        System.err.println(DEBUG_PREFIX + " violationgraphnowin has " + history.size() + " vcs");
        System.err.println(DEBUG_PREFIX + " violationgraphnowin has " + (new HashSet(history.values())).size()+ " subgraphs");
        System.err.println(DEBUG_PREFIX + " violationgraphnowin has at most " +
                history.values().stream().mapToInt(x->x.getSuperCellNum()).max().getAsInt() + " super cells in a subgraph");
        System.err.println(DEBUG_PREFIX + " violationgraphnowin has at most " +
                history.values().stream().mapToInt(x->x.getSubgraphID().size()).max().getAsInt() + " vcs in a subgraph");
        System.err.println(DEBUG_PREFIX + " violationgraphnowin has at most " +
                history.values().stream().mapToInt(x->x.getMergeCausesCount()).max().getAsInt() + " merge causes in a subgraph");
    }

}
