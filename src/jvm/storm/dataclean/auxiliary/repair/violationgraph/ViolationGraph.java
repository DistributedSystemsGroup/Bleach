package storm.dataclean.auxiliary.repair.violationgraph;

//import org.apache.storm.guava.base.Predicate;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.base.ViolationCause;
import storm.dataclean.auxiliary.repair.cellvchistory.AbstractCellVcHistory;
import storm.dataclean.auxiliary.repair.subgraph.AbstractSubGraph;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposal;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.ImpossibleException;

import java.util.Collection;
import java.util.function.Predicate;

/**
 * Created by yongchao on 3/3/16.
 */
public abstract class ViolationGraph {

    public Predicate<Integer> tid_predicate;
    public int proposal_size;
    public AbstractCellVcHistory cell_vc_map;

    // debug use
    public String DEBUG_PREFIX = "Debug: ";

    public ViolationGraph(){}

    protected abstract boolean containVC(ViolationCause vc);

    public abstract AbstractSubGraph getSubgraph(ViolationCause vc);

    protected abstract void put(ViolationCause vc, AbstractSubGraph peqclass) throws ImpossibleException;

    public abstract void updateVcSubGraph(ViolationCause vc, AbstractSubGraph peqclass);

    public abstract void updateVcSubGraphFromMerge(ViolationCause vc, AbstractSubGraph peqclass);

    public abstract AbstractSubGraph getSubGraphWithUpdate(Violation v) throws BleachException;

    public AbstractSubGraph getMergedSubGraph(AbstractSubGraph sg, MergeEQClassProposal smp) throws ImpossibleException{
        Collection<ViolationCause> vios = smp.getVcs();
        for(ViolationCause vc : vios) {
            if (sg == null) {
                sg = getSubgraph(vc);
                if (sg == null) {
                    // It is ok as the coordination of merge is delayed, a sg may be out of the window
                    System.out.println("one sg out of window: vc="+vc + ", tid="+smp.getTid());
                }
            } else {
                AbstractSubGraph sg2 = getSubgraph(vc);
                if(sg2 == null){
                    System.out.println("one sg out of window: vc="+vc + ", tid="+smp.getTid());
                    continue;
                }
                if(sg.getVcCellGroupSize() > sg2.getVcCellGroupSize()){
                    if (sg.merge(sg2)) {
                        for(ViolationCause vc_sid : sg2.getSubgraphID())
                        {
                            updateVcSubGraphFromMerge(vc_sid, sg);
                        }
                    }
                }
                else {
                    if (sg2.merge(sg)) {
                        for (ViolationCause vc_sid : sg.getSubgraphID()) {
                            updateVcSubGraphFromMerge(vc_sid, sg2);
                        }
                    }
                    sg = sg2;
                }
            }
        }
        sg.addMergeCauses(smp.getMergecauses());
        return sg;
    }

    public Collection<ViolationCause> getVioCauses(int attrid, int oldtid){
        return cell_vc_map.getVioCauses(attrid, oldtid);
    }

    public abstract void delete_rule(int rid);

    public abstract void print_log();

    public void addCellVcFromViolation(Violation v){
        if(tid_predicate.test(v.getTid()))
        {
            cell_vc_map.add(v.getRattr_index(), v.getTid(), v.getVioCause());
        }
        if(v.isNewVio()){
            for(int tid : v.getOthervalue_tids().getTids()) {
                if(tid_predicate.test(tid))
                {
                    cell_vc_map.add(v.getRattr_index(), tid, v.getVioCause());
                }
            }
        }
    }

}
