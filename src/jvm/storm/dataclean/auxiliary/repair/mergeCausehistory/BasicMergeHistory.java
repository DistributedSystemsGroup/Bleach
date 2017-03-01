package storm.dataclean.auxiliary.repair.mergeCausehistory;

import storm.dataclean.auxiliary.base.ViolationCause;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by yongchao on 3/7/16.
 */
public class BasicMergeHistory extends MergeHistory{

    protected Set<Collection<ViolationCause>> vcs;

    private Collection<ViolationCause> vclist;

    public BasicMergeHistory(){
        vcs = new HashSet();
        vclist = new HashSet();
    }

    private BasicMergeHistory(Collection<Collection<ViolationCause>> split_vcs){
        // only debug use, to be deleted
        if( !(split_vcs instanceof HashSet)){
            System.err.println("Bleach: split_vcs_counter is a map but not a hashmap");
        }
        vcs = new HashSet<>(split_vcs);
        build_vcs();
    }


    private void build_vcs(){
        vclist = new HashSet();
        vcs.forEach(x->vclist.addAll(x));
    }

    // debug use
    public int size(){
        return vcs.size();
    }

    @Override
    public void merge(MergeHistory mch) {
        vcs.addAll(mch.getMergeCauses());
        build_vcs();
    }

    public void add(int tid, Collection<ViolationCause> mc){
        if(mc.size() > 1)
        {
            vcs.add(mc);
            vclist.addAll(mc);
        }
    }

    @Override
    public Collection<ViolationCause> getVcs() {
        return vclist;
    }

    public Collection<Collection<ViolationCause>> getMergeCauses(){
        return vcs;
    }

    /**
     * To arrive here, split should already be invoked.
     * @param sid
     * @return
     */
    @Override
    public MergeHistory getSubsetbySid(Collection<ViolationCause> sid) {
        return new BasicMergeHistory(vcs.stream().
                filter(x -> !Collections.disjoint(x, sid)).
                collect(Collectors.toSet()));
    }

    @Override
    public void delete_rule(int rid) {
        for(Collection<ViolationCause> vc : vcs){
            vc.removeIf(x -> x.getRuleid() == rid);
        }
        vcs = vcs.stream().filter(x -> x.size() > 1).collect(Collectors.toSet());
        build_vcs();
    }

}
