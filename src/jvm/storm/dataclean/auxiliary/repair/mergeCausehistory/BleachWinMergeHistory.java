package storm.dataclean.auxiliary.repair.mergeCausehistory;

import storm.dataclean.auxiliary.base.ViolationCause;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by yongchao on 3/7/16.
 */
public class BleachWinMergeHistory extends MergeHistory{

    protected Set<Collection<ViolationCause>> vcs;

    private Collection<ViolationCause> vclist;

    public BleachWinMergeHistory(){
        vcs = new HashSet();
        vclist = new HashSet();
    }

    private BleachWinMergeHistory(Collection<Collection<ViolationCause>> split_vcs){
        vcs = new HashSet<>(split_vcs);
        build_vcs();
    }

    private void build_vcs(){
        vclist = new HashSet();
        vcs.forEach(x->vclist.addAll(x));
    }

    public int size(){
        return vcs.size();
    }

    @Override
    public void merge(MergeHistory mch) {
        for(Collection<ViolationCause> vcs_record : mch.getMergeCauses()){
            vcs.add(vcs_record);
            vclist.addAll(vcs_record);
        }
    }

    public void add(int tid, Collection<ViolationCause> mc){
        if(mc.size() > 1)
        {
            // copy to avoid concurrentmodificationexception
            HashSet<ViolationCause> mc_copy = new HashSet(mc);
//            vcs.add(mc);
//            vclist.addAll(mc);
            vcs.add(mc_copy);
            vclist.addAll(mc_copy);
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
        Collection<Collection<ViolationCause>> split_vcs = new HashSet();
        for(Collection<ViolationCause> cvc : vcs){
            if(!Collections.disjoint(cvc,sid)){
                split_vcs.add(cvc);
            }
        }
        return new BleachWinMergeHistory(split_vcs);

    }

    @Override
    public void delete_rule(int rid) {
        for(Collection<ViolationCause> vc : vcs){
            vc.removeIf(x -> x.getRuleid() == rid);
        }

        vcs = vcs.stream().filter(x -> x.size() > 1).collect(Collectors.toSet());
//        HashSet<Collection<ViolationCause>> new_vcs = new HashSet();
//        for(Collection<ViolationCause> cvc : vcs){
//            if(cvc.size() > 1){
//                new_vcs.add(cvc);
//            }
//        }
//        vcs = new_vcs;
        build_vcs();
    }

    public void updateWindow(Collection<ViolationCause> dvcs){
        if(dvcs.size() > 0){
            Set<Collection<ViolationCause>> new_vcs = new HashSet();
            for(Collection<ViolationCause> cvc : vcs){
                cvc.removeAll(dvcs);
                if(cvc.size() > 1){
                    new_vcs.add(cvc);
                }
            }
            vcs = new_vcs;
            build_vcs();
        }
    }

}
