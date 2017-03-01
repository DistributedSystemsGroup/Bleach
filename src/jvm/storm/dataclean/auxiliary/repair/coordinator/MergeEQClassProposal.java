package storm.dataclean.auxiliary.repair.coordinator;

import storm.dataclean.auxiliary.base.ViolationCause;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by yongchao on 12/30/15.
 */
public class MergeEQClassProposal {
    public int tid; // current tid
    private Collection<ViolationCause> vcs;
    private HashMap<Integer, Collection<ViolationCause>> tid_vc_map;

    public MergeEQClassProposal(){}

    public MergeEQClassProposal(int t){
        vcs = new HashSet();
        tid_vc_map = new HashMap();
        tid = t;
    }

    public void add(int t, ViolationCause vc){
        vcs.add(vc);
        if(tid_vc_map.containsKey(t)){
            tid_vc_map.get(t).add(vc);
        } else {
            HashSet<ViolationCause> vc_set = new HashSet();
            vc_set.add(vc);
            tid_vc_map.put(t, vc_set);
        }
    }

    public void add(int oldtid, Collection<ViolationCause> new_vcs){
        vcs.addAll(new_vcs);
        if(tid_vc_map.containsKey(oldtid)){
            tid_vc_map.get(oldtid).addAll(new_vcs);
        } else {
            tid_vc_map.put(oldtid, new HashSet(new_vcs));
        }
    }

    public int getTid(){
        return tid;
    }

    public Collection<ViolationCause> getVcs(){
        return vcs;
    }

    public HashMap<Integer, Collection<ViolationCause>> getTidVcMap(){
        return tid_vc_map;
    }

    public void merge(MergeEQClassProposal smp){
        vcs.addAll(smp.getVcs());
        // here, it is because history of a tid can only be stored in a single repair worker. Thus, simply merge hashmap is enough
        tid_vc_map.putAll(smp.getTidVcMap());
    }

    public HashMap<Integer, Collection<ViolationCause>> getMergecauses(){
        return tid_vc_map;
    }

    @Override
    public String toString(){
        return "tid="+tid+", vcs="+vcs+", tid_vc_map:"+tid_vc_map;
    }

}
