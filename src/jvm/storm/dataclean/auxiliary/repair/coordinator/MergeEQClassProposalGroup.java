package storm.dataclean.auxiliary.repair.coordinator;

import storm.dataclean.auxiliary.base.ViolationCause;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tian on 01/12/2015.
 */
public class MergeEQClassProposalGroup {

    public int tid;
    public int kid;

    private HashMap<Integer, MergeEQClassProposal> attr_map;

    public MergeEQClassProposalGroup(){}

    public MergeEQClassProposalGroup(int t, int k){
        tid = t;
        kid = k;
        attr_map = new HashMap();
    }

    public void add(int attr_index, int t, ViolationCause vc){
        if(attr_map.containsKey(attr_index)){
            attr_map.get(attr_index).add(t, vc);
        } else {
            MergeEQClassProposal p = new MergeEQClassProposal(tid);
            p.add(t, vc);
            attr_map.put(attr_index, p);

        }
    }

    public void add(int attr_index, int t, Collection<ViolationCause> vcs){
        if(attr_map.containsKey(attr_index)){
            attr_map.get(attr_index).add(t, vcs);
        } else {
            MergeEQClassProposal p = new MergeEQClassProposal(tid);
            p.add(t, vcs);
            attr_map.put(attr_index, p);
        }
    }

    public int size(){
        return attr_map.size();
    }

    public boolean isEmpty(){
        return attr_map.size() == 0;
    }

    public HashMap<Integer, MergeEQClassProposal> getAttr_map(){
        return attr_map;
    }

    public boolean hasAttr(int attr){
        return attr_map.containsKey(attr);
    }

    public MergeEQClassProposal getSingleMergeEQClassProposal(int attr){
        return attr_map.get(attr);
    }

    public boolean containAttr(int attr){
        return attr_map.containsKey(attr);
    }


    public int getTid(){
        return tid;
    }

    public int getKid(){
        return kid;
    }


    @Override
    public String toString(){
        return "Tid " + tid + ": " + attr_map;
    }

    public void merge(MergeEQClassProposalGroup mp){
        for(Map.Entry<Integer, MergeEQClassProposal> entry : mp.getAttr_map().entrySet()){
            int attr_id = entry.getKey();
            MergeEQClassProposal smp = entry.getValue();
            if(attr_map.containsKey(attr_id)){
                attr_map.get(attr_id).merge(smp);
            } else {
                attr_map.put(attr_id, smp);
            }
        }
    }
}
