package storm.dataclean.auxiliary.repair.mergeCausehistory;

import storm.dataclean.auxiliary.base.ViolationCause;
import storm.dataclean.auxiliary.base.Windowing;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yongchao on 3/7/16.
 */
public class BasicWinMergeHistory extends MergeHistory implements Windowing {

    protected Set<Collection<ViolationCause>> first_vcs;
    protected Set<Collection<ViolationCause>> second_vcs;

    public Collection<ViolationCause> vcs;

    public int win_cursor;
    public int win_step;

    public BasicWinMergeHistory(int wcursor, int wstep){
        first_vcs = new HashSet();
        second_vcs = new HashSet();
        vcs = new HashSet();
        win_cursor = wcursor;
        win_step = wstep;
    }

    private BasicWinMergeHistory(int wcursor, int wstep, Set<Collection<ViolationCause>> first, Set<Collection<ViolationCause>> second){
        first_vcs = first;
        second_vcs = second;
        build_vcs();
        win_cursor = wcursor;
        win_step = wstep;
    }

    private void build_vcs(){
        vcs = new HashSet();
        first_vcs.forEach(x->vcs.addAll(x));
        second_vcs.forEach(x->vcs.addAll(x));
    }

    @Override
    public Collection<ViolationCause> getVcs(){
        return vcs;
    }

    public int size(){
        return vcs.size();
    }

    @Override
    public void merge(MergeHistory mch) {
        first_vcs.addAll(((BasicWinMergeHistory)mch).getFirstmergeCauses());
        second_vcs.addAll(((BasicWinMergeHistory)mch).getSecondmergeCauses());
        vcs.addAll(((BasicWinMergeHistory) mch).getVcs());
    }

    public void add(int tid, Collection<ViolationCause> mc){
        if(mc.size() > 1)
        {
            int win_thre = win_cursor - win_step;
            if(tid <= win_thre){
                if(!second_vcs.contains(mc))
                {
                    if(!first_vcs.contains(mc)){
                        first_vcs.add(mc);
                        vcs.addAll(mc);
                    }
                }
            } else {
                if(!second_vcs.contains(mc)) {
                    second_vcs.add(mc);
                    if (first_vcs.contains(mc)) {
                        first_vcs.remove(mc);
                    } else {
                        vcs.addAll(mc);
                    }
                }
            }
        }
    }

    public Collection<Collection<ViolationCause>> getMergeCauses(){
        return Stream.concat(first_vcs.stream(), second_vcs.stream()).collect(Collectors.toCollection(HashSet::new));
    }

    private Collection<Collection<ViolationCause>> getFirstmergeCauses(){
        return first_vcs;
    }

    private Collection<Collection<ViolationCause>> getSecondmergeCauses(){
        return second_vcs;
    }
    /**
     * To arrive here, split should already be invoked.
     * @param sid
     * @return
     */
    @Override
    public MergeHistory getSubsetbySid(Collection<ViolationCause> sid) {
        Set<Collection<ViolationCause>> sub_first = new HashSet();
        for(Collection<ViolationCause> cvc : first_vcs){
            if(!Collections.disjoint(cvc, sid)){
                sub_first.add(cvc);
            }
        }
        Set<Collection<ViolationCause>> sub_second = new HashSet();
        for(Collection<ViolationCause> cvc : second_vcs){
            if(!Collections.disjoint(cvc, sid)){
                sub_second.add(cvc);
            }
        }
        return new BasicWinMergeHistory(win_cursor, win_step, sub_first, sub_second);
    }

    @Override
    public void delete_rule(int rid) {
        for(Collection<ViolationCause> vc : first_vcs){
            vc.removeIf(x -> x.getRuleid() == rid);
        }
        Set<Collection<ViolationCause>> new_first_vcs = new HashSet();
        for(Collection<ViolationCause> cvc : first_vcs){
            if(cvc.size() > 1){
                new_first_vcs.add(cvc);
            }
        }
        first_vcs = new_first_vcs;
        for(Collection<ViolationCause> vc : second_vcs){
            vc.removeIf(x -> x.getRuleid() == rid);
        }
        Set<Collection<ViolationCause>> new_second_vcs = new HashSet();
        for(Collection<ViolationCause> cvc : second_vcs){
            if(cvc.size() > 1){
                new_second_vcs.add(cvc);
            }
        }
        second_vcs = new_second_vcs;

        build_vcs();
    }

    @Override
    public boolean updateWindow(int tid) {
        if(tid > win_cursor){
            win_cursor += win_step;
            first_vcs = second_vcs;
            second_vcs = new HashSet();
            build_vcs();
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public String toString(){
        return "MCH: first=" + first_vcs + ", second=" + second_vcs;
    }
}
