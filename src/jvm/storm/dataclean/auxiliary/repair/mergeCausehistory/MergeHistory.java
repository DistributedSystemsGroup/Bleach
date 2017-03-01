package storm.dataclean.auxiliary.repair.mergeCausehistory;

import storm.dataclean.auxiliary.base.ViolationCause;

import java.util.*;

/**
 * Created by yongchao on 3/10/16.
 */
public abstract class MergeHistory {
    public abstract int size();

    public abstract void merge(MergeHistory mch);

    public abstract void add(int tid, Collection<ViolationCause> mc);

    public void addAll(HashMap<Integer, Collection<ViolationCause>> mcs){
        mcs.forEach((x, y) -> add(x, y));
    }

    public abstract Collection<ViolationCause> getVcs();

    public abstract Collection<Collection<ViolationCause>> getMergeCauses();

    public abstract MergeHistory getSubsetbySid(Collection<ViolationCause> sid);

    public abstract void delete_rule(int rid);

}
