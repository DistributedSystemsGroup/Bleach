package storm.dataclean.auxiliary.repair.cellvchistory;

import storm.dataclean.auxiliary.base.ViolationCause;

import java.util.Collection;

/**
 * Created by yongchao on 3/4/16.
 */
public abstract class AbstractCellVcHistory {

    public String DEBUG_PREFIX = "DEBUGPRINT: ";

    public abstract Collection<ViolationCause> getVioCauses(int attr_index, int tid);

    public abstract void add(int attr_id, int tid, ViolationCause vc);

    public abstract void delete_rule(int rid);

    public abstract void print_log();
}
