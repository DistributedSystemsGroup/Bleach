package storm.dataclean.auxiliary.detect;

import storm.dataclean.auxiliary.base.DataInWindowRecursive;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.exceptions.BleachException;

/**
 * Created by tian on 18/12/2015.
 */
public class WinDataHistory extends BasicDataHistory {

    public DataInWindowRecursive<Object, WinCellGroup> history; // LHS->cellgroup mapping

    public WinDataHistory(int rid, int rattr, int step) {
        this.rid = rid;
        right_attr_index = rattr;
        history = new DataInWindowRecursive(0, step);
    }

    @Override
    public Violation getViolation(int tid, Object leftvalue, Object rightvalue) throws BleachException {

        history.updateWindow(tid);
        Violation violation;
        WinCellGroup cg;
        if (history.containsKey(leftvalue)) {
            cg = history.get(leftvalue);
            if (!cg.isUnique()) {
                /*
                if vg already has multiple values, violations between tuples in history should already been detected.
                It is enough just returning current tuple in violation without indicating it has violations with which tuples.
                 */
                violation = new Violation(tid, rid, leftvalue, rightvalue, right_attr_index);
            } else {
                if (cg.contain(rightvalue)) {
                    violation = new Violation.NullViolation(tid, rid);
                } else {
                    /*
                    First time detects violations in vg, puts all information inside vg. Now vg should only includes
                    one value (except current value).
                     */
//                    violation = new Violation(tid, rid, leftvalue, rightvalue, right_attr_index, vg.getUniqueValueMap());
                    violation = new Violation(tid, rid, leftvalue, rightvalue, right_attr_index, cg.getUniqueValue(), cg.getUniqueTids());
                }
            }
            cg.add(rightvalue, tid); // store new tuple
            history.update(leftvalue, cg);
        } else {
            cg = new WinCellGroup(history.getWin_cursor(), history.getWin_step(), rightvalue, tid);
            history.put(leftvalue, cg); // store new tuple by creating a new WinCellGroup
            violation = new Violation.NullViolation(tid, rid);
        }
        return violation;
    }

    @Override
    public void print_log() {
        System.err.println(DEBUG_PREFIX + "window size/step " + history.getWin_step());
        String line = "DataHistoryMemwinbasic has " + history.size() + "cell groups";
        System.err.println(DEBUG_PREFIX + line);
        System.err.println(DEBUG_PREFIX + "cellgroupwin has at most " + history.values().stream().mapToInt(cg->cg.getSuperCellNum()).max().getAsInt() + " super cells");
        System.err.println(DEBUG_PREFIX + "cellgroupwin has at most " + history.values().stream().mapToInt(cg->cg.getCellNum()).max().getAsInt() + " cells");
    }


}
