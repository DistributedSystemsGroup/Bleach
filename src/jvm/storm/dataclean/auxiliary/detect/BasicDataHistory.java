package storm.dataclean.auxiliary.detect;

import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.exceptions.BleachException;

import java.util.HashMap;

/**
 * Created by tian on 18/12/2015.
 */
public class BasicDataHistory extends AbstractDataHistory {

    private HashMap<Object, BasicCellGroup> history; // LHS->cellgroup mapping

    public BasicDataHistory(){}

    public BasicDataHistory(int rid, int rattr){
        this.rid = rid;
        history = new HashMap();
        right_attr_index = rattr;
    }

    @Override
    public Violation getViolation(int tid, Object leftvalue, Object rightvalue) throws BleachException {
        Violation violation;
        BasicCellGroup cg;
        if(history.containsKey(leftvalue)){
            cg = history.get(leftvalue);
            if(!cg.isUnique()){
                /*
                if vg already has multiple values, violations between tuples in history should already been detected.
                It is enough just returning current tuple in violation without indicating it has violations with which tuples.
                 */
                violation = new Violation(tid, rid, leftvalue, rightvalue, right_attr_index);
            } else {
                if(cg.contain(rightvalue)){
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
//            history.updateVcSubGraph(leftvalue, cg);
        } else {
            cg = new BasicCellGroup(rightvalue, tid);
            history.put(leftvalue, cg); // store new tuple by creating a new BasicCellGroup
            violation = new Violation.NullViolation(tid, rid);
        }
        return violation;
    }

    @Override
    public void print_log() {
        String line = "BasicDataHistory has " + history.size() + "cell groups";
        System.err.println(DEBUG_PREFIX + line);
        System.err.println(DEBUG_PREFIX + "cellgroup has at most " + history.values().stream().mapToInt(cg->cg.getSuperCellNum()).max().getAsInt() + " super cells");
        System.err.println(DEBUG_PREFIX + "cellgroup has at most " + history.values().stream().mapToInt(cg->cg.getCellNum()).max().getAsInt() + " cells");
    }

}
