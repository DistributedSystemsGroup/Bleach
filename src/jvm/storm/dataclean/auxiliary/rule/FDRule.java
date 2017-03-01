package storm.dataclean.auxiliary.rule;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.detect.AbstractDataHistory;
import storm.dataclean.auxiliary.detect.DataTuple;
import storm.dataclean.exceptions.RuleDefinitionException;
import storm.dataclean.exceptions.BleachException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by yongchao on 11/13/15.
 */
public class FDRule implements Rule{

    public int rid;
    public String schema[];
    public String left_attr;
    public String right_attr;
    public int left_attr_index;
    public int right_attr_index;
    public AbstractDataHistory history;

    public String DEBUG_PREFIX = "DEBUGPRINT: ";


    public FDRule(int id, String schemastring, String left, String right) throws RuleDefinitionException {
        rid = id;
        schema = schemastring.split(",");
        left_attr = left;
        right_attr = right;
        left_attr_index = -1;
        right_attr_index = -1;
        for(int i = 0; i < schema.length; i++){
            if(schema[i].equals(left_attr)){
                left_attr_index = i;
            }
            if(schema[i].equals(right_attr)){
                right_attr_index = i;
            }
        }
        if(left_attr_index == -1 || right_attr_index == -1){
            throw new RuleDefinitionException(left_attr+","+right_attr, schema);
        }
        history = AbstractDataHistory.create(rid, right_attr_index);
    }

    public FDRule(int id, String schemastring, String left, String right, int win, int win_size) throws RuleDefinitionException {
        System.err.println("new a fdrule");
        rid = id;
        schema = schemastring.split(",");
        left_attr = left;
        right_attr = right;
        left_attr_index = -1;
        right_attr_index = -1;
        for(int i = 0; i < schema.length; i++){
            if(schema[i].equals(left_attr)){
                left_attr_index = i;
            }
            if(schema[i].equals(right_attr)){
                right_attr_index = i;
            }
        }
        if(left_attr_index == -1 || right_attr_index == -1){
            throw new RuleDefinitionException(left_attr+","+right_attr, schema);
        }
        history = AbstractDataHistory.create(rid, right_attr_index, win, win_size);
    }


    @Override
    public Values getSubtuple(int tid, Tuple tuple){
        return new Values(tid, tuple.getValueByField("kid"), tuple.getValueByField(left_attr), tuple.getValueByField(right_attr), null);
    }

    /**
     * TODO
     * @param tuple
     * @return violation detected against history and addFromCellVcHistory new (sub) tuple to history
     */
    @Override
    public Violation detect(Tuple tuple) throws BleachException {
        int tid = tuple.getIntegerByField("tid");
        String left_value = tuple.getStringByField("left_attr");
        String right_value = tuple.getStringByField("right_attr");
        return getViolation(tid, left_value, right_value);
    }

    @Override
    public Violation detect(int tid, DataTuple datatuple) throws BleachException {
        String left_value = (String)datatuple.get(left_attr);
        String right_value = (String)datatuple.get(right_attr);
        return getViolation(tid, left_value, right_value);
    }

    protected Violation getViolation(int tid, String leftvalue, String rightvalue) throws BleachException {
        return history.getViolation(tid, leftvalue, rightvalue);
    }


    @Override
    public String getValueAttr() {
        return right_attr;
    }

    @Override
    public Collection<String> getAttrs() {
        List<String> attrs = new ArrayList();
        attrs.add(left_attr);
        attrs.add(right_attr);
        return attrs;
    }

    @Override
    public int getRid() {
        return rid;
    }

    @Override
    public void print_log() {
        String fd_string = "FD " + rid + " = (" + left_attr + ", " + right_attr + ")";
        System.err.println(DEBUG_PREFIX + fd_string);
        history.print_log();
    }

    @Override
    public String toString(){
        return "FD " + rid + " = (" + left_attr + ", " + right_attr + ")";
    }

    @Override
    public int hashCode(){
        return (rid << 24) + (right_attr.hashCode() << 12) + left_attr.hashCode();
    }

}
