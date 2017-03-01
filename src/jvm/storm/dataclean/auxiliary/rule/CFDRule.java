package storm.dataclean.auxiliary.rule;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
//import org.apache.storm.guava.base.Predicate;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.detect.DataTuple;
import storm.dataclean.exceptions.RuleDefinitionException;
import storm.dataclean.exceptions.BleachException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;


/**
 * Created by tian on 20/11/2015.
 */
public class CFDRule extends FDRule {

    public String cond_attr;
    public int cond_attr_index;
    public Predicate<String> predicate;

    public static String empty="";

    public CFDRule(int id, String schemastring, String left, String right, String condition) throws RuleDefinitionException {
        super(id, schemastring, left, right);

//        String[] ss = condition.replaceAll("\\p{P}","").split(" ");
        String[] ss = condition.trim().substring(1, condition.length()-1).split(" ");
        cond_attr = ss[0];
        String operator = ss[1];
        String operator_value = ss[2];
        cond_attr_index = -1;
        for(int i = 0; i < schema.length; i++){
            if(schema[i].equals(cond_attr)){
                cond_attr_index = i;
            }
        }
        if(cond_attr_index == -1){
            throw new RuleDefinitionException(left_attr+","+right_attr, schema);
        }
        if(operator.equals("eq")){
            predicate = s -> s.equals(operator_value);
        } else if (ss[1].equals("neq")){
            predicate = s -> !s.equals(operator_value) && !s.equals(empty);
        } else {
            throw new RuleDefinitionException("operator" + operator + " not recognized");
        }
    }

    public CFDRule(int id, String schemastring, String left, String right, String condition, int win, int win_size) throws RuleDefinitionException {
        super(id, schemastring, left, right, win, win_size);
//        String[] ss = condition.replaceAll("\\p{P}","").split(" ");
        String[] ss = condition.trim().substring(1, condition.length()-1).split(" ");


        System.out.println("cfdrule create: id="+id+", schemastring="+schemastring+",left="+left+",right="+right+",condition="+condition);


        cond_attr = ss[0];
        String operator = ss[1];
        String operator_value = ss[2];
        cond_attr_index = -1;
        for(int i = 0; i < schema.length; i++){
            if(schema[i].equals(cond_attr)){
                cond_attr_index = i;
            }
        }
        if(cond_attr_index == -1){
            throw new RuleDefinitionException(left_attr+","+right_attr, schema);
        }
        if(operator.equals("eq")){
            predicate = s -> s.equals(operator_value);
        } else if (ss[1].equals("neq")){
            predicate = s -> !s.equals(operator_value) && !s.equals(empty);
        } else {
            throw new RuleDefinitionException("operator" + operator + " not recognized");
        }
    }

    @Override
    public Values getSubtuple(int tid, Tuple tuple) {
        return new Values(tid, tuple.getValueByField("kid"), tuple.getValueByField(left_attr), tuple.getValueByField(right_attr), tuple.getValueByField(cond_attr));
    }

    @Override
    public Violation detect(Tuple tuple) throws BleachException {
        int tid = tuple.getIntegerByField("tid");
        String left_value = tuple.getStringByField("left_attr");
        String right_value = tuple.getStringByField("right_attr");
        String cond_value = tuple.getStringByField("cond_attr");
        if(predicate.test(cond_value)){
            return getViolation(tid, left_value, right_value);
        } else {
            return new Violation.NullViolation(tid, rid);
        }
    }

    @Override
    public Violation detect(int tid, DataTuple datatuple) throws BleachException {
        String left_value = (String)datatuple.get(left_attr);
        String right_value = (String)datatuple.get(right_attr);
        String cond_value = (String)datatuple.get(cond_attr);
        if(predicate.test(cond_value)){
            return getViolation(tid, left_value, right_value);
        } else {
            return new Violation.NullViolation(tid, rid);
        }
    }

    @Override
    public Collection<String> getAttrs() {
        Collection<String> attrs = new HashSet();
        attrs.add(left_attr);
        attrs.add(right_attr);
        attrs.add(cond_attr);
        return attrs;
    }

    @Override
    public String toString(){
        return "CFD " + rid + " = (" + left_attr + ", " + right_attr + ", cond_attr: " + predicate + ")";
    }

    @Override
    public int hashCode(){
        return (rid << 24) + (right_attr.hashCode() << 16) + (left_attr.hashCode() << 8) + cond_attr.hashCode();
    }

}
