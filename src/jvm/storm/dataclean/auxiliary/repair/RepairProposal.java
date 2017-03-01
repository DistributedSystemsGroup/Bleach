package storm.dataclean.auxiliary.repair;


import storm.dataclean.auxiliary.repair.subgraph.AbstractSubGraph;
import storm.dataclean.exceptions.BleachException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by tian on 20/11/2015.
 */
public class RepairProposal {

    public HashMap<Integer, HashMap<Object, Integer>> proposals;    // currently it is attrid -> valuecount mapping

    public RepairProposal(){
        proposals = new HashMap();
    }

    public void addProposal(AbstractSubGraph eqclass){
        HashMap<Object, Integer> result = eqclass.getTopObjectWithCount();
        proposals.put(eqclass.getAttrid(), result);
    }

    public Set<Map.Entry<Integer, HashMap<Object, Integer>>> getEntrySet(){
        return proposals.entrySet();
    }

    public void merge(RepairProposal rp){
        for(Map.Entry<Integer, HashMap<Object, Integer>> entry : rp.getEntrySet()){
            int attr = entry.getKey();
            HashMap<Object, Integer> new_value_count_map = entry.getValue();
            if(proposals.containsKey(attr)){
                HashMap<Object, Integer> exist_value_count_map = proposals.get(attr);
                for(Map.Entry<Object, Integer> entry2 : new_value_count_map.entrySet()){
                    Object value = entry2.getKey();
                    int count = entry2.getValue();
                    if(exist_value_count_map.containsKey(value)){
                        exist_value_count_map.put(value, exist_value_count_map.get(value)+count);
                    } else {
                        exist_value_count_map.put(value, count);
                    }
                }
            } else {
                proposals.put(attr, new_value_count_map);
            }
        }
    }

    public List<Object> getFixedTuple(List<Object> original_tuple) throws BleachException {
        List<Object> fields = original_tuple;
        for(Map.Entry<Integer, HashMap<Object, Integer>> attr_proposal : proposals.entrySet()){
            int attr_id = attr_proposal.getKey();
            HashMap<Object, Integer> gathered_proposal = attr_proposal.getValue();
            int max_num = -1;
            Object max_value = null;
            for(Map.Entry<Object, Integer> value_num : gathered_proposal.entrySet()){
                int num = value_num.getValue();
                Object v = value_num.getKey();
                if(num > max_num){
                    max_value = v;
                    max_num = num;
                } else if(num == max_num){
                    if(v.equals(original_tuple.get(attr_id))){ // TODO to check
                        max_value = v;
                    }
                }
            }
            if(max_value == null){
                throw new BleachException("attrid = " + attr_id + " is null");
            }
            fields.set(attr_id, max_value);
        }
        return fields;
    }

    @Override
    public String toString(){
        StringBuilder str = new StringBuilder();
        str.append("[");
        for(Map.Entry<Integer, HashMap<Object, Integer>> entry : proposals.entrySet()){
            str.append("attrID=" + entry.getKey() + " possiblevalue: " + entry.getValue());
        }
        return str.toString();
    }

}
