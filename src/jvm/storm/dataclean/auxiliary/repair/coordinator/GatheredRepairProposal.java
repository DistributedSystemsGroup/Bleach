package storm.dataclean.auxiliary.repair.coordinator;

import storm.dataclean.auxiliary.repair.RepairProposal;
import storm.dataclean.exceptions.BleachException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tian on 24/11/2015.
 */
public class GatheredRepairProposal extends RepairProposal {

    public List<Object> original_tuple;
    public HashMap<Integer, Object> attrid_value_map;
    public Collection<RepairProposal> rplist;


    public GatheredRepairProposal(){
        super();
        attrid_value_map = new HashMap();
    }

    public GatheredRepairProposal(List<Object> tuple, Collection<RepairProposal> list){
        this();
        original_tuple = tuple;
        rplist = list;
    }

    public List<Object> getFixedTuple() throws BleachException {
        for(RepairProposal rp : rplist){
            for(Map.Entry<Integer, HashMap<Object, Integer>> attr_proposal : rp.getEntrySet()){
                int attr_id = attr_proposal.getKey();
                HashMap<Object, Integer> proposal = attr_proposal.getValue();
                if(proposals.containsKey(attr_id)){
                    HashMap<Object, Integer> existing_proposal = proposals.get(attr_id);
                    for(Map.Entry<Object, Integer> value_num : proposal.entrySet()){
                        Object v = value_num.getKey();
                        int num = value_num.getValue();
                        if(existing_proposal.containsKey(v)){
                            existing_proposal.put(v, num + existing_proposal.get(v));
                        } else {
                            existing_proposal.put(v, num);
                        }
                    }
                } else {
                    proposals.put(attr_id, proposal);
                }
            }
        }

        // Begin to construct new tuple
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
                    if(v.equals(original_tuple.get(attr_id))){
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
}
