package storm.dataclean.auxiliary.repair.cellvchistory;


import org.apache.storm.guava.collect.HashMultimap;
import org.apache.storm.guava.collect.Multimap;
import storm.dataclean.auxiliary.base.ViolationCause;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by tian on 01/12/2015.
 */
public class BasicCellVcHistory extends AbstractCellVcHistory {

    HashMap<Integer, Multimap<Integer, ViolationCause>> attr_cell_vc_map;

    public BasicCellVcHistory(String[] attrs){
        attr_cell_vc_map = new HashMap();
        for(int i = 0; i < attrs.length; i++){
            attr_cell_vc_map.put(i, HashMultimap.create());
        }
    }

    @Override
    public Collection<ViolationCause> getVioCauses(int attr_index, int tid){
        return new HashSet(attr_cell_vc_map.get(attr_index).get(tid));
    }

    @Override
    public void add(int attr_id, int tid, ViolationCause vc){
        attr_cell_vc_map.get(attr_id).put(tid, vc);
    }

    @Override
    public void delete_rule(int rid){
        for(Multimap<Integer, ViolationCause> cell_vc_map : attr_cell_vc_map.values()){
            cell_vc_map.entries().removeIf(x -> x.getValue().getRuleid() == rid);
        }
    }

    @Override
    public void print_log() {
        for(int attr : attr_cell_vc_map.keySet())
        {
            System.err.println(DEBUG_PREFIX + " cellvchistorynowin: attr " + attr + " has " +
                    attr_cell_vc_map.get(attr).keySet().size() + " tids(cells)");
            Multimap<Integer, ViolationCause> records = attr_cell_vc_map.get(attr);
            int most_vcs =  records.keySet().stream().mapToInt(x->records.get(x).size()).max().getAsInt();
            System.err.println(DEBUG_PREFIX + " cellvchistorynowin: attr " + attr +
                    " has at most" + most_vcs + " vcs for a cell");
        }
    }


}
