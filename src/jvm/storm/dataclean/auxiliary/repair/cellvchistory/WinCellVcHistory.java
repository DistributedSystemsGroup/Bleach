package storm.dataclean.auxiliary.repair.cellvchistory;

import storm.dataclean.auxiliary.base.ViolationCause;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by tian on 01/12/2015.
 */
public class WinCellVcHistory extends AbstractCellVcHistory
{

    public String[] attributes;

    HashMap<Integer, CellVcsMap> attr_cell_vc_map;
    HashMap<Integer, CellVcsMap> old_attr_cell_vc_map;

    public int win_cursor;
    public int win_step;

    public WinCellVcHistory(String[] attrs, int start, int step){
        attributes = attrs;
        attr_cell_vc_map = genAttrCellVcMap();
        old_attr_cell_vc_map = genAttrCellVcMap();
        win_cursor = start;
        win_step = step;
    }

    public HashMap<Integer, CellVcsMap> genAttrCellVcMap(){
        HashMap<Integer, CellVcsMap> map = new HashMap();
        for(int i = 0; i < attributes.length; i++){
            map.put(i, new CellVcsMap(win_step));
        }
        return map;
    }

    public Collection<ViolationCause> getVioCauses(int attr_index, int tid){

        int tid_index = tid % win_step;

        Collection<ViolationCause> vcs;

        if(tid > (win_cursor - win_step)){
            vcs = attr_cell_vc_map.get(attr_index).get(tid_index);
        } else {
            vcs = old_attr_cell_vc_map.get(attr_index).get(tid_index);
        }

        if(vcs == null){
            return new HashSet();
        } else {
            return new HashSet(vcs);
        }
    }


    public void add(int attr_id, int tid, ViolationCause vc){
        updateWindow(tid);
        int tid_index = tid % win_step;
        CellVcsMap oldarraylist = old_attr_cell_vc_map.get(attr_id);
        CellVcsMap arraylist = attr_cell_vc_map.get(attr_id);
        if(tid > (win_cursor - win_step)){
            if(arraylist.contains(tid_index)){
                arraylist.get(tid_index).add(vc);
            } else {
                ArrayList<ViolationCause> vcs = new ArrayList();
                vcs.add(vc);
                arraylist.add(tid_index, vcs);
            }
        } else {
            if(oldarraylist.contains(tid_index)){
                oldarraylist.get(tid_index).add(vc);
            } else {
                ArrayList<ViolationCause> vcs = new ArrayList();
                vcs.add(vc);
                oldarraylist.add(tid_index, vcs);
            }
        }
    }

    @Override
    public void delete_rule(int rid) {
        attr_cell_vc_map.values().forEach(x->x.remove_rule(rid));
        old_attr_cell_vc_map.values().forEach(x->x.remove_rule(rid));
    }

    @Override
    public void print_log() {
        System.err.println("print log not implemented in CellVcHistoryWin arraylist");
    }

    public void updateWindow(int tid) {

        if(tid > win_cursor){
            old_attr_cell_vc_map = attr_cell_vc_map;
            attr_cell_vc_map = genAttrCellVcMap();
            win_cursor += win_step;
        }
    }

    private class CellVcsMap{

        public Collection<ViolationCause>[] data;

        public CellVcsMap(int win_step){
            data = new Collection[win_step];
        }

        public boolean contains(int index){
            return data[index]!=null;
        }

        public Collection<ViolationCause> get(int index){
            return data[index];
        }

        public void add(int index, Collection<ViolationCause> vcs){
            data[index] = vcs;
        }

        public void remove(int index){
            data[index] = null;
        }

        public void remove_rule(int rid){
            for(Collection<ViolationCause> vc_list : data){
                if(vc_list != null)
                {
                    vc_list.removeIf(x->x.getRuleid()==rid);
                }
            }
        }

    }



}
