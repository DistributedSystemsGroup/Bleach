package storm.dataclean.auxiliary.repair.overlap;

import storm.dataclean.auxiliary.base.ComWinSuperCell;
import storm.dataclean.auxiliary.base.ViolationCause;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yongchao on 3/9/16.
 *  Overlapping cells, since we use windowing, we use windowing for tid_vcs_map, and vc left in the windowing
 */
public class BleachOverlpCells {

    public Map<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> records;
    public Map<Integer, Collection<ViolationCause>> tid_vcs_map;

    public int win_cursor;
    public int win_step;

    public BleachOverlpCells(int wcursor, int wstep){
        records = new HashMap();
        tid_vcs_map = new HashMap();
        win_cursor = wcursor;
        win_step = wstep;
    }

    private BleachOverlpCells(Map<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> a, Map<Integer, Collection<ViolationCause>> b, int wcursor, int wstep){
        records = a;
        tid_vcs_map = b;
        win_cursor = wcursor;
        win_step = wstep;
    }

    public void addFromCellVcHistory(int tid, Object value, Collection<ViolationCause> vcs){
        if(tid_vcs_map.containsKey(tid)){
            Collection<ViolationCause> old_vcs = tid_vcs_map.get(tid);
            if(old_vcs.equals(vcs)){
                return;
            }
            HashMap<Object, ComWinSuperCell> old_subset = records.get(old_vcs);
            ComWinSuperCell old_sc = old_subset.get(value);
            old_sc.remove(tid);
            if(old_sc.size() == 0){
                old_subset.remove(value);
            }
            if(old_subset.size()==0){
                records.remove(old_vcs);
            }
        }
        tid_vcs_map.put(tid, vcs);
        HashMap<Object, ComWinSuperCell> subset;
        if(records.containsKey(vcs)){
            subset = records.get(vcs);
            if(subset.containsKey(value)){
                subset.get(value).add(tid); // TODO need to specify to addFromCellVcHistory to first or second set
            } else {
                subset.put(value, new ComWinSuperCell(tid, win_cursor, win_step));
            }
        } else {
            subset = new HashMap();
            subset.put(value, new ComWinSuperCell(tid, win_cursor, win_step));
            records.put(vcs, subset);
        }
    }

    public Map<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> getRecords(){
        return records;
    }

    public Map<Integer, Collection<ViolationCause>> getTid_vcs_map(){
        return tid_vcs_map;
    }

    public void delete_rule(int rid){
        tid_vcs_map.values().forEach(x -> x.removeIf(y -> y.getRuleid() == rid));
        tid_vcs_map.entrySet().removeIf(x -> x.getValue().size() <= 1);
        HashMap<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> newrecords = new HashMap();
        for(Map.Entry<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> entry : records.entrySet()){
            Collection<ViolationCause> vcs = entry.getKey();
            vcs.removeIf(x -> x.getRuleid() == rid);
            if(vcs.size() > 1) {
                if (newrecords.containsKey(vcs)) {
                    HashMap<Object, ComWinSuperCell> existing_scs = newrecords.get(vcs);
                    HashMap<Object, ComWinSuperCell> scs = entry.getValue();
                    for (Map.Entry<Object, ComWinSuperCell> entry2 : scs.entrySet()) {
                        if (existing_scs.containsKey(entry2.getKey())) {
                            existing_scs.get(entry2.getKey()).merge(entry2.getValue());
                        } else {
                            existing_scs.put(entry2.getKey(), entry2.getValue());
                        }
                    }
                } else {
                    newrecords.put(vcs, entry.getValue());
                }
            }
        }
        records = newrecords;
    }

    /**
     * only invoked from updateWindow, as a way to delete obsolete vcs
     * @param dvcs
     */
    private void delete_vcs(Collection<ViolationCause> dvcs){
        HashMap<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> newrecords = new HashMap();
        for(Map.Entry<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> entry : records.entrySet()){
            Collection<ViolationCause> vcs = entry.getKey();
            vcs.removeIf(x -> dvcs.contains(x));
            if(vcs.size() > 1) {
                if (newrecords.containsKey(vcs)) {
                    HashMap<Object, ComWinSuperCell> existing_scs = newrecords.get(vcs);
                    HashMap<Object, ComWinSuperCell> scs = entry.getValue();
                    for (Map.Entry<Object, ComWinSuperCell> entry2 : scs.entrySet()) {
                        if (existing_scs.containsKey(entry2.getKey())) {
                            existing_scs.get(entry2.getKey()).merge(entry2.getValue());
                        } else {
                            existing_scs.put(entry2.getKey(), entry2.getValue());
                        }
                    }
                } else {
                    newrecords.put(vcs, entry.getValue());
                }
            }
        }
        records = newrecords;
    }

    public BleachOverlpCells getSubsetbySid(Collection<ViolationCause> sid){
        Map<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> sub_record = new HashMap();
        for(Map.Entry<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> entry : records.entrySet()){
            if(!Collections.disjoint(entry.getKey(), sid)){
                sub_record.put(entry.getKey(), entry.getValue());
            }
        }
        Map<Integer, Collection<ViolationCause>> sub_tid_vcs = new HashMap();
        for(Map.Entry<Integer, Collection<ViolationCause>> entry : tid_vcs_map.entrySet()){
            if(!Collections.disjoint(entry.getValue(), sid)){
                sub_tid_vcs.put(entry.getKey(), entry.getValue());
            }
        }
        return new BleachOverlpCells(sub_record, sub_tid_vcs, win_cursor, win_step);
    }

    public void updateWindow(int tid, Collection<ViolationCause> vcs){
        if(tid > win_cursor)
        {
            int thre = win_cursor - win_step;
            Map<Integer, Collection<ViolationCause>> new_tid_vcs = new HashMap();
            for(Map.Entry<Integer, Collection<ViolationCause>> entry : tid_vcs_map.entrySet()){
                if(entry.getKey() > thre){
                    new_tid_vcs.put(entry.getKey(),entry.getValue());
                }
            }
            tid_vcs_map = new_tid_vcs;
            if(vcs.size() > 0)
            {
                delete_vcs(vcs);
            }
            for(Map.Entry<Collection<ViolationCause>, HashMap<Object, ComWinSuperCell>> entry : records.entrySet()){
                for(ComWinSuperCell sc : entry.getValue().values()){
                    sc.updateWindow(tid);
                }
            }
            win_cursor += win_step;
        }
    }

    public void reset(){
        records.entrySet().stream().forEach(
                x ->{
                    x.getValue().entrySet().removeIf(y->y.getValue().isEmpty());
                    x.getValue().entrySet().forEach(y->y.getValue().reset());}
        );
    }


    // attention that this merge is not enough, must be followed by complementary
    public void merge(BleachOverlpCells olc){
        records.putAll(olc.getRecords());
        Map<Integer, Collection<ViolationCause>> other_tid_vcs_map = olc.getTid_vcs_map();
        for(Map.Entry<Integer, Collection<ViolationCause>> entry : other_tid_vcs_map.entrySet()){
            int tid = entry.getKey();
            Collection<ViolationCause> vcs = entry.getValue();
            if(tid_vcs_map.containsKey(tid)){
                tid_vcs_map.get(tid).addAll(vcs);
            } else {
                tid_vcs_map.put(tid, vcs);
            }
        }
    }

}
