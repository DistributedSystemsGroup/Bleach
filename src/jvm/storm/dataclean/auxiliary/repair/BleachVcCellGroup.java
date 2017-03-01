package storm.dataclean.auxiliary.repair;

import storm.dataclean.auxiliary.base.ComWinSuperCell;
import storm.dataclean.auxiliary.base.ViolationCause;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.ImpossibleException;

import java.util.*;

/**
 * Created by yongchao on 3/20/16.
 */
public class BleachVcCellGroup {
    public Map<ViolationCause, HashMap<Object, ComWinSuperCell>> first_history;
    public Map<ViolationCause, HashMap<Object, ComWinSuperCell>> second_history;

    public int win_cursor;
    public int win_step;

    public BleachVcCellGroup(int start, int step){
        win_cursor = start;
        win_step = step;
        first_history = new HashMap();
        second_history = new HashMap();
    }

    public boolean containsKey(ViolationCause v){
        return first_history.containsKey(v) || second_history.containsKey(v);
    }

    public boolean containsKeyInFirst(ViolationCause v){
        return first_history.containsKey(v);
    }


    public Collection<ViolationCause> keySet(){
//        return Stream.concat(first_history.keySet().stream(), second_history.keySet().stream())
//                .collect(Collectors.toList());
        Collection<ViolationCause> result = new ArrayList();
        for(ViolationCause vc : first_history.keySet()) result.add(vc);
        for(ViolationCause vc : second_history.keySet()) result.add(vc);
        return result;
    }

    public Collection<HashMap<Object, ComWinSuperCell>> values(){
//        return Stream.concat(first_history.values().stream(), second_history.values().stream())
//                .collect(Collectors.toList());
        Collection<HashMap<Object, ComWinSuperCell>> result = new ArrayList();
        for(HashMap<Object, ComWinSuperCell> map : first_history.values()) result.add(map);
        for(HashMap<Object, ComWinSuperCell> map : second_history.values()) result.add(map);
        return result;


    }

    public List<Map.Entry<ViolationCause,HashMap<Object, ComWinSuperCell>>> entrySet(){
//        return Stream.concat(first_history.entrySet().stream(), second_history.entrySet().stream())
//                .collect(Collectors.toList());
        List<Map.Entry<ViolationCause, HashMap<Object, ComWinSuperCell>>> result = new ArrayList();
        for(Map.Entry<ViolationCause, HashMap<Object, ComWinSuperCell>> entry : first_history.entrySet()) result.add(entry);
        for(Map.Entry<ViolationCause, HashMap<Object, ComWinSuperCell>> entry : second_history.entrySet()) result.add(entry);
        return result;
    }


    public HashMap<Object, ComWinSuperCell> get(ViolationCause v){
        HashMap<Object, ComWinSuperCell> t = first_history.get(v);
        if (t == null) {
            return second_history.get(v);
        }
        return t;
    }

    public Void put(ViolationCause v, HashMap<Object, ComWinSuperCell> t) {
        second_history.put(v, t);
        return null;
    }

    public Void put(ViolationCause v, HashMap<Object, ComWinSuperCell> t, boolean first) {
     if(first){
         first_history.put(v, t);
     } else {
         second_history.put(v, t);
     }
        return null;
    }

    public void putIfAbsent(ViolationCause v, HashMap<Object, ComWinSuperCell> t) {
        if(first_history.containsKey(v)){
            return;
        }
        second_history.putIfAbsent(v, t);
    }

    public void update(ViolationCause v, HashMap<Object, ComWinSuperCell> t) {
        if (first_history.containsKey(v)) {
            first_history.remove(v);
            second_history.put(v, t);
        }
    }

    public void update(ViolationCause v){
        if (first_history.containsKey(v)) {
            second_history.put(v, first_history.get(v));
            first_history.remove(v);
        }
    }


    public void update_singleton() throws BleachException{
        if( (first_history.size() + second_history.size()) > 1){
            throw new ImpossibleException("This should be singleton!");
        }
        else {
            if(first_history.size()>0){
                second_history = first_history;
                first_history = new HashMap();
            }
        }
    }

    public void putAll(BleachVcCellGroup other){
        first_history.putAll(other.getFirst_history());
        second_history.putAll(other.getSecond_history());
    }

    /**
     * @return deleted keys which are in the first_history
     */
    public Collection<ViolationCause> updateWindowWithReturn(int tid){
        if(tid > win_cursor) {
            Collection<ViolationCause> returned_values = first_history.keySet();
            updateWindow(tid);
            return returned_values;
        } else {
            return null;
        }
    }


    private boolean updateWindow(int tid){
        if (tid > win_cursor) {
//            second_history.values().stream().forEach(x -> x.values().stream().forEach(y -> y.updateWindow(tid)));
            for(HashMap<Object, ComWinSuperCell> map : second_history.values()){
                for(ComWinSuperCell sc : map.values()){
                    sc.updateWindow(tid);
                }
            }
            first_history = second_history;
            second_history = new HashMap();


            return true;
        }
        return false;
    }

    public int size(){
        return first_history.size() + second_history.size();
    }

    public int getWin_cursor(){
        return win_cursor;
    }

    public int getWin_step(){
        return win_step;
    }

    public Map<ViolationCause, HashMap<Object, ComWinSuperCell>> getFirst_history(){
        return first_history;
    }

    public Map<ViolationCause, HashMap<Object, ComWinSuperCell>> getSecond_history(){
        return second_history;
    }

    public void reset(){
        first_history.entrySet().stream().forEach(
                x ->{
                    x.getValue().entrySet().removeIf(y->y.getValue().isEmpty());
                        x.getValue().entrySet().forEach(y->y.getValue().reset());}
        );
        second_history.entrySet().stream().forEach(
                x ->{
                        x.getValue().entrySet().removeIf(y->y.getValue().isEmpty());
                    x.getValue().entrySet().forEach(y->y.getValue().reset());}
        );
    }


    public void delete_rule(int rid){
//        first_history = first_history.entrySet().stream().filter(x->x.getKey().getRuleid()!=rid).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
//        second_history = second_history.entrySet().stream().filter(x->x.getKey().getRuleid()!=rid).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
        Map<ViolationCause, HashMap<Object, ComWinSuperCell>> new_first_history = new HashMap();
        Map<ViolationCause, HashMap<Object, ComWinSuperCell>> new_second_history = new HashMap();
        for(Map.Entry<ViolationCause, HashMap<Object, ComWinSuperCell>> entry : first_history.entrySet()){
            if(entry.getKey().getRuleid() != rid){
                new_first_history.put(entry.getKey(),entry.getValue());
            }
        }
        for(Map.Entry<ViolationCause, HashMap<Object, ComWinSuperCell>> entry : second_history.entrySet()){
            if(entry.getKey().getRuleid() != rid){
                new_second_history.put(entry.getKey(),entry.getValue());
            }
        }
        first_history = new_first_history;
        second_history = new_second_history;

    }

    @Override
    public int hashCode(){
        return first_history.keySet().hashCode() + second_history.keySet().hashCode();
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof BleachVcCellGroup){
            BleachVcCellGroup o2 = (BleachVcCellGroup)o;
            if(first_history.keySet().equals(o2.getFirst_history().keySet()) &&
                    second_history.keySet().equals(o2.getSecond_history().keySet())){
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }


}
