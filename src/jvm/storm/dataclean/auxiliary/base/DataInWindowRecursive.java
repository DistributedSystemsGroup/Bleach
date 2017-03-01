package storm.dataclean.auxiliary.base;


import storm.dataclean.exceptions.ImpossibleException;

import java.util.*;

/**
 * Created by yongchao on 1/3/16.
 *  We assume sliding step is half of window size
 */
public class DataInWindowRecursive<K, T extends Windowing> implements Windowing{

    public Map<K, T> first_history;
    public Map<K, T> second_history;

    public int win_cursor;
    public int win_step;

    private boolean from_first;

    public DataInWindowRecursive(int start, int step){
        win_cursor = start;
        win_step = step;
        first_history = new HashMap();
        second_history = new HashMap();
    }

    public boolean containsKey(K v){
        return second_history.containsKey(v) || first_history.containsKey(v);
    }

    public boolean containsKeyInFirst(K v){
        return first_history.containsKey(v);
    }

    public Collection<K> keySet(){
//        return Stream.concat(first_history.keySet().stream(), second_history.keySet().stream())
//                .collect(Collectors.toList());
        Collection<K> result = new ArrayList();
        for(K k : first_history.keySet()) result.add(k);
        for(K k : second_history.keySet()) result.add(k);
        return result;
    }

    public Collection<T> values(){
//        return Stream.concat(first_history.values().stream(), second_history.values().stream())
//                .collect(Collectors.toList());
        Collection<T> result = new ArrayList();
        for(T t : first_history.values()) result.add(t);
        for(T t : second_history.values()) result.add(t);
        return result;
    }

    // we use list instead of set, because two histories may have the same pair, we want reserve both of them.
    public List<Map.Entry<K,T>> entrySet(){
//        return Stream.concat(first_history.entrySet().stream(), second_history.entrySet().stream())
//                .collect(Collectors.toList());
        List<Map.Entry<K,T>> result = new ArrayList();
        for(Map.Entry<K,T> entry : first_history.entrySet()) result.add(entry);
        for(Map.Entry<K,T> entry : second_history.entrySet()) result.add(entry);
        return result;
    }


    public T get(K v){
        T t = first_history.get(v);
        if (t == null) {
            from_first = false;
            return second_history.get(v);
        }
        from_first = true;
        return t;
    }

    public Void putFirst(K v, T t) {
        first_history.put(v, t);
        return null;
    }

    public Void put(K v, T t) {
        second_history.put(v, t);
        return null;
    }

    public Void put(K v, T t, boolean first) {
        if(first){
            first_history.put(v, t);
        } else
        {
            second_history.put(v, t);
        }
        return null;
    }

    public void putIfAbsent(K v, T t) {
        if(first_history.containsKey(v)){
            second_history.put(v, first_history.get(v));
            return;
        }
        second_history.putIfAbsent(v, t);
    }

    public void update(K v, T t) {
        if(from_first){
            first_history.remove(v);
            second_history.put(v, t);
        }
    }

    public void update_singleton() throws ImpossibleException {
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

    public void update(K v){
        if(first_history.containsKey(v)){
            second_history.put(v, first_history.get(v));
            first_history.remove(v);
        }
    }

    public void update_value(K v, T t){
        if (first_history.containsKey(v)) {
//            first_history.remove(v);
            first_history.put(v, t);
        } else if(second_history.containsKey(v)){
            second_history.put(v, t);
        } else {
            // debug use
            System.err.println("DataInWindowRecursive update_value failed, v " + v + " does not exist");
        }
    }

    public void putAll(DataInWindowRecursive<K, T> other){
        first_history.putAll(other.getFirst_history());
        second_history.putAll(other.getSecond_history());
    }

    public void putAll(Map<K, T> other){
        second_history.putAll(other);
    }

    @Override
    public boolean updateWindow(int tid){
        if (tid > win_cursor) {
            for(Map.Entry<K, T> entry : second_history.entrySet()){
                entry.getValue().updateWindow(tid);
            }
            first_history = second_history;
            second_history = new HashMap();
            win_cursor += win_step;
            return true;
        }
        return false;
    }

    public int size(){
        return first_history.size() + second_history.size();
    }

    public K getFirstKey(){
        if(first_history.size() == 0){
            return second_history.entrySet().iterator().next().getKey();
        } else {
            return first_history.entrySet().iterator().next().getKey();
        }
    }

    public T getFirstValue(){
        if(first_history.size() == 0){
            return second_history.entrySet().iterator().next().getValue();
        } else {
            return first_history.entrySet().iterator().next().getValue();
        }
    }

    public int getWin_cursor(){
        return win_cursor;
    }

    public int getWin_step(){
        return win_step;
    }

    public Map<K, T> getFirst_history(){
        return first_history;
    }

    public Map<K, T> getSecond_history(){
        return second_history;
    }

    public void resetFirst_history(Map<K, T> first_history){
        this.first_history = first_history;
    }

    public void resetSecond_history(Map<K, T> second_history){
        this.second_history = second_history;
    }

//    public

    @Override
    public String toString(){
        return "first_history: " + first_history + ", second_history: " + second_history;
    }


}
