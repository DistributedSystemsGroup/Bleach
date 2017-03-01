package storm.dataclean.auxiliary.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yongchao on 3/2/16.
 */
public class WinSuperCell extends AbstractSuperCell implements Windowing {

    public int win_cursor;
    private int win_step;

    private Collection<Integer> first_tids;
    private Collection<Integer> second_tids;

    public boolean merged;

    public WinSuperCell(){
        merged = false;
        first_tids = new ArrayList();
        second_tids = new ArrayList();

    }

    public WinSuperCell(int cursor, int step){
        this();
        win_cursor = cursor;
        win_step = step;
    }


    public WinSuperCell(int tid, int cursor, int step){
        this(cursor, step);
        second_tids.add(tid);
    }

    public WinSuperCell(int cursor, int step, Collection<Integer> first, Collection<Integer> second, boolean is_merged){
        win_cursor = cursor;
        win_step = step;
        first_tids = first;
        second_tids = second;
        merged = is_merged;
    }

    @Override
    public boolean add(int tid){
        return second_tids.add(tid);
    }

    @Override
    public Collection<Integer> getTids(){
        try{
            return Stream.concat(first_tids.stream(), second_tids.stream()).collect(Collectors.toList());
        } catch( ConcurrentModificationException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int size(){
        return first_tids.size() + second_tids.size();
    }

    @Override
    public WinSuperCell copy(){
        if(merged){
            return new WinSuperCell(win_cursor, win_step, new HashSet(first_tids), new HashSet(second_tids), merged);
        } else {
            return new WinSuperCell(win_cursor, win_step, new ArrayList(first_tids), new ArrayList(second_tids), merged);
        }
    }

    public WinSuperCell copy(boolean ismerged){
        if(ismerged){
            return new WinSuperCell(win_cursor, win_step, new HashSet(first_tids), new HashSet(second_tids), merged);
        } else {
            return new WinSuperCell(win_cursor, win_step, new ArrayList(first_tids), new ArrayList(second_tids), merged);
        }
    }

    @Override
    public Collection<Integer> merge(AbstractSuperCell sc) {
        if(!merged) {
            merged = true;
            first_tids = new HashSet(first_tids);
            second_tids = new HashSet(second_tids);
        }
        first_tids.addAll(((WinSuperCell)sc).getFirst_tids());
        second_tids.addAll(((WinSuperCell)sc).getSecond_tids());
        return null;
    }

    @Override
    public boolean filter(Predicate<Integer> tp) {
        first_tids.removeIf(tp.negate());
        second_tids.removeIf(tp.negate());
        if(size() == 0){
            return false; // supercell become an empty supercell.
        } else {
            return true;
        }
    }

    public Collection<Integer> getFirst_tids(){
        return first_tids;
    }

    public Collection<Integer> getSecond_tids(){
        return second_tids;
    }

    @Override
    public boolean updateWindow(int tid) {
        if(tid > win_cursor) {
            first_tids = second_tids;
            if(merged){
                second_tids = new HashSet();
            } else {
                second_tids = new ArrayList();
            }
//            second_tids = new ArrayList(); // TODO!!! should judge if it's arraylist or hashset
            win_cursor += win_step;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString(){
        return "[ supercell: " + first_tids.toString() + ", " + second_tids + "]";
    }
}
