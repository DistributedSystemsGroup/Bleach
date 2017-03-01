package storm.dataclean.auxiliary.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.function.Predicate;

/** To Do
 * Created by yongchao on 3/1/16.
 */
public class ComWinSuperCell extends AbstractSuperCell implements Windowing {

    public int win_cursor;
    public int win_step;
    private Collection<Integer> first_tids;
    private Collection<Integer> second_tids;

    private int size;
    public boolean merged;

    public ComWinSuperCell(){
        merged = false;
        first_tids = new ArrayList();
        second_tids = new ArrayList();
        size = 0;
    }

    public ComWinSuperCell(int cursor, int step){
        this();
        win_cursor = cursor;
        win_step = step;
    }

    public ComWinSuperCell(int tid, int wcursor, int wstep){
        this(wcursor, wstep);
        if(tid > (win_cursor - win_step)){
            second_tids.add(tid);
        } else {
            first_tids.add(tid);
        }
        size++;
    }

    public ComWinSuperCell(int cursor, int step, Collection<Integer> first, Collection<Integer> second, boolean ismerged, int s){
        win_cursor = cursor;
        win_step = step;
        first_tids = first;
        second_tids = second;
        merged = ismerged;
        size = s;
    }


    public ComWinSuperCell(int cursor, int step, WinSuperCell scw){
        WinSuperCell scw_copy = scw.copy();
        first_tids = scw_copy.getFirst_tids();
        second_tids = scw_copy.getSecond_tids();
        size = scw_copy.size();
        win_cursor = cursor;
        win_step = step;
    }


    public void addAll(WinSuperCell sc){
        if(merged) {
            int asize = 0;
            for (int tid : sc.getFirst_tids()) {
                if (!first_tids.contains(tid)) {
                    first_tids.add(tid);
                    asize++;
                }
            }
            for (int tid : sc.getSecond_tids()) {
                if (!second_tids.contains(tid)) {
                    second_tids.add(tid);
                    asize++;
                }
            }
            size += asize;
        } else {
            // very tricky here. If code reaches here, second_tids and first_tids are arraylists.
            // And the first_tid must already cover all the cells in sc.getFirst_tids().
            second_tids.addAll(sc.getSecond_tids());
            size += sc.getSecond_tids().size();
        }
    }

    @Override
    public Collection<Integer> merge(AbstractSuperCell scc){
        if(!merged){
            merged = true;
            first_tids = new HashSet(first_tids);
            second_tids = new HashSet(second_tids);
        }
        ComWinSuperCell sc = (ComWinSuperCell)scc;
        size = size + sc.size();
        Collection<Integer> overlp_tids = new HashSet(); // or arraylist?
        for(int tid : sc.getFirst_tids()){
            if(first_tids.contains(tid)){
                overlp_tids.add(tid);
            } else {
                first_tids.add(tid);
            }
        }
        for(int tid : sc.getSecond_tids()){
            if(second_tids.contains(tid)){
                overlp_tids.add(tid);
            } else {
                second_tids.add(tid);
            }
        }
        return overlp_tids;
    }

    public void reduceDuplicate(int k){
        size -= k;
    }

    @Override
    public boolean updateWindow(int tid) {
        if(tid > win_cursor) {
            first_tids = second_tids;
            second_tids = new HashSet();
            win_cursor += win_step;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean add(int tid) {
        if(second_tids.add(tid)){
            size++;
            return true;
        } else {
            return false;
        }
    }

    public void remove(int tid) {
        if(first_tids.remove(tid) || second_tids.remove(tid)){
            size--;
        }
    }

    @Override
    public Collection<Integer> getTids() {
        System.err.println("Bleach: supercellwincom does not support getTids method");
        return null;
    }

    @Override
    public int size() {
        return size;
    }

    public boolean isEmpty(){
        return first_tids.size()+second_tids.size() == 0;
    }

    @Override
    public ComWinSuperCell copy() {
        if(merged){
            return new ComWinSuperCell(win_cursor, win_step, new HashSet(first_tids), new HashSet(second_tids), merged, size);
        } else {
            return new ComWinSuperCell(win_cursor, win_step, new ArrayList(first_tids), new ArrayList(second_tids), merged, size);
        }

    }


    public ComWinSuperCell copy(boolean ismerged) {
        if(ismerged){
            return new ComWinSuperCell(win_cursor, win_step, new HashSet(first_tids), new HashSet(second_tids), merged, size);
        } else {
            return new ComWinSuperCell(win_cursor, win_step, new ArrayList(first_tids), new ArrayList(second_tids), merged, size);
        }

    }

    @Override
    public boolean filter(Predicate<Integer> tp) {
        System.err.println("Bleach: supercellwincom does not support filter method");
        return false;
    }

    public Collection<Integer> getFirst_tids(){
        return first_tids;
    }

    public Collection<Integer> getSecond_tids(){
        return second_tids;
    }

    @Override
    public String toString(){
        return "[ComWinSuperCell size: " + size +  ": " + first_tids + ", " + second_tids + "]";
    }

    public void reset(){
        size = first_tids.size() + second_tids.size();
    }

}
