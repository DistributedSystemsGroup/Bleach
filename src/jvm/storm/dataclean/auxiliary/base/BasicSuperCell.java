package storm.dataclean.auxiliary.base;

//import org.apache.storm.guava.base.Predicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Predicate;

/**
 * Created by tian on 18/12/2015.
 */
public class BasicSuperCell extends AbstractSuperCell {

    private Collection<Integer> tids;

    public BasicSuperCell(){
        tids = new HashSet();
    }

    public BasicSuperCell(int tid){
        this();
        tids.add(tid);
    }

    public BasicSuperCell(Collection<Integer> ts){
        tids = new HashSet();
        tids.addAll(ts);
    }

    @Override
    public boolean add(int tid){
        return tids.add(tid);
    }

    @Override
    public Collection<Integer> getTids(){
        return tids;
    }

    @Override
    public int size(){
        return tids.size();
    }

    @Override
    public AbstractSuperCell copy() {
        return new BasicSuperCell(new HashSet(tids));
    }

    @Override
    public Collection<Integer> merge(AbstractSuperCell s) {
        tids.addAll(s.getTids());
        return null;
    }

    @Override
    public boolean filter(Predicate<Integer> tp){
        tids.removeIf(tp.negate());
        if(tids.size() == 0){
            return false; // supercell become an empty supercell.
        } else {
            return true;
        }
    }

    @Override
    public String toString(){
        return "[ supercell: " + tids.toString() + "]";
    }

}
