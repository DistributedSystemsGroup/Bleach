package storm.dataclean.auxiliary.base;

import java.util.Collection;
import java.util.function.Predicate;

/**
 * Created by yongchao on 3/8/16.
 */
public abstract class AbstractSuperCell {

    public abstract boolean add(int tid);

    public abstract Collection<Integer> getTids();

    public abstract int size();

    public abstract AbstractSuperCell copy();

    // ideally return overlapping cells(tids), but it is only true in ComWinSuperCell
    public abstract Collection<Integer> merge(AbstractSuperCell s);

    public abstract boolean filter(Predicate<Integer> tp);

}
