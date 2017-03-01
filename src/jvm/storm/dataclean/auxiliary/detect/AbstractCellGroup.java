package storm.dataclean.auxiliary.detect;

import storm.dataclean.auxiliary.base.AbstractSuperCell;
import storm.dataclean.exceptions.BleachException;

/**
 * Created by yongchao on 3/1/16.
 */
public abstract class AbstractCellGroup {

    public abstract boolean isUnique();

    public abstract boolean contain(Object o);

    public abstract void add(Object v, int tid) throws BleachException;

    public abstract Object getUniqueValue() throws BleachException;

    public abstract AbstractSuperCell getUniqueTids() throws BleachException;

    public abstract int getSuperCellNum();

    public abstract int getCellNum();
}
