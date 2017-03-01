package storm.dataclean.auxiliary.detect;

import storm.dataclean.auxiliary.base.BasicSuperCell;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.ImpossibleException;

import java.util.*;

/**
 * Created by yongchao on 11/15/15.
 */
public class BasicCellGroup extends AbstractCellGroup {

    private HashMap<Object, BasicSuperCell> vt;

    public int last_tid;

    public BasicCellGroup(){
        vt = new HashMap();
    }

    public BasicCellGroup(Object v, int tid){
        this();
        BasicSuperCell tids = new BasicSuperCell();
        tids.add(tid);
        vt.put(v, tids);
        last_tid = tid;
    }

    public boolean isUnique(){
        return vt.size() == 1;
    }

    public boolean contain(Object o){
        return vt.containsKey(o);
    }

    public void add(Object v, int tid) throws BleachException{
        if(vt.containsKey(v)){
            vt.get(v).add(tid);
        } else {
            BasicSuperCell newtids = new BasicSuperCell();
            newtids.add(tid);
            vt.put(v, newtids);
        }
        if(last_tid > tid){
            throw new ImpossibleException("BasicCellGroup: tuple " + last_tid + " should be received before tuple " + tid);
        } else {
            last_tid = tid;
        }
    }

    public Object getUniqueValue() throws BleachException {
        if(vt.size() > 1){
            throw new BleachException("BasicCellGroup vt is not unique");
        }
        return vt.entrySet().iterator().next().getKey();
    }

    public BasicSuperCell getUniqueTids() throws BleachException {
        if(vt.size() > 1){
            throw new BleachException("BasicCellGroup vt is not unique");
        }
        return vt.entrySet().iterator().next().getValue();
    }

    @Override
    public int getSuperCellNum(){
        return vt.size();
    }

    @Override
    public int getCellNum() {
        return vt.values().stream().mapToInt(sc->sc.size()).reduce(0, (a, b) -> a + b);
    }

}

