package storm.dataclean.auxiliary.detect;

import storm.dataclean.auxiliary.base.DataInWindowRecursive;
import storm.dataclean.auxiliary.base.WinSuperCell;
import storm.dataclean.auxiliary.base.Windowing;
import storm.dataclean.exceptions.BleachException;
import storm.dataclean.exceptions.ImpossibleException;


/**
 * Created by yongchao on 3/1/16.
 */
public class WinCellGroup extends AbstractCellGroup implements Windowing{

    private DataInWindowRecursive<Object, WinSuperCell> history;

    public int win_step;
    public int win_start;

    public int last_tid;

    public WinCellGroup(int cursor, int step){
        history = new DataInWindowRecursive(cursor, step);
        win_step = step;
        win_start = cursor;
    }

    public WinCellGroup(int cursor, int step, Object v, int tid){
        this(cursor, step);
        WinSuperCell sc = new WinSuperCell(win_start, win_step); // Important, to be modified here.
        sc.add(tid);
        history.put(v, sc);
        last_tid = tid;
    }

    public boolean isUnique(){
        return history.size() == 1;
    }

    public boolean contain(Object o) {
        return history.containsKey(o);
    }

    private WinSuperCell get(Object v) {
        return history.get(v);
    }

    private void put(Object v, WinSuperCell sc) {
        history.put(v, sc);
    }

    private void update(Object v, WinSuperCell sc) {
        history.update(v, sc);
    }

    public void add(Object v, int tid) throws BleachException {
        if(contain(v)){
            WinSuperCell sc = get(v);
            sc.add(tid);
            update(v, sc);
        } else {
            WinSuperCell sc = new WinSuperCell(win_start, win_step);
            sc.add(tid);
            put(v, sc);
        }
        if(last_tid > tid){
            throw new ImpossibleException("BasicCellGroup: tuple " + last_tid + " should be received before tuple " + tid);
        } else {
            last_tid = tid;
        }
    }

    public Object getUniqueValue() throws BleachException {
        if(history.size() > 1){
            throw new BleachException("WinCellGroup vt is not unique");
        }
        return history.getFirstKey();
    }

    public WinSuperCell getUniqueTids() throws BleachException {
        if(history.size() > 1){
            throw new BleachException("WinCellGroup vt is not unique");
        }
        return history.getFirstValue();
    }

    @Override
    public int getSuperCellNum() {
        return history.size();
    }

    @Override
    public int getCellNum() {
//        return history.values().stream().mapToInt(sc->sc.size()).reduce(0, (a, b) -> a + b);
        int num = 0;
        for(WinSuperCell sc : history.values()) num += sc.size();
        return num;
    }

    @Override
    public boolean updateWindow(int tid) {
//        System.err.println("Bleach: should be not reachable");
        if(tid > win_start) {
            win_start += win_step;
            return history.updateWindow(tid);
        } else {
            return false;
        }
    }

}
