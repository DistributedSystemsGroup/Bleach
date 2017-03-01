package storm.dataclean.auxiliary.detect;

import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.exceptions.BleachException;

/**
 * Created by tian on 17/12/2015.
 *  This can have many kinds of implementation, in-memory, in-disk (MapDB, BerkeleyDB...)
 */
public abstract class AbstractDataHistory {

    public int rid;
    public String schema[];
    public int right_attr_index;

    // debug use
    public String DEBUG_PREFIX = "DEBUGPRINT: ";

    public static AbstractDataHistory create(int rid, int rattr){
        return new BasicDataHistory(rid, rattr);
    }

    public static AbstractDataHistory create(int rid, int rattr, int win, int win_size){
        if(win == 0){
            System.err.println("create nowindow");
            return new BasicDataHistory(rid, rattr);
        } else if(win == 1){
            System.err.println("create basic window");
            return new WinDataHistory(rid, rattr, win_size);
        } else if(win == 2){
            System.err.println("create bleach window, but it is still basic window, " +
                    "the only difference is in repair worker");
            return new WinDataHistory(rid, rattr, win_size);
//            return null;
        } else {
            System.err.println("Bleach: unkown windowing option in detect worker");
            return null;
        }
    }

    public abstract Violation getViolation(int tid, Object leftvalue, Object rightvalue) throws BleachException;

    public abstract void print_log();

}
