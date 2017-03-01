package storm.dataclean.auxiliary.rule;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.detect.DataTuple;
import storm.dataclean.exceptions.BleachException;

import java.util.Collection;
import java.util.List;

/**
 * Created by yongchao on 11/13/15.
 */
public interface Rule {

    Values getSubtuple(int tid, Tuple tuple);

    Violation detect(Tuple tuple) throws BleachException;

    Violation detect(int tid, DataTuple datatuple)  throws BleachException;

    String getValueAttr();

    Collection<String> getAttrs();

    int getRid();

    // debug use
    public void print_log();
}
