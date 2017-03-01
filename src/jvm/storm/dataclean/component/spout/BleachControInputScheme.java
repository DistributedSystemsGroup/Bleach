package storm.dataclean.component.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.dataclean.util.BleachConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yongchao on 11/6/15.
 */
public class BleachControInputScheme implements Scheme {

    public String del;

    public BleachControInputScheme(BleachConfig config){
        del = config.get(BleachConfig.KAFKA_DATA_DELIMITER);
    }

    public static String deserializeString(byte[] string) {
        try {
            return new String(string);
        } catch (Exception var) {
            throw new RuntimeException(var);
        }
    }

    @Override
    public List<Object> deserialize(byte[] bytes) {
        String value = deserializeString(bytes);
        String[] values = value.split(del, -1);
        return new Values(values);
    }

    @Override
    public Fields getOutputFields() {
        ArrayList<String> newattrs = new ArrayList<>();
        newattrs.add("control-msg-id");
        newattrs.add("control-msg");
        return new Fields(newattrs);
    }
}
