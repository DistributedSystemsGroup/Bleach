package storm.dataclean.component.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dataclean.util.BleachConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yongchao on 11/6/15.
 */
public class BleachDataInputScheme implements Scheme {

    public List<String> attributes;

    public int counter;
    public String del;

    public String nothing = "";


    private static final Logger LOG = LoggerFactory.getLogger(BleachDataInputScheme.class);

    public BleachDataInputScheme(BleachConfig config){
        String schemestring = config.get(BleachConfig.SCHEMA);
        attributes = Arrays.asList(schemestring.split(","));
        counter = 0;
        del = config.get(BleachConfig.KAFKA_DATA_DELIMITER);
        System.out.println("delimiter: " + del);
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
        Object[] objects = new Object[values.length];
        objects[0] = Integer.parseInt(values[0]);

        for(int i = 1; i < objects.length; i++){
            objects[i] = values[i];
        }

        counter++;
        if(counter == 100){
            counter = 0;
            ArrayList<Object> a = new ArrayList<>();
            for(Object o : objects)
            {
                a.add(o);
                break;
            }
            a.add(nothing);
            LOG.info("DATA_OUTPUT "+a);
        }

        return new Values(objects);
    }

    @Override
    public Fields getOutputFields() {
        ArrayList<String> newattrs = new ArrayList<>();
//        newattrs.add("kid");
        newattrs.addAll(attributes);
        return new Fields(newattrs);
    }
}
