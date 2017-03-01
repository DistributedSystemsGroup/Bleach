package storm.dataclean.auxiliary.detect;

import java.util.HashMap;

/**
 * Created by tian on 04/04/2016.
 */
public class DataTuple {
    public HashMap<String, Object> values;

    public DataTuple(){
        values = new HashMap();
    }

    public void add(String k, Object v){
        values.put(k, v);
    }

    public Object get(String k) {
        return values.get(k);
    }

    @Override
    public String toString(){
        return values.values().toString();
    }

}
