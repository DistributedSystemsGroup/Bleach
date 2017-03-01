package storm.dataclean.auxiliary.base;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by tian on 01/12/2015.
 */
public class ViolationGroup {

    public int tid;
    public int kid;
    private HashMap<Integer, Collection<Violation>> attr_vio_map;
    public boolean require_coordinate;

    public ViolationGroup(){}

    public ViolationGroup(int t){
        tid = t;
        require_coordinate = false;
        attr_vio_map = new HashMap<>();
    }

    public void addViolation(Violation v, Set<Integer> intersecting_attrs){
        if(attr_vio_map.containsKey(v.getRattr_index())){
            attr_vio_map.get(v.getRattr_index()).add(v);
        } else {
            Collection<Violation> list = new ArrayList();
            attr_vio_map.put(v.getRattr_index(), list);
            list.add(v);
        }

        if(v.isNewVio() && intersecting_attrs.contains(v.getRid())){
            require_coordinate = true;
        }
    }

    public int getTid(){
        return tid;
    }

    public int getKid() {
        return kid;
    }

    public void setKid(int id) {
        kid = id;
    }

    public boolean isEmptyViolation(){
        return attr_vio_map.size() == 0;
    }

    public Collection<Integer> getAttrs(){
        return attr_vio_map.keySet();
    }

    public Collection<Violation> getViolations(int attr){
        return attr_vio_map.get(attr);
    }

    public boolean isRequire_coordinate(){
        return require_coordinate;
    }

    @Override
    public String toString(){
        return "ViolationGroup: tid=" + tid + " ,attr_vio_map=" + attr_vio_map + " ,require_coordinate=" + require_coordinate;
    }

}
