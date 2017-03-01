package storm.dataclean.auxiliary.base;

import storm.dataclean.exceptions.BleachException;

/**
 * Created by yongchao on 12/28/15.
 */
public class Violation {

    public int tid; // id assigned by detectingress
    public int kid; // id assigned originally (e.g., from kafka), only used in last
    public int rattr_index;

    public ViolationCause vc;

    public Object value;

    public Object othervalue;
    public AbstractSuperCell othervalue_tids;

    protected boolean newVio;

    public Violation(){}

    public Violation(int t, int r, Object k, Object v, int r_attr_index){
        tid = t;
        vc = new ViolationCause(r, k);
        value = v;
        newVio = false;
        rattr_index = r_attr_index;
    }

    public Violation(int t, int r, Object k, Object v, int r_attr_index, Object obj, AbstractSuperCell tids) throws BleachException {
        tid = t;
        vc = new ViolationCause(r, k);
        value = v;
        newVio = true;
        rattr_index = r_attr_index;
        othervalue = obj;
        othervalue_tids = tids.copy();
    }

    public int getTid(){
        return tid;
    }

    public int getKid(){
        return kid;
    }


    public int getRid() { return vc.getRuleid(); }

    public int getRattr_index() {
        return rattr_index;
    }

    public Object getValue(){
        return value;
    }

    public AbstractSuperCell getOthervalue_tids(){
        return othervalue_tids;
    }

    public Object getOthervalue(){
        return othervalue;
    }

    public boolean isNewVio(){
        return newVio;
    }

    public ViolationCause getVioCause(){
        return vc;
    }

    public void setKid(int id){
        kid = id;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Violation: tid=").append(tid).append(", attrid=").append(rattr_index).append("; vc=").append(vc).append("; value=").append(value).
                append("; newVio=").append(newVio).append("; othervalue=").append(othervalue).append(", othertids=").append(othervalue_tids);
        return sb.toString();
    }

    /**
     * To be verified
     * @return
     */
    @Override
    public int hashCode(){
        return tid + (vc.getRuleid()<< 24);
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Violation){
            if(o.hashCode() == hashCode()){
                return true;
            }
        }
        return false;
    }

    public static class NullViolation extends Violation {

        public int rid;

        public NullViolation(){}

        public NullViolation(int t, int r){
            tid = t;
            rid = r;
            newVio = false;
        }

        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append("NullViolation: tid=").append(tid).append(", rid=").append(rid);
            return sb.toString();
        }

        @Override
        public boolean equals(Object o){
            if(o instanceof  NullViolation){
                if(o.hashCode() == hashCode()){
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode(){
            return tid + (rid<< 24);
        }

    }

}
