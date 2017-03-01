package storm.dataclean.auxiliary.base;

/**
 * Created by yongchao on 8/13/15.
 */
public class ViolationCause {
    public int ruleid;

    public Object value;

    public ViolationCause(){

    }

    public ViolationCause(int ruleid, Object value){
        this.ruleid = ruleid;
        this.value = value;
    }

    public int getRuleid(){
        return ruleid;
    }

    @Override
    public String toString(){
        return "vc: ruleid="+ruleid + " , value="+value;
    }

    @Override
    public int hashCode(){
        int code = (ruleid << 24) + value.hashCode();
        return code;
    }

    @Override
    public boolean equals(Object v){
        ViolationCause vc = (ViolationCause)v;
        if(ruleid == vc.ruleid && value.equals(vc.value)){
            return true;
        } else {
            return false;
        }
    }

}
