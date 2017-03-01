package storm.dataclean.auxiliary;

/**
 * Created by yongchao on 11/13/15.
 */
public class GlobalConstant {

//    public static final String CONTROL_STREAM = "control-stream";
    public static final String CONTROL_STREAM = "rule_updates";
//    public static final String REPAIR_CONTROL_STREAM = "repaircontrol-stream";
    public static final String REPAIR_CONTROL_STREAM = "rule_updates";

    public static final String DETECT_DATA_STREAM = "data";

    public static final String REPAIR_DATA_STREAM = DETECT_DATA_STREAM;
    public static final String REPAIR_FROM_COORDINATE_STREAM = "coordinator-repair";
//    public static final String REPAIR_CONTROL_STREAM = "repair-control";
    public static final String REPAIR_TO_COORDINATE_STREAM = "repair-coordinator";

    public static final String REPAIR_PROPOSAL_STREAM = "repair-proposal";
    public static final String REPAIR_CLEAN_NOTIFICATION_STREAM = "repair-clean";
    public static final String REPAIR_ORIGINAL_DATA_STREAM = "default"; // the same with stream outputted by kafkaspout


    public static final int TIDSET_SIZE_LIMIT = 100;


    public static final String START_DETECTINGRESS_STREAM = "data";
    public static final String DETECTINGRESS_DETECTWORKER_STREAM = "subtuples";
    public static final String DETECTWORKER_DETECTEGRESS_STREAM = "violations";
    public static final String DETECTEGRESS_START_STREAM = "clean_tuples";
    public static final String DETECTEGRESS_REPAIRWORKER_STREAM = "violation_group";
    public static final String REPAIRWORKER_REPAIRAGG_STREAM = "repair_proposal";
    public static final String REPAIR_COORDINATION_STREAM = "repair_coordination";
    public static final String REPAIRAGG_START_STREAM = "repair_decision";


}
