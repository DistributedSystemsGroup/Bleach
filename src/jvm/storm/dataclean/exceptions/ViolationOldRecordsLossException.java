package storm.dataclean.exceptions;

import storm.dataclean.auxiliary.base.Violation;

/**
 * Created by tian on 30/11/2015.
 */
public class ViolationOldRecordsLossException extends BleachException{
    public static String s1 = "Violation: ";
    public static String s2 = " should have old records ";

    public ViolationOldRecordsLossException(Violation v) {
        super(s1 + v.toString() + s2);
    }
}
