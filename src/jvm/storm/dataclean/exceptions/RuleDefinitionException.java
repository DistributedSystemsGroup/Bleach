package storm.dataclean.exceptions;

/**
 * Created by yongchao on 8/4/15.
 */
public class RuleDefinitionException extends BleachException{
    public RuleDefinitionException(String attr) {
        super(attr);
    }

    public RuleDefinitionException(String attr, String[] schema) {
        super("Attr: " + attr + " is not found in Schema:" + schema);
    }
}
