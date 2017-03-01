package storm.dataclean.auxiliary.rule;

import storm.dataclean.exceptions.RuleDefinitionException;

/**
 * Created by tian on 04/12/2015.
 */
public class RuleGenerator {
    public static Rule parse(int ruleid, String rulestring, String schema) throws RuleDefinitionException {
        String[] attrs = rulestring.split(",");
        Rule rule = null;
        if(attrs[0].equals("FD")){
            rule = new FDRule(ruleid, schema, attrs[1], attrs[2]);
        } else if(attrs[0].equals("CFD")){
            rule = new CFDRule(ruleid, schema, attrs[1], attrs[2], attrs[3]);
        } else {
            throw new RuleDefinitionException("Not recognized rule:" + rulestring);
        }
        return rule;
    }

    public static Rule parse(int ruleid, String rulestring, String schema, int window, int window_size) throws RuleDefinitionException {
        String[] attrs = rulestring.split(",");
        Rule rule = null;
        if(attrs[0].equals("FD")){
            rule = new FDRule(ruleid, schema, attrs[1], attrs[2], window, window_size);
        } else if(attrs[0].equals("CFD")){
            rule = new CFDRule(ruleid, schema, attrs[1], attrs[2], attrs[3], window, window_size);
        } else {
            throw new RuleDefinitionException("Not recognized rule:" + rulestring);
        }
        return rule;
    }
}
