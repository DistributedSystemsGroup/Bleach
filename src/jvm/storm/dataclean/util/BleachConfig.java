package storm.dataclean.util;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by yongchao on 10/27/15.
 */
public class BleachConfig implements Serializable{

    public static String TOPONAME = "bleach.topology.name";

    public static String RW = "bleach.topology.rw";

    public static String NUMPROCESS = "bleach.topology.numprocess";
    public static String MAXSPOUTPENDING = "bleach.topology.maxspoutpending";
    public static String NUMACKER = "bleach.topology.numacker";
    public static String INPUT = "bleach.input";
    public static String OUTPUT = "bleach.output";
    public static String SCHEMA = "bleach.schema";
    public static String REPAIR_PROPOSAL_SIZE = "bleach.repair.proposal.size";

    public static String WINDOWS = "bleach.window";
    public static String WINDOWS_SIZE = "bleach.window.size";

    public static String RULE_NUM = "bleach.rule.num";
    public static String RULE_PREFIX = "bleach.rule.";

    public static String ZKHOST = "zookeeper.host";
    public static String ZKROOT = "zookeeper.bleachkafka.path";      // /bleachkafka

    public static String KAFKA_Data_TOPIC = "kafka.topic";
    public static String KAFKA_Control_TOPIC = "kafka.control.topic";
    public static String KAFKA_SERVER = "kafka.server";
    public static String KAFKA_SERVER_PORT = "kafka.server.port";
    public static String KAFKA_PARTITION = "kafka.server.partition";
    public static String KAFKA_PRODUCE_INPUT = "kafka.producer.input";
    public static String KAFKA_DATA_DELIMITER = "kafka.data.delimiter";

    public static String SPOUT_DATA_ID = "bleach.topology.spout.id";
    public static String SPOUT_CONTROL_ID = "bleach.topology.control.spout.id";
    public static String SPOUT_NUM = "bleach.topology.spout.num";

    public static String BOLT_DETECT_WORKER_ID = "bleach.topology.detect.worker.bolt.id";
    public static String BOLT_DETECT_WORKER_NUM = "bleach.topology.detect.worker.bolt.num";
    public static String BOLT_DETECT_INGRESS_ID = "bleach.topology.detect.ingress.bolt.id";
    public static String BOLT_DETECT_INGRESS_NUM = "bleach.topology.detect.ingress.bolt.num";
    public static String BOLT_DETECT_EGRESS_ID = "bleach.topology.detect.egress.bolt.id";
    public static String BOLT_DETECT_EGRESS_NUM = "bleach.topology.detect.egress.bolt.num";
    public static String BOLT_DETECT_CONTROL_ID = "bleach.topology.detect.control.bolt.id";
//    public static String BOLT_DETECT_CONTROL_STREAM_ID = "bleach.topology.detect.control.stream.id";
//    public static String BOLT_DETECT_DATA_STREAM_ID = "bleach.topology.detect.data.stream.id";

    public static String BOLT_REPAIR_WORKER_ID = "bleach.topology.repair.worker.bolt.id";
    public static String BOLT_REPAIR_WORKER_NUM = "bleach.topology.repair.worker.bolt.num";
    public static String BOLT_REPAIR_COORDINATOR_ID = "bleach.topology.repair.coordinator.bolt.id";
    public static String BOLT_REPAIR_EGRESS_ID = "bleach.topology.repair.egress.bolt.id";
    public static String BOLT_REPAIR_EGRESS_NUM = "bleach.topology.repair.egress.bolt.num";

    public static String BOLT_REPAIR_AGGREGATOR_ID = "bleach.topology.repair.aggregator.bolt.id";
    public static String BOLT_REPAIR_AGGREGATOR_NUM = "bleach.topology.repair.aggregator.bolt.num";

    public static String BOLT_DETECT_INGRESS_REPAIR_EGRESS_ID = "bleach.topology.detect-repair.worker.bolt.id";
    public static String BOLT_DETECT_INGRESS_REPAIR_EGRESS_NUM = "bleach.topology.detect-repair.worker.bolt.num";

    public static String HDFSFS = "hdfs.fs";


    public Properties prop;


    public BleachConfig(){
        prop = new Properties();
    }

    public void load(String input) throws IOException {
        InputStream in = new FileInputStream(input);
        prop.load(in);
    }

    public String get(String key){
        return prop.getProperty(key).trim();
    }

    public int getRW(){
        return Integer.parseInt(prop.getProperty(RW));
    }


    public int getMaxspoutpending(){
        return Integer.parseInt(prop.getProperty(MAXSPOUTPENDING));
    }

    public int getNumAcker(){
        return Integer.parseInt(prop.getProperty(NUMACKER, "0"));
    }

    public String getTopologyname(){
        return prop.getProperty(TOPONAME);
    }


    public String getInput(){
        return prop.getProperty(INPUT);
    }

    public String getOutput(){
        return prop.getProperty(OUTPUT);
    }

    public String getSchema(){
        return prop.getProperty(SCHEMA);
    }

    public String[] getRules(){
        int num = Integer.parseInt(prop.getProperty(RULE_NUM));
        String[] rules = new String[num];
        for(int i = 0; i < num; i++){
            rules[i] = prop.getProperty(RULE_PREFIX + i);
        }
        return rules;
    }

//    public boolean getWindow(){
//        if(prop.getProperty(WINDOWS).equals("true")){
//            System.err.println("use windows");
//            return true;
//        } else {
//            System.err.println("not use windows");
//            return false;
//        }
//    }

    public int getWindow(){
        if(prop.getProperty(WINDOWS).equals("false")){
            System.err.println("use windows");
            return 0;
        } else if(prop.getProperty(WINDOWS).equals("basic")){
            System.err.println("use basic windows");
            return 1; // basic windowing
        } else if(prop.getProperty(WINDOWS).equals("bleach")){
            System.err.println("use bleach windows");
            return 2; // bleach specific windowing
        } else {
            System.err.println("window problem. Either false, basic or bleach");
            return 100;
        }
    }


    public int getWindowSize(){
//        if(prop.getProperty(WINDOWS).equals("true")){
            return Integer.parseInt(prop.getProperty(WINDOWS_SIZE));
//        } else {
//            return -1;
//        }
    }

    public static BleachConfig genConfig(String[] args){
        String configfile = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-config")) {
                configfile = args[++i];
            }
        }

        if(configfile.equals("")){
            System.err.println("config file is missing");
            System.err.println("-config <configfile>:  parameter is mandatory");
            System.exit(0);
        }

        BleachConfig config = BleachConfig.parse(configfile);
        return config;
    }


    public static BleachConfig parse(String path){
        BleachConfig config = new BleachConfig();
        try {
            config.load(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return config;
    }

    public static void main(String[] args) {

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("config.properties");
            prop.load(input);

            System.out.println("bleach.input: " + prop.getProperty("bleach.input"));
            System.out.println("dbuser: " + prop.getProperty("dbuser"));
            System.out.println("dbpassword: " + prop.getProperty("dbpassword"));

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
