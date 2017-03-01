package storm.dataclean.component.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.dataclean.util.BleachConfig;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

/**
 * Created by yongchao on 11/6/15.
 */
public class KafkaSpoutBuilder {

    public static KafkaSpout buildKafkaDataSpout(BleachConfig bleachConfig) {
        String topic = bleachConfig.get(BleachConfig.KAFKA_Data_TOPIC);
        String zkRoot = bleachConfig.get(BleachConfig.ZKROOT);
        String zkSpoutId = bleachConfig.get(BleachConfig.SPOUT_DATA_ID);
        int numpartition = Integer.parseInt(bleachConfig.get(BleachConfig.KAFKA_PARTITION));
        Broker[] brokers = new Broker[numpartition];
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        for(int i = 0; i < numpartition; i++){
            brokers[i] = new Broker(bleachConfig.get(BleachConfig.KAFKA_SERVER));
            partitionInfo.addPartition(i, brokers[i]);
        }
        StaticHosts hosts = new StaticHosts(partitionInfo);
        SpoutConfig spoutCfg = new SpoutConfig(hosts, topic, zkRoot, zkSpoutId);
        spoutCfg.forceFromStart = true;
        spoutCfg.scheme = new SchemeAsMultiScheme(new BleachDataInputScheme(bleachConfig));
        KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);

        return kafkaSpout;
    }

    public static KafkaSpout buildKafkaControlSpout(BleachConfig bleachConfig) {
        String topic = bleachConfig.get(BleachConfig.KAFKA_Control_TOPIC);
        String zkRoot = bleachConfig.get(BleachConfig.ZKROOT);
        String zkSpoutId = bleachConfig.get(BleachConfig.SPOUT_CONTROL_ID);
        int numpartition = Integer.parseInt(bleachConfig.get(BleachConfig.KAFKA_PARTITION));
        Broker[] brokers = new Broker[numpartition];
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        for(int i = 0; i < numpartition; i++){
            brokers[i] = new Broker(bleachConfig.get(BleachConfig.KAFKA_SERVER));
            partitionInfo.addPartition(i, brokers[i]);
        }
        StaticHosts hosts = new StaticHosts(partitionInfo);
        SpoutConfig spoutCfg = new SpoutConfig(hosts, topic, zkRoot, zkSpoutId);
        spoutCfg.forceFromStart = false; // important
        spoutCfg.scheme = new SchemeAsMultiScheme(new BleachControInputScheme(bleachConfig));
        KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
        return kafkaSpout;
    }
}
