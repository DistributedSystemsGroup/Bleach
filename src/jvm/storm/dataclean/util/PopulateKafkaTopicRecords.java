package storm.dataclean.util;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;


public class PopulateKafkaTopicRecords {

  public String TOPIC;
  public BufferedReader br;
  private Producer<Object, String> kproducer;
  
  public static void main(String[] args) {

    BleachConfig config = BleachConfig.genConfig(args);

    PopulateKafkaTopicRecords source = new PopulateKafkaTopicRecords();
    source.init(config);
    source.populate();
    source.close();
  }

  private void init(BleachConfig bconfig) {
    Properties props = new Properties();
    props.put("metadata.broker.list", bconfig.get(BleachConfig.KAFKA_SERVER)+":"+bconfig.get(BleachConfig.KAFKA_SERVER_PORT));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

//    NUM_TO_PUBLISH = Integer.parseInt(bconfig.get(BleachConfig.KAFKA_NUM_MESSAGE));
    TOPIC = bconfig.get(BleachConfig.KAFKA_Data_TOPIC);
    ProducerConfig config = new ProducerConfig(props);

    // first type: partition key
    // second type: type of message
    kproducer = new Producer<>(config);
    String filepath = (String) bconfig.get(BleachConfig.KAFKA_PRODUCE_INPUT);

    Path pt = new Path(filepath);

    FileSystem fs = null;
    Configuration hdfsconf = new Configuration();
    hdfsconf.set("fs.defaultFS", bconfig.get(BleachConfig.HDFSFS));
    hdfsconf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    try {
      fs = FileSystem.get(hdfsconf);
      br = new BufferedReader(new InputStreamReader(fs.open(pt)));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void populate() {
    try {
      int id = 0;
      String line = br.readLine();
      while (line != null ){
//        KeyedMessage<Integer, String> msg =  new KeyedMessage(TOPIC, id, line);
        KeyedMessage<Object, String> msg =  new KeyedMessage(TOPIC, line);
        kproducer.send(msg);
        line = br.readLine();
        id++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void close() {
    kproducer.close();
  }

}
